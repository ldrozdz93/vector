use std::{pin::Pin, sync::Arc};

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_stream::stream;
use azure_core::{self};
use azure_storage_blobs::prelude::ContainerClient;
use azure_storage_queues::{QueueClient, operations::Message};
use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Bytes;
use futures::{future::ready, stream::StreamExt};
use serde::Deserialize;
use serde_with::serde_as;
use snafu::Snafu;
use tokio::{select, time};
use tokio_util::{codec::FramedRead, io::StreamReader};

use vector_lib::configurable::configurable_component;

use crate::{
    internal_events::{
        BlobDoesntExist, QueueMessageDeleteError, QueueMessageProcessingError,
        QueueMessageReceiveError, QueueStorageInvalidEventIgnored,
        QueueStorageMismatchingContainerName,
    },
    line_agg::{self, LineAgg},
    shutdown::ShutdownSignal,
    sources::azure_blob::{
        AzureBlobConfig, BlobWithAck, BlobWithAckStream, Compression, determine_compression,
    },
};

/// Azure Queue configuration options.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// The name of the storage queue to poll for events.
    pub(super) queue_name: String,

    /// How long to wait while polling the event grid queue for new messages, in seconds.
    ///
    // Stored as u32 for serde compatibility, converted to u64 Duration.
    #[serde(default = "default_poll_secs")]
    #[derivative(Default(value = "default_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub(super) poll_secs: u32,
}

/// Creates a stream of blobs with acknowledgements from the Azure Storage Queue.
///
/// This function polls the configured Azure Storage Queue for Event Grid notifications
/// about blob creation/modification and rename events, then streams the blob contents
/// with acknowledgement handlers that delete queue messages upon successful processing.
///
/// # Arguments
/// * `cfg` - The Azure Blob source configuration
/// * `shutdown` - Signal to gracefully shutdown the stream
/// * `compression` - The compression scheme to use for blob decompression
/// * `framing` - The framing configuration for splitting blob contents
/// * `multiline_config` - Optional multiline aggregation configuration
///
/// # Returns
/// A pinned boxed stream of `BlobWithAck` items
///
/// # Errors
/// Returns an error if queue client creation fails or configuration is invalid
pub fn make_blob_with_ack_stream(
    cfg: &AzureBlobConfig,
    shutdown: ShutdownSignal,
    compression: Compression,
    framing: vector_lib::codecs::decoding::FramingConfig,
    multiline_config: Option<line_agg::Config>,
) -> crate::Result<BlobWithAckStream> {
    let queue_client = make_queue_client(cfg)?;
    let container_client = make_container_client(cfg)?;
    let poll_interval = std::time::Duration::from_secs(
        cfg.queue
            .as_ref()
            .ok_or("Missing Event Grid queue config.")?
            .poll_secs as u64,
    );
    let framer = framing.build();

    Ok(Box::pin(stream! {
        loop {
            let messages = match queue_client.get_messages().number_of_messages(num_messages()).await {
                Ok(messages) => messages,
                Err(e) => {
                    emit!(QueueMessageReceiveError{error: &e});
                    continue;
                }
            };
            if !messages.messages.is_empty() {
                for message in messages.messages {
                    let msg_id = message.message_id.clone();
                    match process_event_grid_message(
                        message,
                        &container_client,
                        &queue_client,
                        compression,
                        framer.clone(),
                        multiline_config.clone(),
                    ).await {
                        Ok(Some(bp)) => yield bp,
                        Ok(None) => trace!("Message {msg_id} processed, no blob stream produced (event may have been ignored or blob unavailable)."),
                        Err(e) => {
                            emit!(QueueMessageProcessingError{
                                error: &e,
                                message_id: &msg_id
                            });
                        }
                    }
                }
            } else {
                select! {
                    _ = shutdown.clone() => {
                        info!("Shutdown signal received, stopping Azure Blob queue polling.");
                        break;
                    },
                    _ = time::sleep(poll_interval) => { }
                }
            }
        }
    }))
}

/// Creates an Azure Queue client from the source configuration.
///
/// This function initializes a queue client using the connection string from the configuration.
/// The client is used to poll for Event Grid messages about blob events.
///
/// # Arguments
/// * `cfg` - The Azure Blob source configuration containing connection string and queue name
///
/// # Returns
/// An Arc-wrapped Azure QueueClient configured for the specified queue
///
/// # Errors
/// Returns an error if:
/// - The queue configuration is missing
/// - The connection string is missing or invalid
/// - The queue service client cannot be initialized
pub fn make_queue_client(cfg: &AzureBlobConfig) -> crate::Result<Arc<QueueClient>> {
    let q = cfg.queue.clone().ok_or("Missing queue.")?;
    crate::azure::build_queue_client(cfg.connection_string.inner(), q.queue_name)
}

/// Creates an Azure Blob Storage container client from the source configuration.
///
/// This function initializes a container client using the connection string and container name
/// from the configuration. The client is used to read blob contents when blob events are received.
///
/// # Arguments
/// * `cfg` - The Azure Blob source configuration containing connection string and container name
///
/// # Returns
/// An Arc-wrapped Azure ContainerClient configured for the specified container
///
/// # Errors
/// Returns an error if:
/// - The connection string is missing
/// - The container client cannot be initialized
/// - Azure authentication fails
pub fn make_container_client(cfg: &AzureBlobConfig) -> crate::Result<Arc<ContainerClient>> {
    crate::azure::build_client(cfg.connection_string.clone().into(), cfg.container_name.clone())
        .map_err(|e| format!("Failed to create Azure container client: {}", e).into())
}

/// Applies decompression to an async reader based on the compression type.
///
/// # Arguments
/// * `reader` - The async reader to wrap with decompression
/// * `compression` - The compression scheme to use
/// * `blob_name` - The name of the blob (used for auto-detection)
/// * `content_type` - Optional Content-Type header value (used for auto-detection)
///
/// # Returns
/// A boxed async reader that decompresses the input stream
fn apply_decompression(
    reader: Pin<Box<dyn tokio::io::AsyncRead + Send>>,
    compression: Compression,
    blob_name: &str,
    content_type: Option<&str>,
) -> Box<dyn tokio::io::AsyncRead + Send + Unpin> {
    let r = tokio::io::BufReader::new(reader);

    let compression = match compression {
        Compression::Auto => {
            determine_compression(content_type, blob_name).unwrap_or(Compression::None)
        }
        _ => compression,
    };

    use Compression::*;
    match compression {
        Auto => unreachable!(),
        None => Box::new(r),
        Gzip => {
            let mut decoder = GzipDecoder::new(r);
            decoder.multiple_members(true);
            Box::new(decoder)
        }
        Zstd => {
            let mut decoder = ZstdDecoder::new(r);
            decoder.multiple_members(true);
            Box::new(decoder)
        }
    }
}

/// Creates a streaming async reader for a blob with optional decompression.
///
/// # Arguments
/// * `blob_client` - The Azure blob client for the blob to stream
/// * `compression` - The compression configuration
/// * `blob_name` - The name of the blob
/// * `content_type` - Optional Content-Type header value
///
/// # Returns
/// An async reader that streams and optionally decompresses blob contents
///
/// # Errors
/// Returns an error if blob streaming fails
async fn create_blob_stream(
    blob_client: &azure_storage_blobs::prelude::BlobClient,
    compression: Compression,
    blob_name: &str,
    content_type: Option<&str>,
) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, ProcessingError> {
    let mut stream = blob_client.get().into_stream();

    let byte_stream = stream! {
        while let Some(response) = stream.next().await {
            match response {
                Ok(resp) => {
                    let mut body = resp.data;
                    while let Some(chunk) = body.next().await {
                        match chunk {
                            Ok(data) => yield Ok(data),
                            Err(e) => yield Err(std::io::Error::other(e)),
                        }
                    }
                }
                Err(e) => yield Err(std::io::Error::other(e)),
            }
        }
    };

    let reader = Box::pin(StreamReader::new(byte_stream));
    Ok(apply_decompression(reader, compression, blob_name, content_type))
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AzureStorageEvent {
    pub subject: String,
    pub event_type: String,
}

#[derive(Debug, Snafu)]
pub enum ProcessingError {
    #[snafu(display("Could not decode Queue message with id {}: {}", message_id, error))]
    InvalidQueueMessage {
        error: serde_json::Error,
        message_id: String,
    },

    #[snafu(display("Failed to base64 decode message: {}", error))]
    FailedDecodingMessageBase64 { error: base64::DecodeError },

    #[snafu(display("Failed to utf8 decode message: {}", error))]
    FailedDecodingUTF8 { error: std::string::FromUtf8Error },

    #[snafu(display("Failed to get blob: {}", error))]
    FailedToGetBlob { error: azure_core::Error },

    #[snafu(display("Failed to parse {} as subject", subject))]
    FailedToParseSubject { subject: String },
}

/// Processes an Azure Event Grid message from the storage queue.
///
/// Decodes the queue message, validates the blob event type (BlobCreated/BlobRenamed),
/// creates a streaming reader for the blob content with optional decompression and framing,
/// and returns a `BlobWithAck` with an acknowledgement handler that deletes the queue message.
///
/// # Returns
/// - `Ok(Some(BlobWithAck))` - Successfully created blob stream
/// - `Ok(None)` - Event ignored (wrong type, wrong container, or blob doesn't exist)
/// - `Err(ProcessingError)` - Failed to process message or retrieve blob
async fn process_event_grid_message(
    message: Message,
    container_client: &ContainerClient,
    queue_client: &QueueClient,
    compression: Compression,
    framer: vector_lib::codecs::decoding::Framer,
    multiline_config: Option<line_agg::Config>,
) -> Result<Option<BlobWithAck>, ProcessingError> {
    let msg_id = message.message_id.clone();
    let decoded_bytes = BASE64_STANDARD
        .decode(&message.message_text)
        .map_err(|e| ProcessingError::FailedDecodingMessageBase64 { error: e })?;
    let decoded_string = String::from_utf8(decoded_bytes)
        .map_err(|e| ProcessingError::FailedDecodingUTF8 { error: e })?;
    let body: AzureStorageEvent = serde_json::from_str(decoded_string.as_str()).map_err(|e| {
        ProcessingError::InvalidQueueMessage {
            error: e,
            message_id: msg_id,
        }
    })?;
    if body.event_type != "Microsoft.Storage.BlobCreated"
        && body.event_type != "Microsoft.Storage.BlobRenamed"
    {
        emit!(QueueStorageInvalidEventIgnored {
            container: container_client.container_name(),
            subject: &body.subject,
            event_type: &body.event_type,
        });
        return Ok(None);
    }
    let (container, blob) = parse_subject(body.subject.clone())
        .ok_or(ProcessingError::FailedToParseSubject {
            subject: body.subject,
        })?;

    if container != container_client.container_name() {
        emit!(QueueStorageMismatchingContainerName {
            configured_container: container_client.container_name(),
            container: container.as_str(),
        });

        return Ok(None);
    }
    trace!(
        "Detected blob event ({}) in container '{}': '{}'",
        &body.event_type, &container, &blob
    );
    let blob_client = container_client.blob_client(&blob);

    // Get blob properties to determine content type for compression auto-detection.
    // Note: Content-Encoding header is NOT used due to Azure SDK limitation.
    // When blobs have Content-Encoding set, the SDK fails with "header not found content-length"
    // because Azure uses chunked transfer encoding for such responses.
    let content_type = match blob_client.get_properties().await {
        Ok(response) => Some(response.blob.properties.content_type.clone()),
        Err(e) => {
            // Handle 404 (blob doesn't exist)
            if let Some(http_error) = e.as_http_error()
                && http_error.status() == 404u16
            {
                emit!(BlobDoesntExist {
                    nonexistent_blob_name: blob_client.blob_name(),
                });
                remove_message_from_queue(queue_client, message).await;
                return Ok(None);
            }
            return Err(ProcessingError::FailedToGetBlob {
                error: azure_core::Error::new(azure_core::error::ErrorKind::Other, e),
            });
        }
    };

    // Create streaming decompressing reader
    let object_reader =
        create_blob_stream(&blob_client, compression, &blob, content_type.as_deref()).await?;

    // Use FramedRead with configurable framer
    let queue_client_copy = queue_client.clone();

    Ok(Some(BlobWithAck {
        blob_data_stream: Box::pin({
            let blob_for_error = blob.clone();
            let lines: Box<dyn futures::Stream<Item = Bytes> + Send + Unpin> = Box::new(
                FramedRead::new(object_reader, framer)
                    .map(move |res| {
                        res.inspect_err(|err| {
                            error!("Framing error for blob '{}': {}", blob_for_error, err);
                        })
                        .ok()
                    })
                    .take_while(|res| ready(res.is_some()))
                    .map(|r| r.expect("validated by take_while")),
            );

            // Apply multiline aggregation if configured
            let lines: Box<dyn futures::Stream<Item = Bytes> + Send + Unpin> =
                match multiline_config {
                    Some(config) => Box::new(
                        LineAgg::new(
                            lines.map(|line| ((), line, ())),
                            line_agg::Logic::new(config),
                        )
                        .map(|(_src, line, _ctx, _lastline_ctx)| line),
                    ),
                    None => lines,
                };

            lines
        }),
        success_handler: Box::new(|| {
            Box::pin(async move {
                remove_message_from_queue(&queue_client_copy, message).await;
            })
        }),
        container,
        blob_name: blob,
    }))
}

/// Parses the subject field from an Azure Event Grid notification.
///
/// The subject field contains the blob path in the format:
/// `/blobServices/default/containers/{container}/blobs/{blob-path}`
///
/// This function extracts the container name and blob path from the subject string.
///
/// # Arguments
/// * `subject` - The subject string from an Azure Event Grid blob event
///
/// # Returns
/// A tuple containing (container_name, blob_path) if parsing succeeds, None otherwise
///
/// # Examples
/// ```
/// let subject = "/blobServices/default/containers/logs/blobs/2024/01/file.txt";
/// let result = parse_subject(subject.to_string());
/// assert_eq!(result, Some(("logs".to_string(), "2024/01/file.txt".to_string())));
/// ```
fn parse_subject(subject: String) -> Option<(String, String)> {
    let parts: Vec<&str> = subject.split('/').collect();
    if parts.len() < 7 {
        warn!(
            "Ignoring event: subject has invalid format (expected /blobServices/default/containers/{{container}}/blobs/{{blob}}), got: '{}'",
            subject
        );
        return None;
    }
    let container = parts[4];
    let blob = parts[6..].join("/");
    Some((container.to_string(), blob))
}

const fn default_poll_secs() -> u32 {
    15
}

// Maximum allowed by the Azure API.
const fn num_messages() -> u8 {
    32
}

async fn remove_message_from_queue(queue_client: &QueueClient, message: Message) {
    _ = queue_client
        .pop_receipt_client(message)
        .delete()
        .await
        .inspect_err(move |e| emit!(QueueMessageDeleteError { error: &e }))
}

#[test]
fn test_azure_storage_event_deserialization() {
    let event_value: AzureStorageEvent = serde_json::from_str(
        r#"{
          "topic": "/subscriptions/fa5f2180-1451-4461-9b1f-aae7d4b33cf8/resourceGroups/events_poc/providers/Microsoft.Storage/storageAccounts/eventspocaccount",
          "subject": "/blobServices/default/containers/content/blobs/foo",
          "eventType": "Microsoft.Storage.BlobCreated",
          "id": "be3f21f7-201e-000b-7605-a29195062628",
          "data": {
            "api": "PutBlob",
            "clientRequestId": "1fa42c94-6dd3-4172-95c4-fd9cf56b5009",
            "requestId": "be3f21f7-201e-000b-7605-a29195000000",
            "eTag": "0x8DC701C5D3FFDF6",
            "contentType": "application/octet-stream",
            "contentLength": 0,
            "blobType": "BlockBlob",
            "url": "https://eventspocaccount.blob.core.windows.net/content/foo",
            "sequencer": "0000000000000000000000000005C5360000000000276a63",
            "storageDiagnostics": {
              "batchId": "fec5b12c-2006-0034-0005-a25936000000"
            }
          },
          "dataVersion": "",
          "metadataVersion": "1",
          "eventTime": "2024-05-09T11:37:10.5637878Z"
        }"#,
    ).unwrap();

    assert_eq!(
        event_value.subject,
        "/blobServices/default/containers/content/blobs/foo"
    );
    assert_eq!(event_value.event_type, "Microsoft.Storage.BlobCreated");
}

#[test]
fn test_parse_subject() {
    // (input, expected_container, expected_blob) — None means parse should fail
    let cases: Vec<(&str, Option<(&str, &str)>)> = vec![
        // Simple blob name
        ("/blobServices/default/containers/content/blobs/foo", Some(("content", "foo"))),
        // Single file in container
        ("/blobServices/default/containers/logs/blobs/file.txt", Some(("logs", "file.txt"))),
        // Nested path
        ("/blobServices/default/containers/logs/blobs/path/to/file.txt", Some(("logs", "path/to/file.txt"))),
        // Deep nested path with dates
        ("/blobServices/default/containers/data/blobs/2024/01/15/logs/app.log", Some(("data", "2024/01/15/logs/app.log"))),
        // Real-world Azure log path
        (
            "/blobServices/default/containers/insights-logs-signinlogs/blobs/tenantId=0e35ee7a-425d-45a5-9013-218c1eae8fd4/y=2024/m=06/d=20/h=05/m=00/PT1H.json",
            Some(("insights-logs-signinlogs", "tenantId=0e35ee7a-425d-45a5-9013-218c1eae8fd4/y=2024/m=06/d=20/h=05/m=00/PT1H.json")),
        ),
        // Special characters (URL-encoded spaces)
        ("/blobServices/default/containers/my-container/blobs/file%20with%20spaces.txt", Some(("my-container", "file%20with%20spaces.txt"))),
        // Unicode
        ("/blobServices/default/containers/données/blobs/файл.txt", Some(("données", "файл.txt"))),
        // Path traversal preserved as-is (Azure handles security)
        ("/blobServices/default/containers/backup/blobs/../../../etc/passwd", Some(("backup", "../../../etc/passwd"))),
        // Invalid: too short
        ("/blobServices/default", None),
        // Invalid: empty
        ("", None),
        // Invalid: wrong format
        ("not/a/valid/subject", None),
    ];

    for (subject, expected) in cases {
        let result = parse_subject(subject.to_string());
        match expected {
            Some((container, blob)) => {
                let (c, b) = result.unwrap_or_else(|| panic!("Expected Some for subject: {subject}"));
                assert_eq!(c, container, "container mismatch for subject: {subject}");
                assert_eq!(b, blob, "blob mismatch for subject: {subject}");
            }
            None => {
                assert!(result.is_none(), "Expected None for subject: {subject}");
            }
        }
    }
}

#[test]
fn test_config_deny_unknown_fields() {
    let json = r#"{"queue_name": "test", "poll_secs": 10, "unknown_field": "value"}"#;
    let result: Result<Config, _> = serde_json::from_str(json);
    assert!(result.is_err());
}

#[test]
fn test_make_queue_client_invalid_connection_string() {
    use crate::sources::azure_blob::AzureBlobConfig;

    let config = AzureBlobConfig {
        connection_string: "invalid-connection-string".to_string().into(),
        container_name: "test".to_string(),
        queue: Some(Config {
            queue_name: "queue".to_string(),
            poll_secs: 10,
        }),
        ..Default::default()
    };

    let result = make_queue_client(&config);
    assert!(result.is_err());
}

#[test]
fn test_make_container_client_with_connection_string() {
    use crate::sources::azure_blob::AzureBlobConfig;

    let config = AzureBlobConfig {
        connection_string: "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net".to_string().into(),
        container_name: "test-container".to_string(),
        queue: Some(Config {
            queue_name: "test-queue".to_string(),
            poll_secs: default_poll_secs(),
        }),
        ..Default::default()
    };

    let result = make_container_client(&config);
    assert!(result.is_ok());
    let client = result.unwrap();
    assert_eq!(client.container_name(), "test-container");
}
