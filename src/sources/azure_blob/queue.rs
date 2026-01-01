use std::{
    io::{BufRead, BufReader, Cursor},
    panic,
    sync::Arc,
};

use crate::sinks::azure_common;
use anyhow::anyhow;
use async_stream::stream;
use azure_core::{self};
use azure_storage;
use azure_storage_blobs::prelude::ContainerClient;
use azure_storage_queues::{QueueClient, operations::Message};
use base64::{Engine, prelude::BASE64_STANDARD};
use futures::stream::StreamExt;
use serde::Deserialize;
use serde_with::serde_as;
use snafu::Snafu;
use tokio::{select, time};

use vector_lib::{
    configurable::configurable_component,
    internal_event::{ByteSize, BytesReceived, InternalEventHandle, Protocol, Registered},
};

use crate::{
    internal_events::{
        BlobDoesntExist, QueueMessageDeleteError, QueueMessageProcessingError,
        QueueMessageReceiveError, QueueStorageInvalidEventIgnored,
        QueueStorageMismatchingContainerName,
    },
    shutdown::ShutdownSignal,
    sources::azure_blob::{AzureBlobConfig, BlobPack, BlobPackStream},
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
    // Restricted to u32 for safe conversion to i32 later.
    #[serde(default = "default_poll_secs")]
    #[derivative(Default(value = "default_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub(super) poll_secs: u32,
}

/// Creates a stream of blob packs from the Azure Storage Queue.
///
/// This function polls the configured Azure Storage Queue for Event Grid notifications
/// about blob creation/modification events, then streams the blob contents.
///
/// # Arguments
/// * `cfg` - The Azure Blob source configuration
/// * `shutdown` - Signal to gracefully shutdown the stream
///
/// # Returns
/// A pinned boxed stream of `BlobPack` items
///
/// # Errors
/// Returns an error if queue client creation fails or configuration is invalid
pub fn make_azure_row_stream(
    cfg: &AzureBlobConfig,
    shutdown: ShutdownSignal,
) -> crate::Result<BlobPackStream> {
    let queue_client = make_queue_client(cfg)?;
    let container_client = make_container_client(cfg)?;
    let bytes_received = register!(BytesReceived::from(Protocol::HTTP));
    let poll_interval = std::time::Duration::from_secs(
        cfg.queue
            .as_ref()
            .ok_or(anyhow!("Missing Event Grid queue config."))?
            .poll_secs as u64,
    );

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
                    match proccess_event_grid_message(
                        message,
                        &container_client,
                        &queue_client,
                        bytes_received.clone()
                    ).await {
                        Ok(Some(bp)) => yield bp,
                        Ok(None) => trace!("Message {msg_id} is ignored, \
                                          no blob stream stream created from it. \
                                          Will retry on next message."),
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
                        info!("Shutdown signal received, terminating azure row stream.");
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
    let connection_string_raw = cfg
        .connection_string
        .clone()
        .ok_or("Missing connection string")?;

    // Use the same pattern as build_client in azure_common
    use azure_core_for_storage::RetryOptions;
    use azure_storage::CloudLocation;
    use azure_storage::ConnectionString;
    use azure_storage_queues::QueueServiceClientBuilder;

    let service_client = {
        let connection_string = ConnectionString::new(connection_string_raw.inner())?;
        let account_name = connection_string
            .account_name
            .ok_or("Account name missing in connection string")?;

        match connection_string.queue_endpoint {
            Some(uri) => QueueServiceClientBuilder::with_location(
                CloudLocation::Custom {
                    uri: uri.to_string(),
                    account: account_name.to_string(),
                },
                connection_string.storage_credentials()?,
            ),
            None => QueueServiceClientBuilder::new(
                account_name,
                connection_string.storage_credentials()?,
            ),
        }
        .retry(RetryOptions::none())
        .build()
    };

    let client = service_client.queue_client(q.queue_name.clone());

    Ok(Arc::new(client))
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
    // Use the azure_common approach like the sink does
    let connection_string = cfg
        .connection_string
        .clone()
        .ok_or("Missing connection string")?;
    azure_common::config::build_client(connection_string.into(), cfg.container_name.clone())
        .map_err(|e| format!("Failed to create Azure container client: {}", e).into())
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

async fn proccess_event_grid_message(
    message: Message,
    container_client: &ContainerClient,
    queue_client: &QueueClient,
    bytes_received: Registered<BytesReceived>,
) -> Result<Option<BlobPack>, ProcessingError> {
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
    if body.event_type != "Microsoft.Storage.BlobCreated" {
        emit!(QueueStorageInvalidEventIgnored {
            container: container_client.container_name(),
            subject: &body.subject,
            event_type: &body.event_type,
        });
        return Ok(None);
    }
    match parse_subject(body.subject.clone()) {
        Some((container, blob)) => {
            if container != container_client.container_name() {
                emit!(QueueStorageMismatchingContainerName {
                    configured_container: container_client.container_name(),
                    container: container.as_str(),
                });

                return Ok(None);
            }
            trace!(
                "Detected new blob creation in container '{}': '{}'",
                &container, &blob
            );
            let blob_client = container_client.blob_client(blob);
            let mut result: Vec<u8> = vec![];
            let mut stream = blob_client.get().into_stream();
            while let Some(value) = stream.next().await {
                match value {
                    Ok(response) => {
                        let mut body = response.data;
                        while let Some(value) = body.next().await {
                            match value {
                                Ok(chunk) => result.extend(&chunk),
                                Err(e) => {
                                    trace!("Failed to read body chunk: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if let Some(http_error) = e.as_http_error() {
                            if http_error.status() == 404u16 {
                                emit!(BlobDoesntExist {
                                    nonexistent_blob_name: blob_client.blob_name(),
                                });
                                remove_message_from_queue(queue_client, message).await;
                                return Ok(None);
                            }
                        }
                        return Err(ProcessingError::FailedToGetBlob {
                            error: azure_core::Error::new(azure_core::error::ErrorKind::Other, e),
                        });
                    }
                }
            }

            let reader = Cursor::new(result);
            let buffered = BufReader::new(reader);
            let queue_client_copy = queue_client.clone();
            let bytes_received_copy = bytes_received.clone();

            Ok(Some(BlobPack {
                row_stream: Box::pin(stream! {
                    for line in buffered.lines() {
                        let line = line.map(|line| line.as_bytes().to_vec());
                        let line = match line {
                            Ok(l) => l,
                            Err(e) => {
                                error!("Failed to map line: {}", e);
                                break;
                            }
                        };
                        bytes_received_copy.emit(ByteSize(line.len()));
                        yield line;
                    }
                }),
                success_handler: Box::new(|| {
                    Box::pin(async move {
                        remove_message_from_queue(&queue_client_copy, message).await;
                    })
                }),
            }))
        }
        None => Err(ProcessingError::FailedToParseSubject {
            subject: body.subject,
        }),
    }
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
pub(super) fn parse_subject(subject: String) -> Option<(String, String)> {
    let parts: Vec<&str> = subject.split('/').collect();
    if parts.len() < 7 {
        warn!("Ignoring event because of wrong subject format");
        return None;
    }
    let container = parts[4];
    let blob = parts[6..].join("/");
    Some((container.to_string(), blob))
}

pub(super) const fn default_poll_secs() -> u32 {
    15
}

// Maximum allowed by the Azure API.
pub(super) const fn num_messages() -> u8 {
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
fn test_azure_storage_event() {
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
        "/blobServices/default/containers/content/blobs/foo".to_string()
    );
    assert_eq!(
        event_value.event_type,
        "Microsoft.Storage.BlobCreated".to_string()
    );
}

#[test]
fn test_parse_subject_no_dir() {
    let subject = "/blobServices/default/containers/content/blobs/foo".to_string();
    let result = parse_subject(subject);
    assert_eq!(result, Some(("content".to_string(), "foo".to_string())));
}

#[test]
fn test_parse_subject_with_dirs() {
    let subject = "/blobServices/default/containers/insights-logs-signinlogs/blobs/tenantId=0e35ee7a-425d-45a5-9013-218c1eae8fd4/y=2024/m=06/d=20/h=05/m=00/PT1H.json".to_string();
    let result = parse_subject(subject);
    assert_eq!(
        result,
        Some((
            "insights-logs-signinlogs".to_string(),
            "tenantId=0e35ee7a-425d-45a5-9013-218c1eae8fd4/y=2024/m=06/d=20/h=05/m=00/PT1H.json"
                .to_string()
        ))
    );
}

// Additional parse_subject tests with various inputs
#[test]
fn test_parse_subject_valid() {
    let subject = "/blobServices/default/containers/logs/blobs/file.txt";
    let result = parse_subject(subject.to_string());
    assert!(result.is_some());
    let (container, blob) = result.unwrap();
    assert_eq!(container, "logs");
    assert_eq!(blob, "file.txt");
}

#[test]
fn test_parse_subject_with_path() {
    let subject = "/blobServices/default/containers/logs/blobs/path/to/file.txt";
    let result = parse_subject(subject.to_string());
    assert!(result.is_some());
    let (container, blob) = result.unwrap();
    assert_eq!(container, "logs");
    assert_eq!(blob, "path/to/file.txt");
}

#[test]
fn test_parse_subject_with_deep_path() {
    let subject = "/blobServices/default/containers/data/blobs/2024/01/15/logs/app.log";
    let result = parse_subject(subject.to_string());
    assert!(result.is_some());
    let (container, blob) = result.unwrap();
    assert_eq!(container, "data");
    assert_eq!(blob, "2024/01/15/logs/app.log");
}

#[test]
fn test_parse_subject_invalid_too_short() {
    let subject = "/blobServices/default";
    let result = parse_subject(subject.to_string());
    assert!(result.is_none());
}

#[test]
fn test_parse_subject_empty() {
    let subject = "";
    let result = parse_subject(subject.to_string());
    assert!(result.is_none());
}

#[test]
fn test_parse_subject_wrong_format() {
    let subject = "not/a/valid/subject";
    let result = parse_subject(subject.to_string());
    assert!(result.is_none());
}

#[test]
fn test_parse_subject_special_characters() {
    let subject = "/blobServices/default/containers/my-container/blobs/file%20with%20spaces.txt";
    let result = parse_subject(subject.to_string());
    assert!(result.is_some());
    let (container, blob) = result.unwrap();
    assert_eq!(container, "my-container");
    assert_eq!(blob, "file%20with%20spaces.txt");
}

#[test]
fn test_parse_subject_unicode() {
    let subject = "/blobServices/default/containers/données/blobs/файл.txt";
    let result = parse_subject(subject.to_string());
    assert!(result.is_some());
    let (container, blob) = result.unwrap();
    assert_eq!(container, "données");
    assert_eq!(blob, "файл.txt");
}

#[test]
fn test_parse_subject_with_dots() {
    let subject = "/blobServices/default/containers/backup/blobs/../../../etc/passwd";
    let result = parse_subject(subject.to_string());
    assert!(result.is_some());
    let (container, blob) = result.unwrap();
    assert_eq!(container, "backup");
    // Path traversal attempts are preserved as-is (Azure will handle security).
    assert_eq!(blob, "../../../etc/passwd");
}

// Test ProcessingError variants
#[test]
fn test_processing_error_display() {
    let error = ProcessingError::FailedToParseSubject {
        subject: "invalid".to_string(),
    };
    assert_eq!(error.to_string(), "Failed to parse invalid as subject");

    let error = ProcessingError::FailedToGetBlob {
        error: azure_core::Error::new(azure_core::error::ErrorKind::Other, "blob error"),
    };
    assert!(error.to_string().contains("Failed to get blob"));
}

// Test default_poll_secs
#[test]
fn test_default_poll_secs() {
    assert_eq!(default_poll_secs(), 15);
}

// Test num_messages
#[test]
fn test_num_messages() {
    assert_eq!(num_messages(), 32);
}

// Test Config with custom values
#[test]
fn test_config_custom_values() {
    let config = Config {
        queue_name: "my-queue".to_string(),
        poll_secs: 30,
    };
    assert_eq!(config.queue_name, "my-queue");
    assert_eq!(config.poll_secs, 30);
}

// Test Config serialization/deserialization
#[test]
fn test_config_serde() {
    let config = Config {
        queue_name: "test-queue".to_string(),
        poll_secs: 20,
    };

    let serialized = serde_json::to_string(&config).unwrap();
    assert!(serialized.contains("test-queue"));
    assert!(serialized.contains("20"));

    let deserialized: Config = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized.queue_name, config.queue_name);
    assert_eq!(deserialized.poll_secs, config.poll_secs);
}

// Test Config with default poll_secs
#[test]
fn test_config_default_poll_secs() {
    let json = r#"{"queue_name": "default-queue"}"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert_eq!(config.queue_name, "default-queue");
    assert_eq!(config.poll_secs, default_poll_secs());
}

// Test AzureStorageEvent parsing for different event types
#[test]
fn test_azure_storage_event_blob_deleted() {
    let event_json = r#"{
        "topic": "/subscriptions/id/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/account",
        "subject": "/blobServices/default/containers/test/blobs/deleted.txt",
        "eventType": "Microsoft.Storage.BlobDeleted",
        "id": "test-id",
        "data": {},
        "dataVersion": "",
        "metadataVersion": "1",
        "eventTime": "2024-01-01T00:00:00Z"
    }"#;

    let event: AzureStorageEvent = serde_json::from_str(event_json).unwrap();
    assert_eq!(event.event_type, "Microsoft.Storage.BlobDeleted");
    assert_eq!(
        event.subject,
        "/blobServices/default/containers/test/blobs/deleted.txt"
    );
}

#[test]
fn test_azure_storage_event_blob_renamed() {
    let event_json = r#"{
        "topic": "/subscriptions/id/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/account",
        "subject": "/blobServices/default/containers/test/blobs/renamed.txt",
        "eventType": "Microsoft.Storage.BlobRenamed",
        "id": "test-id",
        "data": {},
        "dataVersion": "",
        "metadataVersion": "1",
        "eventTime": "2024-01-01T00:00:00Z"
    }"#;

    let event: AzureStorageEvent = serde_json::from_str(event_json).unwrap();
    assert_eq!(event.event_type, "Microsoft.Storage.BlobRenamed");
}

// Test Config edge cases
#[test]
fn test_config_max_poll_secs() {
    let config = Config {
        queue_name: "test".to_string(),
        poll_secs: u32::MAX,
    };
    assert_eq!(config.poll_secs, u32::MAX);
}

#[test]
fn test_config_zero_poll_secs() {
    let config = Config {
        queue_name: "test".to_string(),
        poll_secs: 0,
    };
    assert_eq!(config.poll_secs, 0);
}

// Test deserialization with extra fields (should fail due to deny_unknown_fields)
#[test]
fn test_config_deny_unknown_fields() {
    let json = r#"{"queue_name": "test", "poll_secs": 10, "unknown_field": "value"}"#;
    let result: Result<Config, _> = serde_json::from_str(json);
    assert!(result.is_err());
}

// Test queue client creation with invalid config
#[test]
fn test_make_queue_client_no_auth() {
    use crate::sources::azure_blob::AzureBlobConfig;

    let config = AzureBlobConfig {
        connection_string: None,
        storage_account: None,
        container_name: "test".to_string(),
        queue: Some(Config {
            queue_name: "queue".to_string(),
            poll_secs: 10,
        }),
        endpoint: None,
        client_credentials: None,
        log_namespace: None,
        acknowledgements: Default::default(),
        decoding: crate::serde::default_decoding(),
        blob_pack_stream_factory: None,
    };

    let result = make_queue_client(&config);
    assert!(result.is_err());
}

// Test container client creation with connection string
#[test]
fn test_make_container_client_with_connection_string() {
    use crate::sources::azure_blob::AzureBlobConfig;

    let config = AzureBlobConfig {
        connection_string: Some("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net".to_string().into()),
        storage_account: None,
        container_name: "test-container".to_string(),
        queue: Some(Config {
            queue_name: "test-queue".to_string(),
            poll_secs: default_poll_secs(),
        }),
        endpoint: None,
        client_credentials: None,
        log_namespace: None,
        acknowledgements: Default::default(),
        decoding: crate::serde::default_decoding(),
        blob_pack_stream_factory: None,
    };

    let result = make_container_client(&config);
    assert!(result.is_ok());
    let client = result.unwrap();
    assert_eq!(client.container_name(), "test-container");
}
