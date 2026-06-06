use std::{future::Future, pin::Pin, sync::Arc};

use async_stream::stream;
use bytes::Bytes;
use futures::{Stream, stream::StreamExt};
use tokio::select;
use vrl::{path, value::Kind};

use derivative::Derivative;
use vector_lib::internal_event::Registered;
use vector_lib::{
    codecs::{
        NewlineDelimitedDecoderConfig,
        decoding::{DeserializerConfig, FramingConfig, NewlineDelimitedDecoderOptions},
    },
    config::{LegacyKey, log_schema},
    configurable::configurable_component,
    internal_event::{ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol},
    lookup::{PathPrefix, metadata_path, owned_value_path},
    sensitive_string::SensitiveString,
};

/// Compression scheme for blobs retrieved from Azure Blob Storage.
#[configurable_component]
#[configurable(metadata(docs::advanced))]
#[derive(Clone, Copy, Debug, Derivative, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derivative(Default)]
pub enum Compression {
    /// Automatically attempt to determine the compression scheme.
    ///
    /// The compression scheme is determined from the blob's `Content-Type` metadata
    /// and the blob name suffix (e.g., `.gz`, `.zst`).
    ///
    /// Priority order: 1) Content-Type, 2) File extension.
    ///
    /// Supported Content-Type values: `application/gzip`, `application/x-gzip`, `application/zstd`.
    ///
    /// **Note:** Unlike the AWS S3 source, the `Content-Encoding` header is NOT supported
    /// due to an Azure SDK limitation. When blobs have `Content-Encoding` set, use the
    /// file extension or `Content-Type` header instead, or configure explicit compression.
    ///
    /// It is set to `none` if the compression scheme cannot be determined.
    #[derivative(Default)]
    Auto,

    /// Uncompressed.
    None,

    /// GZIP.
    Gzip,

    /// ZSTD.
    Zstd,
}

/// Returns the default framing configuration for backwards compatibility.
/// Uses newline-delimited framing to match the original hardcoded behavior.
const fn default_framing() -> FramingConfig {
    FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
        newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
    })
}

use crate::{
    SourceSender,
    codecs::{Decoder, DecodingConfig},
    config::{
        LogNamespace, SourceAcknowledgementsConfig, SourceConfig, SourceContext, SourceOutput,
    },
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf, Event},
    internal_events::{
        EventsReceived, InvalidRowEventTypeError, QueueMessageProcessingErrored,
        QueueMessageProcessingRejected, QueueMessageProcessingSucceeded, StreamClosedError,
    },
    line_agg,
    serde::{bool_or_struct, default_decoding},
    shutdown::ShutdownSignal,
    sources::{azure_blob::queue::make_blob_with_ack_stream, util::MultilineConfig},
};

#[cfg(all(test, feature = "azure-blob-integration-tests"))]
mod integration_tests;
pub mod queue;
#[cfg(test)]
mod test;

/// Collects logs from Azure Blob Storage.
///
/// This source reads objects from Azure Blob Storage by processing events from an Azure Storage Queue.
/// When a blob is created or modified in the configured container, an event is sent to the queue,
/// and this source processes those events to read and decode the blob contents.
#[configurable_component(source("azure_blob", "Collect logs from Azure Blob Storage."))]
#[derive(Clone, Derivative)]
#[derivative(Default, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct AzureBlobConfig {
    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,

    /// Factory function for creating blob streams with acknowledgements. Used only for tests.
    #[configurable(metadata(docs::hidden))]
    #[serde(skip)]
    #[derivative(Default(value = "None"), Debug = "ignore")]
    pub blob_stream_factory:
        Option<Arc<dyn Fn(ShutdownSignal) -> crate::Result<BlobWithAckStream> + Send + Sync>>,

    /// Configuration options for Storage Queue.
    queue: Option<queue::Config>,

    /// The Azure Blob Storage Account connection string.
    ///
    /// Authentication with access key is the only supported authentication method.
    #[configurable(metadata(
        docs::examples = "DefaultEndpointsProtocol=https;AccountName=mylogstorage;AccountKey=storageaccountkeybase64encoded;EndpointSuffix=core.windows.net"
    ))]
    pub connection_string: SensitiveString,

    /// The Azure Blob Storage Account container name.
    #[configurable(metadata(docs::examples = "my-logs"))]
    pub(super) container_name: String,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    pub acknowledgements: SourceAcknowledgementsConfig,

    /// Compression scheme used for decompressing blobs retrieved from Azure Blob Storage.
    #[configurable(derived)]
    #[serde(default)]
    pub compression: Compression,

    /// Configurable framing for splitting blob contents into events.
    #[configurable(derived)]
    #[serde(default = "default_framing")]
    #[derivative(Default(value = "default_framing()"))]
    pub framing: FramingConfig,

    /// Multiline aggregation configuration.
    ///
    /// If not specified, multiline aggregation is disabled.
    #[configurable(derived)]
    pub multiline: Option<MultilineConfig>,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    pub decoding: DeserializerConfig,

    /// Whether to delete non-retryable messages from the queue.
    ///
    /// If a message is rejected by the sink and not retryable, setting this to `true`
    /// will delete the message from the queue. When `false`, rejected messages are
    /// retained in the queue and will become visible again after the visibility timeout.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub delete_failed_message: bool,
}

const fn default_true() -> bool {
    true
}

impl_generate_config_from_default!(AzureBlobConfig);

impl AzureBlobConfig {
    /// Self validation
    pub fn validate(&self) -> crate::Result<()> {
        let queue = match self.queue.as_ref() {
            Some(queue) if !queue.queue_name.is_empty() => queue,
            _ => return Err("Azure event grid queue must be set.".into()),
        };

        if self.container_name.is_empty() {
            return Err("Azure Container must be set.".into());
        }

        if !(1..=32).contains(&queue.max_number_of_messages) {
            return Err("Azure queue `max_number_of_messages` must be between 1 and 32.".into());
        }

        if !(1..=604800).contains(&queue.visibility_timeout_secs) {
            return Err(
                "Azure queue `visibility_timeout_secs` must be between 1 and 604800.".into(),
            );
        }

        if queue.poll_secs == 0 {
            return Err("Azure queue `poll_secs` must be greater than 0.".into());
        }

        Ok(())
    }
}

/// Determines the compression format from content type and blob name.
/// Priority order: 1) Content-Type, 2) File extension.
///
/// Note: Content-Encoding header is NOT supported due to Azure SDK limitation.
/// The SDK requires Content-Length header in responses, but Azure uses chunked
/// transfer encoding when Content-Encoding is set, causing parsing failures.
pub(super) fn determine_compression(
    content_type: Option<&str>,
    blob_name: &str,
) -> Option<Compression> {
    content_type
        .and_then(content_type_to_compression)
        .or_else(|| blob_name_to_compression(blob_name))
}

/// Converts Content-Type header value to Compression enum.
/// Strips MIME parameters (e.g. `application/gzip; charset=utf-8` → `application/gzip`).
fn content_type_to_compression(content_type: &str) -> Option<Compression> {
    let base_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    match base_type {
        "application/gzip" | "application/x-gzip" => Some(Compression::Gzip),
        "application/zstd" => Some(Compression::Zstd),
        _ => None,
    }
}

/// Determines compression from blob file extension.
fn blob_name_to_compression(blob_name: &str) -> Option<Compression> {
    let extension = std::path::Path::new(blob_name)
        .extension()
        .and_then(std::ffi::OsStr::to_str);

    extension.and_then(|ext| match ext {
        "gz" => Some(Compression::Gzip),
        "zst" => Some(Compression::Zstd),
        _ => None,
    })
}

type BlobDataStream = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

/// Outcome of blob stream consumption, passed to the completion handler.
pub(super) enum StreamResult {
    /// All events delivered successfully — safe to delete queue message.
    Delivered,
    /// Upstream reported an error — retain queue message for retry.
    Errored,
    /// Permanently rejected — delete queue message to prevent infinite reprocessing.
    Rejected,
}

pub struct BlobWithAck {
    pub(super) blob_data_stream: BlobDataStream,
    /// Called after stream consumption to finalize queue message handling.
    /// Encapsulates both the success action (delete queue message) and
    /// read-error checking (retain queue message on framing errors).
    pub(super) completion_handler:
        Box<dyn FnOnce(StreamResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,
    pub(super) container: String,
    pub(super) blob_name: String,
}

type BlobWithAckStream = Pin<Box<dyn Stream<Item = BlobWithAck> + Send>>;

struct AzureBlobStreamer {
    shutdown: ShutdownSignal,
    out: SourceSender,
    log_namespace: LogNamespace,
    acknowledge: bool,
    delete_failed_message: bool,
    decoder: Decoder,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
}

impl AzureBlobStreamer {
    pub fn new(
        shutdown: ShutdownSignal,
        out: SourceSender,
        log_namespace: LogNamespace,
        acknowledge: bool,
        delete_failed_message: bool,
        framing: FramingConfig,
        decoding: DeserializerConfig,
    ) -> crate::Result<Self> {
        Ok(Self {
            shutdown,
            out,
            log_namespace,
            acknowledge,
            delete_failed_message,
            decoder: DecodingConfig::new(framing, decoding, log_namespace).build()?,
            bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            events_received: register!(EventsReceived),
        })
    }

    pub async fn run_streaming(mut self, mut blob_stream: BlobWithAckStream) -> Result<(), ()> {
        debug!("Azure Blob source: starting blob event processing loop.");

        loop {
            select! {
                blob = blob_stream.next() => {
                    match blob{
                        Some(blob) => {
                            self.process_blob(blob).await?;
                        }
                        None => {
                            break; // end of stream
                        }
                    }
                },
                _ = self.shutdown.clone() => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn process_blob(&mut self, blob: BlobWithAck) -> Result<(), ()> {
        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(self.acknowledge);
        let mut data_stream = blob.blob_data_stream;
        let container = blob.container;
        let blob_name = blob.blob_name;
        let mut output_stream = {
            let bytes_received = self.bytes_received.clone();
            let events_received = self.events_received.clone();
            let log_namespace = self.log_namespace;
            let decoder = self.decoder.clone();
            let container = container.clone();
            let blob = blob_name.clone();
            stream! {
                while let Some(chunk) = data_stream.next().await {
                    bytes_received.emit(ByteSize(chunk.len()));
                    let (events, _) = match decoder.deserializer_parse(chunk) {
                        Ok(result) => result,
                        Err(_error) => {
                            // Error is handled by codecs::Decoder, no further handling needed
                            continue;
                        }
                    };
                    for mut event in events {
                        event = event.with_batch_notifier_option(&batch);
                        match event {
                            Event::Log(ref mut log_event) => {
                                log_namespace.insert_source_metadata(
                                    AzureBlobConfig::NAME,
                                    log_event,
                                    Some(LegacyKey::Overwrite(path!("container"))),
                                    path!("container"),
                                    container.clone(),
                                );
                                log_namespace.insert_source_metadata(
                                    AzureBlobConfig::NAME,
                                    log_event,
                                    Some(LegacyKey::Overwrite(path!("blob"))),
                                    path!("blob"),
                                    blob.clone(),
                                );

                                // Insert timestamp metadata following AWS S3 pattern
                                let timestamp = chrono::Utc::now();
                                match log_namespace {
                                    LogNamespace::Vector => {
                                        let ts_path = metadata_path!(AzureBlobConfig::NAME, "timestamp");
                                        log_event.insert(ts_path, timestamp);
                                        let ingest_path = metadata_path!("vector", "ingest_timestamp");
                                        log_event.insert(ingest_path, timestamp);
                                    }
                                    LogNamespace::Legacy => {
                                        if let Some(timestamp_key) = log_schema().timestamp_key() {
                                            log_event.try_insert((PathPrefix::Event, timestamp_key), timestamp);
                                        }
                                    }
                                }

                                events_received.emit(CountByteSize(1, event.estimated_json_encoded_size_of()));
                                yield event
                            }
                            _ => {
                                emit!(InvalidRowEventTypeError{event: &event})
                            }
                        }
                    }
                }
                drop(batch);
            }.boxed()
        };

        let send_error = match self.out.send_event_stream(&mut output_stream).await {
            Ok(_) => None,
            Err(error) => {
                let (count, _) = output_stream.size_hint();
                emit!(StreamClosedError { count });
                Some(error)
            }
        };

        drop(output_stream);

        if send_error.is_some() {
            emit!(QueueMessageProcessingErrored {});
            return Ok(());
        }

        match receiver {
            None => (blob.completion_handler)(StreamResult::Delivered).await,
            Some(receiver) => match receiver.await {
                BatchStatus::Delivered => {
                    (blob.completion_handler)(StreamResult::Delivered).await;
                    emit!(QueueMessageProcessingSucceeded {});
                }
                BatchStatus::Errored => {
                    (blob.completion_handler)(StreamResult::Errored).await;
                    emit!(QueueMessageProcessingErrored {});
                }
                BatchStatus::Rejected => {
                    if self.delete_failed_message {
                        warn!(
                            message = "Blob events rejected by sink. Deleting queue message per config.",
                            container = %container,
                            blob = %blob_name,
                        );
                        (blob.completion_handler)(StreamResult::Rejected).await;
                    }
                    emit!(QueueMessageProcessingRejected {});
                }
            },
        }

        Ok(())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_blob")]
impl SourceConfig for AzureBlobConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        self.validate()?;

        let multiline_config: Option<line_agg::Config> = self
            .multiline
            .as_ref()
            .map(|config| config.try_into())
            .transpose()?;

        let azure_blob_streamer = AzureBlobStreamer::new(
            cx.shutdown.clone(),
            cx.out.clone(),
            cx.log_namespace(self.log_namespace),
            cx.do_acknowledgements(self.acknowledgements),
            self.delete_failed_message,
            self.framing.clone(),
            self.decoding.clone(),
        )?;

        let blob_stream: BlobWithAckStream = match self.blob_stream_factory {
            Some(ref factory) => factory(cx.shutdown.clone())?,
            None => {
                make_blob_with_ack_stream(
                    self,
                    cx.shutdown.clone(),
                    self.compression,
                    self.framing.clone(),
                    multiline_config,
                    &cx.proxy,
                )
                .await?
            }
        };
        Ok(Box::pin(azure_blob_streamer.run_streaming(blob_stream)))
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);
        let schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("container"))),
                &owned_value_path!("container"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                Some(LegacyKey::Overwrite(owned_value_path!("blob"))),
                &owned_value_path!("blob"),
                Kind::bytes(),
                None,
            )
            .with_source_metadata(
                Self::NAME,
                None,
                &owned_value_path!("timestamp"),
                Kind::timestamp(),
                Some("timestamp"),
            )
            .with_standard_vector_source_metadata();

        vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}
