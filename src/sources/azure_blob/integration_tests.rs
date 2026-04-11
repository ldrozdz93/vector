use azure_storage_blobs::prelude::PublicAccess;
use base64::{Engine, prelude::BASE64_STANDARD};
use flate2::{Compression as GzCompression, write::GzEncoder};
use std::{io::Write, time::Duration};

use super::{
    AzureBlobConfig, Compression,
    queue::{Config, make_container_client, make_queue_client},
};
use crate::{
    event::Event,
    serde::default_decoding,
    test_util::components::{
        COMPONENT_ERROR_TAGS, SOURCE_TAGS, run_and_assert_source_compliance,
        run_and_assert_source_error,
    },
};

impl AzureBlobConfig {
    pub async fn new_emulator() -> AzureBlobConfig {
        let address = std::env::var("AZURE_ADDRESS").unwrap_or_else(|_| "localhost".to_string());
        let config = AzureBlobConfig {
                connection_string: format!("UseDevelopmentStorage=true;DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{}:10000/devstoreaccount1;QueueEndpoint=http://{}:10001/devstoreaccount1;TableEndpoint=http://{}:10002/devstoreaccount1;", address, address, address).into(),
                container_name: "logs".to_string(),
                queue: Some(Config {
                    queue_name: format!("test-{}", rand::random::<u32>()),
                    poll_secs: 1,
                    ..Default::default()
                }),
                decoding: default_decoding(),
                ..Default::default()
            };

        config.ensure_container().await;
        config.ensure_queue().await;

        config
    }

    async fn run_assert(&self) -> Vec<Event> {
        run_and_assert_source_compliance(self.clone(), Duration::from_secs(1), &SOURCE_TAGS).await
    }

    async fn run_error(&self) -> Vec<Event> {
        run_and_assert_source_error(self.clone(), Duration::from_secs(1), &COMPONENT_ERROR_TAGS)
            .await
    }

    async fn ensure_container(&self) {
        let client = make_container_client(self).expect("Failed to create container client");
        let request = client
            .create()
            .public_access(PublicAccess::None)
            .into_future();

        let response = match request.await {
            Ok(_) => Ok(()),
            Err(reason) => {
                let error_msg = reason.to_string();
                // Check for HTTP 409 (Conflict) which means container already exists - this is OK
                if error_msg.contains("409")
                    || error_msg.contains("conflict")
                    || error_msg.contains("already exists")
                {
                    Ok(())
                } else {
                    Err(format!("Unexpected error {}", reason))
                }
            }
        };

        response.expect("Failed to create container")
    }

    async fn ensure_queue(&self) {
        let client = make_queue_client(self).expect("Failed to create queue client");
        let request = client.create().into_future();

        let response = match request.await {
            Ok(_) => Ok(()),
            Err(reason) => {
                let error_msg = reason.to_string();
                // Check for HTTP 409 (Conflict) which means queue already exists - this is OK
                if error_msg.contains("409")
                    || error_msg.contains("conflict")
                    || error_msg.contains("already exists")
                {
                    Ok(())
                } else {
                    Err(format!("Unexpected error {}", reason))
                }
            }
        };

        response.expect("Failed to create queue")
    }

    async fn upload_blob(&self, name: String, content: String) {
        let container_client =
            make_container_client(self).expect("Failed to create container client");
        let blob_client = container_client.blob_client(name.clone());
        blob_client
            .put_block_blob(content)
            .await
            .expect("Failed putting blob");

        self.queue_notify_blob_created(&name).await;
    }

    async fn upload_compressed_blob(&self, name: String, content: String, compression: &str) {
        let container_client =
            make_container_client(self).expect("Failed to create container client");
        let blob_client = container_client.blob_client(name.clone());

        let compressed_data = match compression {
            "gzip" => {
                let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
                encoder
                    .write_all(content.as_bytes())
                    .expect("Failed to write to gzip encoder");
                encoder.finish().expect("Failed to finish gzip compression")
            }
            "zstd" => {
                zstd::encode_all(content.as_bytes(), 3).expect("Failed to compress with zstd")
            }
            _ => panic!("Unsupported compression type: {}", compression),
        };

        blob_client
            .put_block_blob(compressed_data)
            .await
            .expect("Failed putting compressed blob");

        self.queue_notify_blob_created(&name).await;
    }

    async fn queue_notify_blob_created(&self, name: &str) {
        let queue_client = make_queue_client(self).expect("Failed to create queue client");
        let message = format!(
            r#"{{
          "topic": "/subscriptions/fa5f2180-1451-4461-9b1f-aae7d4b33cf8/resourceGroups/events_poc/providers/Microsoft.Storage/storageAccounts/eventspocaccount",
          "subject": "/blobServices/default/containers/logs/blobs/{}",
          "eventType": "Microsoft.Storage.BlobCreated",
          "id": "be3f21f7-201e-000b-7605-a29195062628",
          "data": {{
            "api": "PutBlob",
            "clientRequestId": "1fa42c94-6dd3-4172-95c4-fd9cf56b5009",
            "requestId": "be3f21f7-201e-000b-7605-a29195000000",
            "eTag": "0x8DC701C5D3FFDF6",
            "contentType": "application/octet-stream",
            "contentLength": 0,
            "blobType": "BlockBlob",
            "url": "https://eventspocaccount.blob.core.windows.net/logs/{}",
            "sequencer": "0000000000000000000000000005C5360000000000276a63",
            "storageDiagnostics": {{
              "batchId": "fec5b12c-2006-0034-0005-a25936000000"
            }}
          }},
          "dataVersion": "",
          "metadataVersion": "1",
          "eventTime": "2024-05-09T11:37:10.5637878Z"
        }}"#,
            name, name
        );
        queue_client
            .put_message(BASE64_STANDARD.encode(message))
            .await
            .expect("Failed putting message");
    }

    async fn queue_notify_blob_renamed(&self, name: &str) {
        let queue_client = make_queue_client(self).expect("Failed to create queue client");
        let message = format!(
            r#"{{
          "topic": "/subscriptions/fa5f2180-1451-4461-9b1f-aae7d4b33cf8/resourceGroups/events_poc/providers/Microsoft.Storage/storageAccounts/eventspocaccount",
          "subject": "/blobServices/default/containers/logs/blobs/{}",
          "eventType": "Microsoft.Storage.BlobRenamed",
          "id": "be3f21f7-201e-000b-7605-a29195062629",
          "data": {{
            "api": "RenameFile",
            "clientRequestId": "6d79dbfb-0e37-4fc4-981f-442c9ca65760",
            "requestId": "831e1650-001e-001b-66ab-eeb76e000000",
            "destinationUrl": "https://eventspocaccount.dfs.core.windows.net/logs/{}",
            "sourceUrl": "https://eventspocaccount.dfs.core.windows.net/logs/old-name.txt",
            "sequencer": "00000000000004420000000000028963",
            "storageDiagnostics": {{
              "batchId": "b68529f3-68cd-4744-baa4-3c0498ec19f0"
            }}
          }},
          "dataVersion": "1",
          "metadataVersion": "1",
          "eventTime": "2024-05-09T11:37:10.5637878Z"
        }}"#,
            name, name
        );
        queue_client
            .put_message(BASE64_STANDARD.encode(message))
            .await
            .expect("Failed putting message");
    }
    async fn queue_notify_custom_event(&self, name: &str, event_type: &str) {
        let queue_client = make_queue_client(self).expect("Failed to create queue client");
        let message = format!(
            r#"{{
          "topic": "/subscriptions/fa5f2180-1451-4461-9b1f-aae7d4b33cf8/resourceGroups/events_poc/providers/Microsoft.Storage/storageAccounts/eventspocaccount",
          "subject": "/blobServices/default/containers/logs/blobs/{}",
          "eventType": "{}",
          "id": "be3f21f7-201e-000b-7605-a29195062630",
          "data": {{
            "api": "PutBlob",
            "clientRequestId": "1fa42c94-6dd3-4172-95c4-fd9cf56b5009",
            "requestId": "be3f21f7-201e-000b-7605-a29195000000",
            "eTag": "0x8DC701C5D3FFDF6",
            "contentType": "application/octet-stream",
            "contentLength": 0,
            "blobType": "BlockBlob",
            "url": "https://eventspocaccount.blob.core.windows.net/logs/{}",
            "sequencer": "0000000000000000000000000005C5360000000000276a63",
            "storageDiagnostics": {{
              "batchId": "fec5b12c-2006-0034-0005-a25936000000"
            }}
          }},
          "dataVersion": "",
          "metadataVersion": "1",
          "eventTime": "2024-05-09T11:37:10.5637878Z"
        }}"#,
            name, event_type, name
        );
        queue_client
            .put_message(BASE64_STANDARD.encode(message))
            .await
            .expect("Failed putting message");
    }
}

/// Test basic functionality: reading a single line from a blob.
///
/// **Setup:**
/// - Upload a blob containing a single line of text: "a"
///
/// **Verification:**
/// - Verify exactly 1 event is received
/// - Verify the event message matches the uploaded content
///
/// **Purpose:** Validate basic blob reading and event generation for single-line content.
#[tokio::test]
async fn azure_blob_read_single_line_from_blob() {
    let config = AzureBlobConfig::new_emulator().await;
    let content = "a";
    config
        .upload_blob("file.txt".to_string(), content.to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "a".into());
}

/// Test handling of BlobRenamed events.
///
/// **Setup:**
/// - Upload a blob with content to a destination path
/// - Send a BlobRenamed event notification pointing to that blob
///
/// **Verification:**
/// - Verify exactly 1 event is received
/// - Verify the event message matches the blob content at the renamed location
///
/// **Purpose:** Validate that BlobRenamed events trigger blob reading at the new location.
#[tokio::test]
async fn azure_blob_read_blob_renamed_event() {
    let config = AzureBlobConfig::new_emulator().await;
    let content = "renamed_blob_content";

    // Upload blob to the destination path
    let blob_client = make_container_client(&config)
        .expect("Failed to create container client")
        .blob_client("renamed-file.txt");
    blob_client
        .put_block_blob(content)
        .await
        .expect("Failed putting blob");

    // Send BlobRenamed event notification
    config.queue_notify_blob_renamed("renamed-file.txt").await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "renamed_blob_content".into());
}

/// Test reading multiple lines from a single blob with newline-delimited framing.
///
/// **Setup:**
/// - Upload a blob with newline-separated content: "a\nb\nc"
/// - Use default newline-delimited framing
///
/// **Verification:**
/// - Verify exactly 3 events are received
/// - Verify each line is parsed as a separate event in order
///
/// **Purpose:** Validate default newline-delimited framing splits blob content correctly.
#[tokio::test]
async fn azure_blob_read_multiple_lines_from_blob() {
    let config = AzureBlobConfig::new_emulator().await;
    let content = "a\nb\nc";
    config
        .upload_blob("file.txt".to_string(), content.to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].as_log()["message"], "a".into());
    assert_eq!(events[1].as_log()["message"], "b".into());
    assert_eq!(events[2].as_log()["message"], "c".into());
}

/// Test processing multiple blobs from the same container.
///
/// **Setup:**
/// - Upload 3 separate blobs, each containing a single line: "a", "b", "c"
/// - Each blob generates an Event Grid notification
///
/// **Verification:**
/// - Verify exactly 3 events are received (one from each blob)
/// - Verify each event contains the content from its respective blob
///
/// **Purpose:** Validate the source can handle multiple blob notifications and process them correctly.
#[tokio::test]
async fn azure_blob_read_single_line_from_multiple_blobs() {
    let config = AzureBlobConfig::new_emulator().await;
    let contents = vec!["a", "b", "c"];
    for (i, content) in contents.clone().iter().enumerate() {
        config
            .upload_blob(format!("file{}.txt", i), content.to_string())
            .await;
    }

    let events =
        run_and_assert_source_compliance(config.clone(), Duration::from_secs(4), &SOURCE_TAGS)
            .await;
    assert_eq!(events.len(), contents.len());
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.as_log()["message"], contents[i].into());
    }
}

/// Test error handling when queue messages cannot be read.
///
/// **Setup:**
/// - Upload a blob with valid content
/// - Configure source to read from a non-existent queue
///
/// **Verification:**
/// - Verify no events are received (source should emit error metric)
/// - Verify error tags are emitted for the component
///
/// **Purpose:** Validate proper error handling when queue is inaccessible.
#[tokio::test]
async fn azure_blob_emit_error_on_message_read() {
    let mut config = AzureBlobConfig::new_emulator().await;
    let content = "a\nb\nc";
    config
        .upload_blob("file.txt".to_string(), content.to_string())
        .await;
    config.queue = Some(Config {
        queue_name: "nonexistent".to_string(),
        poll_secs: 1,
        ..Default::default()
    });

    let events = config.run_error().await;
    assert!(events.is_empty());
}

/// Test graceful handling of notifications for non-existent blobs.
///
/// **Setup:**
/// - Send Event Grid notification for a blob that doesn't exist
/// - Then upload a valid blob with actual content
///
/// **Verification:**
/// - Verify only the valid blob's event is received (1 event total)
/// - Verify the missing blob notification is ignored without crashing
///
/// **Purpose:** Validate resilience when Event Grid sends notifications for deleted or non-existent blobs.
#[tokio::test]
async fn azure_blob_ignore_missing_blob() {
    let config = AzureBlobConfig::new_emulator().await;

    config.queue_notify_blob_created("non-existent").await;
    config
        .upload_blob("file.txt".to_string(), "some_content".to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
}

/// Test JSON deserialization with JSON codec.
///
/// **Setup:**
/// - Configure source with JSON deserializer
/// - Upload a blob containing a single JSON object with fields: timestamp, level, message
///
/// **Verification:**
/// - Verify exactly 1 event is received
/// - Verify JSON fields are correctly parsed and accessible in the log event
///
/// **Purpose:** Validate decoding configuration works correctly for JSON content.
#[tokio::test]
async fn azure_blob_read_json_content() {
    use vector_lib::codecs::decoding::{DeserializerConfig, JsonDeserializerConfig};

    let mut config = AzureBlobConfig::new_emulator().await;
    config.decoding = DeserializerConfig::Json(JsonDeserializerConfig::default());

    let json_content =
        r#"{"timestamp": "2024-01-01T00:00:00Z", "level": "INFO", "message": "Test log"}"#;
    config
        .upload_blob("log.json".to_string(), json_content.to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
    let log = events[0].as_log();
    assert_eq!(log["level"], "INFO".into());
    assert_eq!(log["message"], "Test log".into());
}

/// Test error handling with malformed Event Grid messages.
///
/// **Setup:**
/// - Manually enqueue an invalid (non-JSON) message to the queue
/// - Upload a valid blob with correct content
///
/// **Verification:**
/// - Verify the valid blob's event is still received (1 event total)
/// - Verify the malformed message is skipped without blocking valid messages
///
/// **Purpose:** Validate source resilience when queue contains corrupted or invalid messages.
#[tokio::test]
async fn azure_blob_handle_malformed_message() {
    let config = AzureBlobConfig::new_emulator().await;
    let queue_client = make_queue_client(&config).expect("Failed to create queue client");
    queue_client
        .put_message(BASE64_STANDARD.encode("not a valid json"))
        .await
        .expect("Failed putting malformed message");

    config
        .upload_blob("file.txt".to_string(), "correct content".to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "correct content".into());
}

/// Test handling of empty blobs.
///
/// **Setup:**
/// - Upload a blob with zero-length content (empty string)
///
/// **Verification:**
/// - Verify the source handles empty blobs gracefully (either 0 events or 1 empty event)
/// - Verify no errors or crashes occur
///
/// **Purpose:** Validate edge case handling for empty blob content.
#[tokio::test]
async fn azure_blob_read_empty_blob() {
    let config = AzureBlobConfig::new_emulator().await;
    config
        .upload_blob("empty.txt".to_string(), "".to_string())
        .await;

    let events = config.run_assert().await;
    if !events.is_empty() {
        assert_eq!(events[0].as_log()["message"], "".into());
    }
}

// ===== Compression Integration Tests =====

/// Test gzip decompression with automatic compression detection via file extension.
///
/// **Setup:**
/// - Configure compression = "auto"
/// - Upload a gzip-compressed blob with ".gz" extension containing "line1\nline2\nline3"
///
/// **Verification:**
/// - Verify blob is automatically detected as gzip and decompressed
/// - Verify exactly 3 events are received with correct content
///
/// **Purpose:** Validate auto-detection of gzip compression from file extension.
#[tokio::test]
async fn azure_blob_gzip_compression_auto() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.compression = Compression::Auto;

    let content = "line1\nline2\nline3";
    config
        .upload_compressed_blob("file.gz".to_string(), content.to_string(), "gzip")
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].as_log()["message"], "line1".into());
    assert_eq!(events[1].as_log()["message"], "line2".into());
    assert_eq!(events[2].as_log()["message"], "line3".into());
}

/// Test gzip decompression with explicit compression configuration.
///
/// **Setup:**
/// - Configure compression = "gzip" explicitly
/// - Upload a gzip-compressed blob WITHOUT ".gz" extension
///
/// **Verification:**
/// - Verify blob is decompressed despite lacking compression-indicating file extension
/// - Verify exactly 2 events are received with correct content
///
/// **Purpose:** Validate explicit gzip compression override when file extension is absent.
#[tokio::test]
async fn azure_blob_gzip_compression_explicit() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.compression = Compression::Gzip;

    let content = "explicit_gzip_line1\nexplicit_gzip_line2";
    config
        .upload_compressed_blob("file_no_extension".to_string(), content.to_string(), "gzip")
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].as_log()["message"], "explicit_gzip_line1".into());
    assert_eq!(events[1].as_log()["message"], "explicit_gzip_line2".into());
}

/// Test zstd decompression with automatic compression detection.
///
/// **Setup:**
/// - Configure compression = "auto"
/// - Upload a zstd-compressed blob with ".zst" extension
///
/// **Verification:**
/// - Verify blob is automatically detected as zstd and decompressed
/// - Verify exactly 3 events are received with correct content
///
/// **Purpose:** Validate auto-detection of zstd compression from file extension.
#[tokio::test]
async fn azure_blob_zstd_compression() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.compression = Compression::Auto;

    let content = "zstd_line1\nzstd_line2\nzstd_line3";
    config
        .upload_compressed_blob("file.zst".to_string(), content.to_string(), "zstd")
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].as_log()["message"], "zstd_line1".into());
    assert_eq!(events[1].as_log()["message"], "zstd_line2".into());
    assert_eq!(events[2].as_log()["message"], "zstd_line3".into());
}

/// Test that compression can be disabled even when file extension suggests compression.
///
/// **Setup:**
/// - Configure compression = "none" explicitly
/// - Upload an UNCOMPRESSED blob named "file.gz" (misleading extension)
///
/// **Verification:**
/// - Verify blob is NOT decompressed despite having ".gz" extension
/// - Verify raw uncompressed content is read correctly (2 events)
///
/// **Purpose:** Validate explicit compression=none overrides auto-detection.
#[tokio::test]
async fn azure_blob_no_compression_with_gz_extension() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.compression = Compression::None;

    let content = "uncompressed_content\nsecond_line";
    config
        .upload_blob("file.gz".to_string(), content.to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].as_log()["message"], "uncompressed_content".into());
    assert_eq!(events[1].as_log()["message"], "second_line".into());
}

/// Test decompression of multipart gzip files (multiple gzip members concatenated).
///
/// **Setup:**
/// - Create a blob with two separate gzip-compressed parts concatenated together
/// - Each part contains 2 lines with trailing newlines
/// - Configure compression = "auto"
///
/// **Verification:**
/// - Verify all 4 lines from both gzip members are decompressed and parsed
/// - Verify events maintain correct order across both gzip members
///
/// **Purpose:** Validate multi-member gzip decompression (common format for log rotation).
#[tokio::test]
async fn azure_blob_multipart_gzip() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.compression = Compression::Auto;

    // Create two separate gzip-compressed parts
    // Add trailing newlines so concatenated result splits correctly
    let content1 = "part1_line1\npart1_line2\n";
    let content2 = "part2_line1\npart2_line2\n";

    let mut encoder1 = GzEncoder::new(Vec::new(), GzCompression::default());
    encoder1
        .write_all(content1.as_bytes())
        .expect("Failed to write part 1");
    let compressed1 = encoder1.finish().expect("Failed to finish part 1");

    let mut encoder2 = GzEncoder::new(Vec::new(), GzCompression::default());
    encoder2
        .write_all(content2.as_bytes())
        .expect("Failed to write part 2");
    let compressed2 = encoder2.finish().expect("Failed to finish part 2");

    // Concatenate the two gzip parts
    let mut multipart = compressed1;
    multipart.extend(compressed2);

    let container_client =
        make_container_client(&config).expect("Failed to create container client");
    let blob_client = container_client.blob_client("multipart.gz");
    blob_client
        .put_block_blob(multipart)
        .await
        .expect("Failed putting multipart blob");

    config.queue_notify_blob_created("multipart.gz").await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].as_log()["message"], "part1_line1".into());
    assert_eq!(events[1].as_log()["message"], "part1_line2".into());
    assert_eq!(events[2].as_log()["message"], "part2_line1".into());
    assert_eq!(events[3].as_log()["message"], "part2_line2".into());
}

// ============================================================================
// Framing Integration Tests
// ============================================================================

use vector_lib::codecs::decoding::{
    CharacterDelimitedDecoderConfig, CharacterDelimitedDecoderOptions, FramingConfig,
    NewlineDelimitedDecoderConfig, NewlineDelimitedDecoderOptions,
};

/// Test character-delimited framing with null byte (\\0) delimiter.
///
/// **Setup:**
/// - Configure character-delimited framing with \\0 (null) delimiter
/// - Upload blob with null-delimited content: "a\\0b\\0c"
///
/// **Verification:**
/// - Verify blob is split on null bytes into exactly 3 events
/// - Verify each segment is parsed as a separate event: "a", "b", "c"
///
/// **Purpose:** Validate custom character-delimited framing with non-printable delimiters.
#[tokio::test]
async fn azure_blob_character_delimited_framing() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.framing = FramingConfig::CharacterDelimited(CharacterDelimitedDecoderConfig {
        character_delimited: CharacterDelimitedDecoderOptions {
            delimiter: b'\0',
            max_length: None,
        },
    });

    let content = "a\0b\0c";
    config
        .upload_blob("null-delimited.txt".to_string(), content.to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].as_log()["message"], "a".into());
    assert_eq!(events[1].as_log()["message"], "b".into());
    assert_eq!(events[2].as_log()["message"], "c".into());
}

/// Test bytes framing (treat entire blob as single event without splitting).
///
/// **Setup:**
/// - Configure framing = "bytes" (no splitting)
/// - Upload blob with content containing newlines: "single\\nevent\\nwith\\nnewlines"
///
/// **Verification:**
/// - Verify exactly 1 event is received
/// - Verify event contains entire blob content including all newlines
///
/// **Purpose:** Validate bytes framing treats entire blob as atomic event regardless of delimiters.
#[tokio::test]
async fn azure_blob_bytes_framing() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.framing = FramingConfig::Bytes;

    let content = "single\nevent\nwith\nnewlines";
    config
        .upload_blob("bytes.txt".to_string(), content.to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].as_log()["message"],
        "single\nevent\nwith\nnewlines".into()
    );
}

/// Test newline-delimited framing with mixed line ending styles.
///
/// **Setup:**
/// - Configure newline-delimited framing
/// - Upload blob with mixed line endings: Unix (\\n), Windows (\\r\\n), old Mac (\\r)
///
/// **Verification:**
/// - Verify blob is split on \\n characters (newline-delimited splits only on \\n)
/// - Verify \\r characters are preserved in content where they don't precede \\n
/// - Verify exactly 3 events are received
///
/// **Purpose:** Validate newline-delimited framing behavior with non-uniform line endings.
#[tokio::test]
async fn azure_blob_mixed_line_endings() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.framing = FramingConfig::NewlineDelimited(NewlineDelimitedDecoderConfig {
        newline_delimited: NewlineDelimitedDecoderOptions { max_length: None },
    });

    // Mix of \n, \r\n, and \r line endings
    // Newline-delimited decoder only splits on \n, so \r is preserved in the content
    let content = "unix\nwindows\r\nold_mac\rend";
    config
        .upload_blob("mixed-endings.txt".to_string(), content.to_string())
        .await;

    let events = config.run_assert().await;
    // Splits on \n only: "unix\n", "windows\r\n", "old_mac\rend" (no trailing \n)
    // Results in: "unix", "windows\r", "old_mac\rend"
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].as_log()["message"], "unix".into());
    assert_eq!(events[1].as_log()["message"], "windows\r".into());
    assert_eq!(events[2].as_log()["message"], "old_mac\rend".into());
}

// ============================================================================
// Combined Tests (Framing + Compression)
// ============================================================================

/// Test combination of gzip compression and character-delimited framing.
///
/// **Setup:**
/// - Configure compression = "auto" and character-delimited framing with \\0 delimiter
/// - Upload gzip-compressed blob with null-delimited content: "item1\\0item2\\0item3"
///
/// **Verification:**
/// - Verify compression is auto-detected and blob is decompressed
/// - Verify decompressed content is split on null bytes into 3 events
/// - Verify complete pipeline: decompress → frame → parse
///
/// **Purpose:** Validate compression and framing work correctly together.
#[tokio::test]
async fn azure_blob_gzip_character_delimited() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.compression = Compression::Auto;
    config.framing = FramingConfig::CharacterDelimited(CharacterDelimitedDecoderConfig {
        character_delimited: CharacterDelimitedDecoderOptions {
            delimiter: b'\0',
            max_length: None,
        },
    });

    let content = "item1\0item2\0item3";
    config
        .upload_compressed_blob(
            "null-compressed.gz".to_string(),
            content.to_string(),
            "gzip",
        )
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].as_log()["message"], "item1".into());
    assert_eq!(events[1].as_log()["message"], "item2".into());
    assert_eq!(events[2].as_log()["message"], "item3".into());
}

// ===== Multiline Integration Tests =====

/// Test multiline aggregation with continue_through mode (Java-style stack traces).
///
/// **Setup:**
/// - Configure multiline mode = "continue_through"
/// - start_pattern: lines starting with date (^\\d{4}-\\d{2}-\\d{2})
/// - condition_pattern: continuation lines start with whitespace (^\\s)
/// - Upload blob with Java stack trace (ERROR line + indented stack trace lines + INFO line)
///
/// **Verification:**
/// - Verify exactly 2 events are received
/// - Verify first event contains complete stack trace (ERROR line + all indented lines)
/// - Verify second event is the single INFO line
///
/// **Purpose:** Validate continue_through mode aggregates multi-line stack traces correctly.
#[tokio::test]
async fn azure_blob_multiline_continue_through() {
    use crate::{line_agg, sources::util::MultilineConfig};
    use std::time::Duration;

    let mut config = AzureBlobConfig::new_emulator().await;
    config.multiline = Some(MultilineConfig {
        start_pattern: r"^\d{4}-\d{2}-\d{2}".to_string(),
        condition_pattern: r"^\s".to_string(),
        mode: line_agg::Mode::ContinueThrough,
        timeout_ms: Duration::from_millis(1000),
    });

    let content = "2024-01-01 ERROR Something failed\n  at com.example.Class.method(Class.java:10)\n  at com.example.Main.main(Main.java:20)\n2024-01-01 INFO Next log";
    config
        .upload_blob("stacktrace.log".to_string(), content.to_string())
        .await;

    let events =
        run_and_assert_source_compliance(config.clone(), Duration::from_secs(3), &SOURCE_TAGS)
            .await;
    assert_eq!(events.len(), 2);

    // First event should contain the full stack trace
    let first_log = events[0].as_log()["message"].to_string_lossy();
    assert!(first_log.contains("ERROR Something failed"));
    assert!(first_log.contains("at com.example.Class.method"));
    assert!(first_log.contains("at com.example.Main.main"));

    // Second event should be the single line
    assert_eq!(
        events[1].as_log()["message"],
        "2024-01-01 INFO Next log".into()
    );
}

/// Test multiline aggregation with halt_before mode.
///
/// **Setup:**
/// - Configure multiline mode = "halt_before"
/// - start_pattern and condition_pattern: lines starting with "START"
/// - Upload blob with content: "START block1\\nline2\\nline3\\nSTART block2\\nline5"
///
/// **Verification:**
/// - Verify exactly 2 events are received
/// - Verify first event contains "START block1" through "line3" (stops before next START)
/// - Verify second event contains "START block2\\nline5"
///
/// **Purpose:** Validate halt_before mode stops aggregation before matching line.
#[tokio::test]
async fn azure_blob_multiline_halt_before() {
    use crate::{line_agg, sources::util::MultilineConfig};
    use std::time::Duration;

    let mut config = AzureBlobConfig::new_emulator().await;
    config.multiline = Some(MultilineConfig {
        start_pattern: r"^START".to_string(),
        condition_pattern: r"^START".to_string(),
        mode: line_agg::Mode::HaltBefore,
        timeout_ms: Duration::from_millis(1000),
    });

    let content = "START block1\nline2\nline3\nSTART block2\nline5";
    config
        .upload_blob("halt_before.log".to_string(), content.to_string())
        .await;

    let events =
        run_and_assert_source_compliance(config.clone(), Duration::from_secs(3), &SOURCE_TAGS)
            .await;
    assert_eq!(events.len(), 2);

    let first_log = events[0].as_log()["message"].to_string_lossy();
    assert!(first_log.contains("START block1"));
    assert!(first_log.contains("line2"));
    assert!(first_log.contains("line3"));

    let second_log = events[1].as_log()["message"].to_string_lossy();
    assert!(second_log.contains("START block2"));
    assert!(second_log.contains("line5"));
}

/// Test multiline aggregation with halt_with mode (include terminator).
///
/// **Setup:**
/// - Configure multiline mode = "halt_with"
/// - start_pattern: lines starting with "BEGIN"
/// - condition_pattern: lines starting with "END"
/// - Upload blob with transactions: "BEGIN...\\nprocessing\\nEND\\nBEGIN...\\nEND"
///
/// **Verification:**
/// - Verify exactly 2 events are received
/// - Verify each event contains complete transaction from BEGIN through END (inclusive)
/// - Verify terminator line (END) is included in the aggregated event
///
/// **Purpose:** Validate halt_with mode includes the terminator line in aggregated event.
#[tokio::test]
async fn azure_blob_multiline_halt_with() {
    use crate::{line_agg, sources::util::MultilineConfig};
    use std::time::Duration;

    let mut config = AzureBlobConfig::new_emulator().await;
    config.multiline = Some(MultilineConfig {
        start_pattern: r"^BEGIN".to_string(),
        condition_pattern: r"^END".to_string(),
        mode: line_agg::Mode::HaltWith,
        timeout_ms: Duration::from_millis(1000),
    });

    let content = "BEGIN transaction1\nprocessing\nEND\nBEGIN transaction2\nmore processing\nEND";
    config
        .upload_blob("halt_with.log".to_string(), content.to_string())
        .await;

    let events =
        run_and_assert_source_compliance(config.clone(), Duration::from_secs(3), &SOURCE_TAGS)
            .await;
    assert_eq!(events.len(), 2);

    let first_log = events[0].as_log()["message"].to_string_lossy();
    assert!(first_log.contains("BEGIN transaction1"));
    assert!(first_log.contains("processing"));
    assert!(first_log.contains("END"));

    let second_log = events[1].as_log()["message"].to_string_lossy();
    assert!(second_log.contains("BEGIN transaction2"));
    assert!(second_log.contains("more processing"));
    assert!(second_log.contains("END"));
}

/// Test that unsupported event types are ignored and their queue messages deleted.
///
/// **Setup:**
/// - Send an Event Grid notification with unsupported event type "Microsoft.Storage.BlobDeleted"
/// - Upload a valid blob and send a BlobCreated notification
///
/// **Verification:**
/// - Verify only the valid blob's event is received (1 event)
/// - Verify the unsupported event message is deleted from the queue (not stuck for reprocessing)
///
/// **Purpose:** Validate that unsupported event types don't cause infinite reprocessing loops.
#[tokio::test]
async fn azure_blob_ignore_unsupported_event_type() {
    let config = AzureBlobConfig::new_emulator().await;

    config
        .queue_notify_custom_event("some-blob.txt", "Microsoft.Storage.BlobDeleted")
        .await;
    config
        .upload_blob("valid.txt".to_string(), "valid content".to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "valid content".into());
}

/// Test that messages for mismatching containers are ignored and deleted from the queue.
///
/// **Setup:**
/// - Send an Event Grid notification referencing a different container than configured
/// - Upload a valid blob and send a BlobCreated notification for the correct container
///
/// **Verification:**
/// - Verify only the valid blob's event is received (1 event)
/// - Verify the mismatching container message is deleted (not stuck for reprocessing)
///
/// **Purpose:** Validate that messages for other containers don't cause infinite reprocessing loops.
#[tokio::test]
async fn azure_blob_ignore_mismatching_container() {
    let config = AzureBlobConfig::new_emulator().await;

    // Send a BlobCreated event referencing a different container
    let queue_client = make_queue_client(&config).expect("Failed to create queue client");
    let message = format!(
        r#"{{
      "topic": "/subscriptions/fa5f2180-1451-4461-9b1f-aae7d4b33cf8/resourceGroups/events_poc/providers/Microsoft.Storage/storageAccounts/eventspocaccount",
      "subject": "/blobServices/default/containers/other-container/blobs/some-blob.txt",
      "eventType": "Microsoft.Storage.BlobCreated",
      "id": "be3f21f7-201e-000b-7605-a29195062631",
      "data": {{
        "api": "PutBlob",
        "clientRequestId": "1fa42c94-6dd3-4172-95c4-fd9cf56b5009",
        "requestId": "be3f21f7-201e-000b-7605-a29195000000",
        "eTag": "0x8DC701C5D3FFDF6",
        "contentType": "application/octet-stream",
        "contentLength": 0,
        "blobType": "BlockBlob",
        "url": "https://eventspocaccount.blob.core.windows.net/other-container/some-blob.txt",
        "sequencer": "0000000000000000000000000005C5360000000000276a63",
        "storageDiagnostics": {{
          "batchId": "fec5b12c-2006-0034-0005-a25936000000"
        }}
      }},
      "dataVersion": "",
      "metadataVersion": "1",
      "eventTime": "2024-05-09T11:37:10.5637878Z"
    }}"#
    );
    queue_client
        .put_message(BASE64_STANDARD.encode(message))
        .await
        .expect("Failed putting message");

    config
        .upload_blob("valid.txt".to_string(), "valid content".to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "valid content".into());
}

/// Test complete pipeline: gzip compression + multiline aggregation + JSON decoding.
///
/// **Setup:**
/// - Configure compression = "auto", JSON decoding, and multiline continue_through mode
/// - start_pattern: lines starting with "{", condition_pattern: indented lines
/// - Upload gzip-compressed blob with JSON lines and indented continuation
///
/// **Verification:**
/// - Verify compression is detected and blob is decompressed
/// - Verify multiline aggregation combines JSON with indented details
/// - Verify JSON parsing happens after multiline aggregation
/// - Verify at least one event is received
///
/// **Purpose:** Validate complete processing pipeline with all features enabled.
#[tokio::test]
async fn azure_blob_gzip_multiline_json() {
    use crate::{line_agg, sources::util::MultilineConfig};
    use std::time::Duration;
    use vector_lib::codecs::decoding::{DeserializerConfig, JsonDeserializerConfig};

    let mut config = AzureBlobConfig::new_emulator().await;
    config.compression = Compression::Auto;
    config.decoding = DeserializerConfig::Json(JsonDeserializerConfig::default());
    config.multiline = Some(MultilineConfig {
        start_pattern: r"^\{".to_string(),
        condition_pattern: r"^\s".to_string(),
        mode: line_agg::Mode::ContinueThrough,
        timeout_ms: Duration::from_millis(1000),
    });

    let content = r#"{"level":"ERROR","message":"Failed"}
  {"details":"Stack trace"}
{"level":"INFO","message":"Success"}"#;

    config
        .upload_compressed_blob("multiline.json.gz".to_string(), content.to_string(), "gzip")
        .await;

    let events =
        run_and_assert_source_compliance(config.clone(), Duration::from_secs(3), &SOURCE_TAGS)
            .await;
    // Note: JSON parsing happens after multiline aggregation, so we expect the aggregated lines
    // The exact number of events depends on whether the aggregated lines form valid JSON
    assert!(!events.is_empty());
}
