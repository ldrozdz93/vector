use azure_storage_blobs::prelude::PublicAccess;
use base64::{prelude::BASE64_STANDARD, Engine};

use std::time::Duration;

use super::{
    queue::{make_container_client, make_queue_client, Config},
    AzureBlobConfig,
};
use crate::{
    event::Event,
    serde::default_decoding,
    test_util::components::{
        run_and_assert_source_compliance, run_and_assert_source_error, COMPONENT_ERROR_TAGS,
        SOURCE_TAGS,
    },
};

impl AzureBlobConfig {
    pub async fn new_emulator() -> AzureBlobConfig {
        let address = std::env::var("AZURE_ADDRESS").unwrap_or_else(|_| "localhost".to_string());
        let config = AzureBlobConfig {
                connection_string: Some(format!("UseDevelopmentStorage=true;DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{}:10000/devstoreaccount1;QueueEndpoint=http://{}:10001/devstoreaccount1;TableEndpoint=http://{}:10002/devstoreaccount1;", address, address, address).into()),
                container_name: "logs".to_string(),
                queue: Some(Config {
                    queue_name: format!("test-{}", rand::random::<u32>()),
                    poll_secs: 1,
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
                if error_msg.contains("409") || error_msg.contains("conflict") || error_msg.contains("already exists") {
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
                if error_msg.contains("409") || error_msg.contains("conflict") || error_msg.contains("already exists") {
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
}

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
    });

    let events = config.run_error().await;
    assert!(events.is_empty());
}

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

// Test with JSON content
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

// Test error handling with malformed messages
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

// Test with empty blob
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
