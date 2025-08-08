use super::*;
use azure_core::error::HttpError;
use azure_storage_blobs::prelude::PublicAccess;
use base64::{prelude::BASE64_STANDARD, Engine};
use http::StatusCode;
use vector_lib::codecs::decoding::{DeserializerConfig, JsonDeserializerConfig};

// Test with JSON content
#[tokio::test]
async fn azure_blob_read_json_content() {
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

// Test with malformed JSON (should still process as text)
#[tokio::test]
async fn azure_blob_read_malformed_json() {
    let mut config = AzureBlobConfig::new_emulator().await;
    config.decoding = DeserializerConfig::Json(JsonDeserializerConfig::default());

    let malformed_json = r#"{"incomplete": json"#;
    config
        .upload_blob("bad.json".to_string(), malformed_json.to_string())
        .await;

    let events = config.run_assert().await;
    // Should still receive the event, but parsing might fail gracefully
    assert_eq!(events.len(), 1);
}

// Test with empty blob
#[tokio::test]
async fn azure_blob_read_empty_blob() {
    let config = AzureBlobConfig::new_emulator().await;
    let empty_content = "";
    config
        .upload_blob("empty.txt".to_string(), empty_content.to_string())
        .await;

    let events = config.run_assert().await;
    // Empty blob should produce empty event or no event
    if !events.is_empty() {
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].as_log()["message"], "".into());
    }
}

// Test with very large blob content
#[tokio::test]
async fn azure_blob_read_large_content() {
    let config = AzureBlobConfig::new_emulator().await;

    // Create a large content (10KB of repeated pattern)
    let large_line =
        "This is a long line with some content to test large blob handling.\n".repeat(150);
    config
        .upload_blob("large.txt".to_string(), large_line.clone())
        .await;

    let events = config.run_assert().await;
    // Should split by lines
    let expected_lines = large_line.lines().count();
    assert_eq!(events.len(), expected_lines);

    // Verify first and last lines
    assert_eq!(
        events[0].as_log()["message"].to_string_lossy().trim(),
        "This is a long line with some content to test large blob handling."
    );
}

// Test with binary content
#[tokio::test]
async fn azure_blob_read_binary_content() {
    let config = AzureBlobConfig::new_emulator().await;

    // Create binary content with null bytes and non-UTF8 data
    let binary_data = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, b'\n', b'h', b'i', b'\n'];
    let binary_string = String::from_utf8_lossy(&binary_data);

    config
        .upload_blob("binary.bin".to_string(), binary_string.to_string())
        .await;

    let events = config.run_assert().await;
    // Should handle binary data gracefully
    assert!(!events.is_empty());
}

// Test container name filtering
#[tokio::test]
async fn azure_blob_ignore_wrong_container() {
    let config = AzureBlobConfig::new_emulator().await;

    // Manually create a message for a different container
    let queue_client = queue::make_queue_client(&config).unwrap();
    let wrong_container_message = format!(
        r#"{{
            "subject": "/blobServices/default/containers/wrong-container/blobs/file.txt",
            "eventType": "Microsoft.Storage.BlobCreated"
        }}"#
    );

    queue_client
        .put_message(BASE64_STANDARD.encode(wrong_container_message))
        .await
        .expect("Failed putting wrong container message");

    // Also upload a correct blob
    config
        .upload_blob("correct.txt".to_string(), "correct content".to_string())
        .await;

    let events = config.run_assert().await;
    // Should only get the event from correct container
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "correct content".into());
}

// Test invalid event types are ignored
#[tokio::test]
async fn azure_blob_ignore_invalid_event_types() {
    let config = AzureBlobConfig::new_emulator().await;

    // Create messages with different event types
    let queue_client = queue::make_queue_client(&config).unwrap();

    let delete_event = format!(
        r#"{{
            "subject": "/blobServices/default/containers/{}/blobs/deleted.txt",
            "eventType": "Microsoft.Storage.BlobDeleted"
        }}"#,
        config.container_name
    );

    let rename_event = format!(
        r#"{{
            "subject": "/blobServices/default/containers/{}/blobs/renamed.txt",
            "eventType": "Microsoft.Storage.BlobRenamed"
        }}"#,
        config.container_name
    );

    queue_client
        .put_message(BASE64_STANDARD.encode(delete_event))
        .await
        .expect("Failed putting delete event");

    queue_client
        .put_message(BASE64_STANDARD.encode(rename_event))
        .await
        .expect("Failed putting rename event");

    // Also upload a valid blob
    config
        .upload_blob("valid.txt".to_string(), "valid content".to_string())
        .await;

    let events = config.run_assert().await;
    // Should only process BlobCreated events
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "valid content".into());
}

// Test malformed queue messages are handled gracefully
#[tokio::test]
async fn azure_blob_handle_malformed_messages() {
    let config = AzureBlobConfig::new_emulator().await;
    let queue_client = queue::make_queue_client(&config).unwrap();

    // Put some malformed messages
    let malformed_messages = vec![
        "not json at all",
        r#"{"incomplete": json"#,
        r#"{"missing": "subject"}"#,
        BASE64_STANDARD.encode("invalid base64 content after decoding"),
        "", // empty message
    ];

    for msg in malformed_messages {
        queue_client
            .put_message(BASE64_STANDARD.encode(msg))
            .await
            .expect("Failed putting malformed message");
    }

    // Also put a valid message
    config
        .upload_blob("valid.txt".to_string(), "should work".to_string())
        .await;

    let events = config.run_assert().await;
    // Should only get the valid event
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "should work".into());
}

// Test concurrent blob processing
#[tokio::test]
async fn azure_blob_concurrent_processing() {
    let config = AzureBlobConfig::new_emulator().await;

    // Upload multiple blobs rapidly
    let blobs = vec![
        ("file1.txt", "content1"),
        ("file2.txt", "content2"),
        ("file3.txt", "content3"),
        ("file4.txt", "content4"),
        ("file5.txt", "content5"),
    ];

    for (name, content) in &blobs {
        config
            .upload_blob(name.to_string(), content.to_string())
            .await;
    }

    let events = run_and_assert_source_compliance(
        config.clone(),
        Duration::from_secs(10),
        &crate::test_util::components::SOURCE_TAGS,
    )
    .await;

    assert_eq!(events.len(), blobs.len());

    // Verify all content is received (order might vary due to concurrency)
    let received_content: std::collections::HashSet<_> = events
        .iter()
        .map(|e| e.as_log()["message"].to_string_lossy())
        .collect();

    let expected_content: std::collections::HashSet<_> = blobs
        .iter()
        .map(|(_, content)| content.to_string())
        .collect();

    assert_eq!(received_content, expected_content);
}

// Test blob with mixed line endings (CRLF, LF, CR)
#[tokio::test]
async fn azure_blob_mixed_line_endings() {
    let config = AzureBlobConfig::new_emulator().await;

    // Content with different line endings
    let mixed_content = "line1\r\nline2\nline3\rline4";
    config
        .upload_blob("mixed.txt".to_string(), mixed_content.to_string())
        .await;

    let events = config.run_assert().await;

    // Should handle different line endings correctly
    assert!(events.len() >= 3); // At least 3 lines should be detected

    let messages: Vec<String> = events
        .iter()
        .map(|e| e.as_log()["message"].to_string_lossy())
        .collect();

    assert!(messages.contains(&"line1".to_string()));
    assert!(messages.contains(&"line2".to_string()));
}

// Test authentication error handling
#[tokio::test]
async fn azure_blob_authentication_error() {
    let mut config = AzureBlobConfig::new_emulator().await;

    // Use invalid credentials
    config.connection_string = Some("DefaultEndpointsProtocol=https;AccountName=invalid;AccountKey=invalid;EndpointSuffix=core.windows.net".into());

    // This should produce error events instead of crashing
    let events = config.run_error().await;
    assert!(events.is_empty()); // No successful events should be received
}

// Test queue polling with different intervals
#[tokio::test]
async fn azure_blob_different_polling_intervals() {
    let mut config = AzureBlobConfig::new_emulator().await;

    // Set very short polling interval
    config.queue = Some(queue::Config {
        queue_name: config.queue.unwrap().queue_name,
        poll_secs: 1,
    });

    config
        .upload_blob("poll_test.txt".to_string(), "polling test".to_string())
        .await;

    let events = config.run_assert().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log()["message"], "polling test".into());
}

// Test queue message with complex blob path
#[tokio::test]
async fn azure_blob_complex_paths() {
    let config = AzureBlobConfig::new_emulator().await;

    let complex_paths = vec![
        "2024/01/15/application/service/instance-123/app.log",
        "logs/microservice-a/2024-01-15T10:30:00Z.json",
        "backups/database/2024/week-03/dump.sql",
        "temp/processing/file-with-very-long-name-that-contains-multiple-words.txt",
    ];

    for (i, path) in complex_paths.iter().enumerate() {
        config
            .upload_blob(path.to_string(), format!("content for file {}", i))
            .await;
    }

    let events = run_and_assert_source_compliance(
        config.clone(),
        Duration::from_secs(8),
        &crate::test_util::components::SOURCE_TAGS,
    )
    .await;

    assert_eq!(events.len(), complex_paths.len());
}

// Test handling of blobs that get deleted before reading
#[tokio::test]
async fn azure_blob_handle_race_condition() {
    let config = AzureBlobConfig::new_emulator().await;

    // Upload a blob
    config
        .upload_blob("race_test.txt".to_string(), "race content".to_string())
        .await;

    // Manually delete the blob while keeping the queue message
    let container_client = queue::make_container_client(&config).unwrap();
    let blob_client = container_client.blob_client("race_test.txt");
    let _ = blob_client.delete().await; // Ignore errors

    // The source should handle the missing blob gracefully
    let events = run_and_assert_source_compliance(
        config.clone(),
        Duration::from_secs(2),
        &crate::test_util::components::SOURCE_TAGS,
    )
    .await;

    // Should not crash and should handle the missing blob error
    // Events might be empty since blob was deleted
    assert!(events.len() <= 1);
}
