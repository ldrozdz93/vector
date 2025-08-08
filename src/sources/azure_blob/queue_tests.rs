use super::queue::{parse_subject, default_poll_secs, num_messages, AzureStorageEvent, make_container_client, make_queue_client, Config, ProcessingError};
use crate::sources::azure_blob::{AzureBlobConfig, Strategy};

// Test parse_subject function with various inputs
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

// Test queue client creation with invalid config
#[test]
fn test_make_queue_client_no_auth() {
    let config = AzureBlobConfig {
        connection_string: None,
        storage_account: None,
        container_name: "test".to_string(),
        strategy: Strategy::StorageQueue,
        queue: Some(Config {
            queue_name: "queue".to_string(),
            poll_secs: 10,
        }),
        endpoint: None,
        client_credentials: None,
        exec_interval_secs: 1,
        log_namespace: None,
        acknowledgements: Default::default(),
        decoding: crate::serde::default_decoding(),
    };

    let result = make_queue_client(&config);
    assert!(result.is_err());
}

// Test container client creation with connection string
#[test]
fn test_make_container_client_with_connection_string() {
    let config = AzureBlobConfig {
        connection_string: Some("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net".to_string().into()),
        storage_account: None,
        container_name: "test-container".to_string(),
        strategy: Strategy::Test,
        queue: None,
        endpoint: None,
        client_credentials: None,
        exec_interval_secs: 1,
        log_namespace: None,
        acknowledgements: Default::default(),
        decoding: crate::serde::default_decoding(),
    };

    let result = make_container_client(&config);
    assert!(result.is_ok());
    let client = result.unwrap();
    assert_eq!(client.container_name(), "test-container");
}

// Test subject parsing edge cases
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
    // Path traversal attempts are preserved as-is (Azure will handle security)
    assert_eq!(blob, "../../../etc/passwd");
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
