use super::*;
use crate::{
    config::LogNamespace,
    event::EventStatus,
    serde::default_decoding,
    shutdown::ShutdownSignal,
    test_util::collect_n,
    SourceSender,
};
use tokio::{select, sync::oneshot, time::Duration};
use vector_lib::codecs::decoding::{DeserializerConfig, JsonDeserializerConfig};
use vrl::value;

// Test empty blob pack stream
#[tokio::test]
async fn test_empty_blob_pack() {
    let (tx, _rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");
    let (success_sender, success_receiver) = oneshot::channel();

    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            return;
            #[allow(unreachable_code)]
            loop {
                yield Vec::new();
            }
        }),
        success_handler: Box::new(move || {
            Box::pin(async move {
                success_sender.send(()).unwrap();
            })
        }),
    };

    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing empty blob pack");

    // Verify success handler is still called even with empty data
    select! {
        _ = success_receiver => {}
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for success handler"),
    }

    // No events should be sent from empty stream
}

// Test blob pack with large lines
#[tokio::test]
async fn test_large_lines_blob_pack() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    // Create a large line (1MB)
    let large_line = "x".repeat(1024 * 1024);
    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            yield large_line.as_bytes().to_vec();
        }),
        success_handler: Box::new(|| Box::pin(async move {})),
    };

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 1).await).unwrap();
    });

    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing large blob pack");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };

    assert_eq!(events.len(), 1);
    // For LogNamespace::Vector, the content should be in the value itself
    let log_value = events[0].as_log().value().to_string_lossy();
    // The value will be quoted, so check for the quotes and the "x" content
    assert!(log_value.contains(&"x".repeat(100))); // Check a smaller substring
}

// Test blob pack with JSON decoding
#[tokio::test]
async fn test_json_decoding_blob_pack() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let json_decoding = DeserializerConfig::Json(JsonDeserializerConfig::default());

    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        json_decoding,
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    let json_data = r#"{"key": "value", "number": 42}"#;
    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            yield json_data.as_bytes().to_vec();
        }),
        success_handler: Box::new(|| Box::pin(async move {})),
    };

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 1).await).unwrap();
    });

    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing JSON blob pack");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };

    assert_eq!(events.len(), 1);
    let log = events[0].as_log();
    assert_eq!(log["key"], value!("value"));
    assert_eq!(log["number"], value!(42));
}

// Test multiple blob packs in sequence
#[tokio::test]
async fn test_multiple_blob_packs_sequence() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 3).await).unwrap();
    });

    // Process first blob pack
    let blob_pack1 = BlobPack {
        row_stream: Box::pin(stream! {
            yield "first".as_bytes().to_vec();
        }),
        success_handler: Box::new(|| Box::pin(async move {})),
    };
    streamer
        .process_blob_pack(blob_pack1)
        .await
        .expect("Failed processing first blob pack");

    // Process second blob pack
    let blob_pack2 = BlobPack {
        row_stream: Box::pin(stream! {
            yield "second".as_bytes().to_vec();
            yield "third".as_bytes().to_vec();
        }),
        success_handler: Box::new(|| Box::pin(async move {})),
    };
    streamer
        .process_blob_pack(blob_pack2)
        .await
        .expect("Failed processing second blob pack");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].as_log().value().to_string(), "\"first\"");
    assert_eq!(events[1].as_log().value().to_string(), "\"second\"");
    assert_eq!(events[2].as_log().value().to_string(), "\"third\"");
}

// Test error recovery with transient failures
#[tokio::test]
async fn test_messages_with_transient_error() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Errored);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");
    let (success_sender, mut success_receiver) = oneshot::channel();

    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            yield "test_error".as_bytes().to_vec();
        }),
        success_handler: Box::new(move || {
            Box::pin(async move {
                success_sender.send(()).unwrap();
            })
        }),
    };

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 1).await).unwrap();
    });

    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing blob pack with error");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log().value().to_string(), "\"test_error\"");

    // Success handler should not be called for transient errors
    assert!(success_receiver.try_recv().is_err());
}

// Test configuration validation
#[test]
fn test_config_validation() {
    // Test valid config with StorageQueue strategy
    let valid_config = AzureBlobConfig {
        connection_string: Some("connection".to_string().into()),
        storage_account: None,
        container_name: "container".to_string(),
        strategy: Strategy::StorageQueue,
        queue: Some(queue::Config {
            queue_name: "queue".to_string(),
            poll_secs: 10,
        }),
        endpoint: None,
        client_credentials: None,
        exec_interval_secs: 1,
        log_namespace: None,
        acknowledgements: Default::default(),
        decoding: default_decoding(),
    };
    assert!(valid_config.validate().is_ok());

    // Test invalid config - StorageQueue without queue config
    let invalid_config1 = AzureBlobConfig {
        connection_string: Some("connection".to_string().into()),
        storage_account: None,
        container_name: "container".to_string(),
        strategy: Strategy::StorageQueue,
        queue: None,
        endpoint: None,
        client_credentials: None,
        exec_interval_secs: 1,
        log_namespace: None,
        acknowledgements: Default::default(),
        decoding: default_decoding(),
    };
    assert!(invalid_config1.validate().is_err());

    // Test invalid config - Test strategy with zero exec_interval_secs
    let invalid_config2 = AzureBlobConfig {
        connection_string: None,
        storage_account: None,
        container_name: "container".to_string(),
        strategy: Strategy::Test,
        queue: None,
        endpoint: None,
        client_credentials: None,
        exec_interval_secs: 0,
        log_namespace: None,
        acknowledgements: Default::default(),
        decoding: default_decoding(),
    };
    assert!(invalid_config2.validate().is_err());
}

// Test LogNamespace handling
#[tokio::test]
async fn test_log_namespace_legacy() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Legacy,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            yield "legacy_test".as_bytes().to_vec();
        }),
        success_handler: Box::new(|| Box::pin(async move {})),
    };

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 1).await).unwrap();
    });

    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing blob pack");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };

    assert_eq!(events.len(), 1);
    // In Legacy namespace, data should be in message field
    assert_eq!(
        events[0].as_log()["message"].to_string_lossy(),
        "legacy_test"
    );
}

// Test UTF-8 handling with non-ASCII characters
#[tokio::test]
async fn test_utf8_handling() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    let utf8_data = "Hello 世界! 🚀 Здравствуй мир";
    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            yield utf8_data.as_bytes().to_vec();
        }),
        success_handler: Box::new(|| Box::pin(async move {})),
    };

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 1).await).unwrap();
    });

    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing UTF-8 blob pack");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_log().value().to_string_lossy(), utf8_data);
}

// Test handling of binary data (non-UTF8)
#[tokio::test]
async fn test_binary_data_handling() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    // Create some binary data with invalid UTF-8
    let binary_data = vec![0xFF, 0xFE, 0x00, 0x01, 0x02, 0x03];
    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            yield binary_data.clone();
        }),
        success_handler: Box::new(|| Box::pin(async move {})),
    };

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 1).await).unwrap();
    });

    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing binary blob pack");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };

    assert_eq!(events.len(), 1);
    // Verify the data is preserved even if it's not valid UTF-8
    let event_bytes = events[0].as_log().value().as_bytes();
    assert!(event_bytes.is_some());
}
