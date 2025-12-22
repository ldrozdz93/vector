use super::*;
use crate::{
    config::LogNamespace, event::EventStatus, serde::default_decoding, shutdown::ShutdownSignal,
    test_util::collect_n, SourceSender,
};
use tokio::{select, sync::oneshot};

#[tokio::test]
async fn test_messages_delivered() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
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
            let lines = vec!["foo", "bar"];
            for line in lines {
                yield line.as_bytes().to_vec();
            }
        }),
        success_handler: Box::new(move || {
            Box::pin(async move {
                success_sender.send(()).unwrap();
            })
        }),
    };
    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 2).await).unwrap();
    });
    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing blob pack");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };
    assert_eq!(events[0].as_log().value().to_string(), "\"foo\"");
    assert_eq!(events[1].as_log().value().to_string(), "\"bar\"");
    select! {
        _ = success_receiver => {}
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for success handler"),
    }
}

#[tokio::test]
async fn test_messages_rejected() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Rejected);
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
            let lines = vec!["foo", "bar"];
            for line in lines {
                yield line.as_bytes().to_vec();
            }
        }),
        success_handler: Box::new(move || {
            Box::pin(async move {
                success_sender.send(()).unwrap();
            })
        }),
    };
    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 2).await).unwrap();
    });
    streamer
        .process_blob_pack(blob_pack)
        .await
        .expect("Failed processing blob pack");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };
    assert_eq!(events[0].as_log().value().to_string(), "\"foo\"");
    assert_eq!(events[1].as_log().value().to_string(), "\"bar\"");
    assert!(success_receiver.try_recv().is_err()); // assert success handler not called
}

// Test empty blob pack stream
#[tokio::test]
async fn test_empty_blob_pack() {
    use tokio::{select, time};

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
    use tokio::{select, time};

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
    use tokio::{select, time};
    use vector_lib::codecs::decoding::{DeserializerConfig, JsonDeserializerConfig};
    use vrl::value;

    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        DeserializerConfig::Json(JsonDeserializerConfig::default()),
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    let json_line = r#"{"level":"info","message":"test log","timestamp":"2023-01-01T00:00:00Z"}"#;
    let blob_pack = BlobPack {
        row_stream: Box::pin(stream! {
            yield json_line.as_bytes().to_vec();
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
    assert_eq!(log["level"], value!("info"));
    assert_eq!(log["message"], value!("test log"));
    assert_eq!(log["timestamp"], value!("2023-01-01T00:00:00Z"));
}

// Test config validation
#[tokio::test]
async fn test_config_validation() {
    // Test invalid config - missing queue for StorageQueue strategy
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
    use tokio::{select, time};

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
