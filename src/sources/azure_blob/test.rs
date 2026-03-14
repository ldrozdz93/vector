use super::*;
use crate::{
    SourceSender, config::LogNamespace, event::EventStatus, serde::default_decoding,
    shutdown::ShutdownSignal, test_util::collect_n,
};
use std::time::Duration;
use tokio::{select, sync::oneshot, time};

#[tokio::test]
async fn test_messages_delivered() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        true,
        default_framing(),
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");
    let (success_sender, success_receiver) = oneshot::channel();
    let blob = BlobWithAck {
        blob_data_stream: Box::pin(stream! {
            let lines = vec!["foo", "bar"];
            for line in lines {
                yield Bytes::from(line.as_bytes().to_vec());
            }
        }),
        completion_handler: Box::new(move |_result: StreamResult| {
            Box::pin(async move {
                success_sender.send(()).unwrap();
            })
        }),
        container: "test-container".to_string(),
        blob_name: "test-blob.log".to_string(),
    };
    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 2).await).unwrap();
    });
    streamer
        .process_blob(blob)
        .await
        .expect("Failed processing blob");

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
async fn test_messages_rejected_delete_failed_message() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Rejected);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        true, // delete_failed_message
        default_framing(),
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");
    let (success_sender, success_receiver) = oneshot::channel();
    let blob = BlobWithAck {
        blob_data_stream: Box::pin(stream! {
            let lines = vec!["foo", "bar"];
            for line in lines {
                yield Bytes::from(line.as_bytes().to_vec());
            }
        }),
        completion_handler: Box::new(move |_result: StreamResult| {
            Box::pin(async move {
                success_sender.send(()).unwrap();
            })
        }),
        container: "test-container".to_string(),
        blob_name: "test-blob.log".to_string(),
    };
    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 2).await).unwrap();
    });
    streamer
        .process_blob(blob)
        .await
        .expect("Failed processing blob");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };
    assert_eq!(events[0].as_log().value().to_string(), "\"foo\"");
    assert_eq!(events[1].as_log().value().to_string(), "\"bar\"");
    // With delete_failed_message=true, completion handler IS called on rejection
    select! {
        _ = success_receiver => {}
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for completion handler"),
    }
}

#[tokio::test]
async fn test_messages_rejected_retain_failed_message() {
    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Rejected);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        false, // delete_failed_message = false: retain on rejection
        default_framing(),
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");
    let (success_sender, mut success_receiver) = oneshot::channel();
    let blob = BlobWithAck {
        blob_data_stream: Box::pin(stream! {
            let lines = vec!["foo", "bar"];
            for line in lines {
                yield Bytes::from(line.as_bytes().to_vec());
            }
        }),
        completion_handler: Box::new(move |_result: StreamResult| {
            Box::pin(async move {
                success_sender.send(()).unwrap();
            })
        }),
        container: "test-container".to_string(),
        blob_name: "test-blob.log".to_string(),
    };
    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 2).await).unwrap();
    });
    streamer
        .process_blob(blob)
        .await
        .expect("Failed processing blob");

    let events = select! {
        value = events_receiver => value.expect("Failed to receive events"),
        _ = time::sleep(Duration::from_secs(5)) => panic!("Timeout waiting for events"),
    };
    assert_eq!(events[0].as_log().value().to_string(), "\"foo\"");
    assert_eq!(events[1].as_log().value().to_string(), "\"bar\"");
    // With delete_failed_message=false, completion handler is NOT called on rejection
    assert!(success_receiver.try_recv().is_err());
}

// Test blob with JSON decoding
#[tokio::test]
async fn test_json_decoding_blob() {
    use tokio::{select, time};
    use vector_lib::codecs::decoding::{DeserializerConfig, JsonDeserializerConfig};
    use vrl::value;

    let (tx, rx) = SourceSender::new_test_finalize(EventStatus::Delivered);
    let streamer = super::AzureBlobStreamer::new(
        ShutdownSignal::noop(),
        tx,
        LogNamespace::Vector,
        true,
        true,
        default_framing(),
        DeserializerConfig::Json(JsonDeserializerConfig::default()),
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    let json_line = r#"{"level":"info","message":"test log","timestamp":"2023-01-01T00:00:00Z"}"#;
    let blob = BlobWithAck {
        blob_data_stream: Box::pin(stream! {
            yield Bytes::from(json_line.as_bytes().to_vec());
        }),
        completion_handler: Box::new(|_result: StreamResult| Box::pin(async move {})),
        container: "test-container".to_string(),
        blob_name: "json-blob.log".to_string(),
    };

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 1).await).unwrap();
    });

    streamer
        .process_blob(blob)
        .await
        .expect("Failed processing blob with JSON");

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
        true,
        default_framing(),
        default_decoding(),
    );
    let mut streamer = streamer.expect("Failed to create streamer");

    let blob = BlobWithAck {
        blob_data_stream: Box::pin(stream! {
            yield Bytes::from("legacy_test".as_bytes().to_vec());
        }),
        completion_handler: Box::new(|_result: StreamResult| Box::pin(async move {})),
        container: "test-container".to_string(),
        blob_name: "legacy-blob.log".to_string(),
    };

    let (events_collector, events_receiver) = oneshot::channel();
    tokio::spawn(async move {
        events_collector.send(collect_n(rx, 1).await).unwrap();
    });

    streamer
        .process_blob(blob)
        .await
        .expect("Failed processing blob");

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

// ===== Compression Helper Tests =====

#[test]
fn test_compression_detection_from_extensions() {
    // Test various file extensions
    assert_eq!(
        super::determine_compression(None, "file.gz"),
        Some(super::Compression::Gzip)
    );
    assert_eq!(
        super::determine_compression(None, "file.tar.gz"),
        Some(super::Compression::Gzip)
    );
    assert_eq!(
        super::determine_compression(None, "/path/to/file.gz"),
        Some(super::Compression::Gzip)
    );
    assert_eq!(
        super::determine_compression(None, "file.zst"),
        Some(super::Compression::Zstd)
    );
    assert_eq!(super::determine_compression(None, "file.txt"), None);
    assert_eq!(super::determine_compression(None, "file"), None);
}

#[test]
fn test_content_type_to_compression() {
    // Test gzip variants
    assert_eq!(
        super::content_type_to_compression("application/gzip"),
        Some(super::Compression::Gzip)
    );
    assert_eq!(
        super::content_type_to_compression("application/x-gzip"),
        Some(super::Compression::Gzip)
    );

    // Test zstd
    assert_eq!(
        super::content_type_to_compression("application/zstd"),
        Some(super::Compression::Zstd)
    );

    // Test unknown types
    assert_eq!(super::content_type_to_compression("text/plain"), None);
    assert_eq!(super::content_type_to_compression("application/json"), None);
}

#[test]
fn test_compression_detection_with_content_type() {
    // Content-Type alone
    assert_eq!(
        super::determine_compression(Some("application/gzip"), "file.txt"),
        Some(super::Compression::Gzip)
    );

    // Content-Type takes priority over file extension
    assert_eq!(
        super::determine_compression(Some("application/zstd"), "file.gz"),
        Some(super::Compression::Zstd)
    );

    // Content-Type with x-gzip variant
    assert_eq!(
        super::determine_compression(Some("application/x-gzip"), "file.txt"),
        Some(super::Compression::Gzip)
    );

    // Unknown content type falls back to file extension
    assert_eq!(
        super::determine_compression(Some("text/plain"), "file.gz"),
        Some(super::Compression::Gzip)
    );
}
