use apalis_pubsub::{PubSubConfig, utils::PubSubContext};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test]
async fn test_pubsub_context_ack() {
    let acked = Arc::new(AtomicBool::new(false));
    let acked_clone = acked.clone();

    let ack_fn = Arc::new(move || {
        let acked = acked_clone.clone();
        Box::pin(async move {
            acked.store(true, Ordering::SeqCst);
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let nack_fn = Arc::new(|| {
        Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let ctx = PubSubContext::new("test-ack-id".into(), ack_fn, nack_fn);
    ctx.ack().await.unwrap();

    assert!(acked.load(Ordering::SeqCst), "Message should have been acknowledged");
}

#[tokio::test]
async fn test_pubsub_context_nack() {
    let nacked = Arc::new(AtomicBool::new(false));
    let nacked_clone = nacked.clone();

    let ack_fn = Arc::new(|| {
        Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let nack_fn = Arc::new(move || {
        let nacked = nacked_clone.clone();
        Box::pin(async move {
            nacked.store(true, Ordering::SeqCst);
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let ctx = PubSubContext::new("test-nack-id".into(), ack_fn, nack_fn);
    ctx.nack().await.unwrap();

    assert!(nacked.load(Ordering::SeqCst), "Message should have been negatively acknowledged");
}

#[test]
fn test_config_defaults() {
    let config = PubSubConfig::default();
    assert_eq!(config.buffer_size, 100, "Default buffer size should be 100");
    assert_eq!(config.max_message_size, 10 * 1024 * 1024, "Default max message size should be 10MB");
    assert_eq!(config.max_outstanding_messages, None, "Default max outstanding messages should be None");
    assert_eq!(config.max_outstanding_bytes, None, "Default max outstanding bytes should be None");
}

#[test]
fn test_config_custom() {
    let config = PubSubConfig {
        buffer_size: 200,
        max_message_size: 5 * 1024 * 1024,
        max_outstanding_messages: Some(1000),
        max_outstanding_bytes: Some(100 * 1024 * 1024),
    };

    assert_eq!(config.buffer_size, 200);
    assert_eq!(config.max_message_size, 5 * 1024 * 1024);
    assert_eq!(config.max_outstanding_messages, Some(1000));
    assert_eq!(config.max_outstanding_bytes, Some(100 * 1024 * 1024));
}

#[test]
fn test_pubsub_context_default() {
    let ctx = PubSubContext::default();
    assert_eq!(ctx.ack_id, "", "Default ack_id should be empty string");
}

#[tokio::test]
async fn test_pubsub_context_ack_error() {
    let ack_fn = Arc::new(|| {
        Box::pin(async { Err("Ack failed".to_string()) })
            as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let nack_fn = Arc::new(|| {
        Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let ctx = PubSubContext::new("test-error-id".into(), ack_fn, nack_fn);
    let result = ctx.ack().await;

    assert!(result.is_err(), "Ack should fail when the underlying function fails");
}

#[tokio::test]
async fn test_pubsub_context_nack_error() {
    let ack_fn = Arc::new(|| {
        Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let nack_fn = Arc::new(|| {
        Box::pin(async { Err("Nack failed".to_string()) })
            as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let ctx = PubSubContext::new("test-error-id".into(), ack_fn, nack_fn);
    let result = ctx.nack().await;

    assert!(result.is_err(), "Nack should fail when the underlying function fails");
}

#[test]
fn test_pubsub_context_clone() {
    let ack_fn = Arc::new(|| {
        Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let nack_fn = Arc::new(|| {
        Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let ctx1 = PubSubContext::new("test-clone-id".into(), ack_fn, nack_fn);
    let ctx2 = ctx1.clone();

    assert_eq!(ctx1.ack_id, ctx2.ack_id, "Cloned context should have the same ack_id");
}

#[test]
fn test_pubsub_context_debug() {
    let ack_fn = Arc::new(|| {
        Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let nack_fn = Arc::new(|| {
        Box::pin(async { Ok(()) }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    });

    let ctx = PubSubContext::new("test-debug-id".into(), ack_fn, nack_fn);
    let debug_str = format!("{:?}", ctx);

    assert!(debug_str.contains("PubSubContext"), "Debug output should contain struct name");
    assert!(debug_str.contains("test-debug-id"), "Debug output should contain ack_id");
}
