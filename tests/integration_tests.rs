use apalis_pubsub::{utils::PubSubContext, PubSubConfig};

#[test]
fn test_config_defaults() {
    let config = PubSubConfig::default();
    assert_eq!(config.buffer_size, 100, "Default buffer size should be 100");
    assert_eq!(
        config.max_message_size,
        10 * 1024 * 1024,
        "Default max message size should be 10MB"
    );
    assert_eq!(
        config.max_outstanding_messages, None,
        "Default max outstanding messages should be None"
    );
    assert_eq!(
        config.max_outstanding_bytes, None,
        "Default max outstanding bytes should be None"
    );
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
