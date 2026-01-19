use apalis::{layers::retry::RetryPolicy, prelude::*};
use apalis_codec::json::JsonCodec;
use apalis_pubsub::{PubSubBackend, PubSubCompact, PubSubConfig};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use google_cloud_pubsub::client::ClientConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage(usize);

async fn test_job(job: TestMessage, count: Data<Arc<AtomicUsize>>) {
    let current = count.fetch_add(1, Ordering::SeqCst);
    println!(
        "Processing job TestMessage({}), count is now: {}",
        job.0,
        current + 1
    );
}

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();

    let config = ClientConfig::default().with_auth().await.unwrap();

    // Example: Using custom configuration
    // You can also use PubSubBackend::new_from_config() for default settings
    let custom_config = PubSubConfig {
        buffer_size: 200,
        max_message_size: 5 * 1024 * 1024, // 5MB
        max_outstanding_messages: Some(1000),
        max_outstanding_bytes: Some(100 * 1024 * 1024), // 100MB
    };

    let mut ps: PubSubBackend<TestMessage, JsonCodec<PubSubCompact>> =
        PubSubBackend::new_with_config(
            config,
            "test-topic1".to_string(),
            "test-subscription1".to_string(),
            custom_config,
        )
        .await
        .unwrap();

    // Push some test jobs to the topic
    ps.push(TestMessage(42)).await.unwrap();
    ps.push(TestMessage(100)).await.unwrap();
    println!("Pushed 2 test messages to the topic");

    // Build and run the worker
    let worker = WorkerBuilder::new("rango-amigo")
        .backend(ps)
        .data(Arc::new(AtomicUsize::new(0)))
        .retry(RetryPolicy::retries(5))
        .build(test_job);

    // In a real application, you might want to handle graceful shutdown
    // tokio::select! {
    //     _ = worker.run() => {},
    //     _ = tokio::signal::ctrl_c() => {
    //         println!("Shutting down gracefully...");
    //         ps.shutdown();
    //     }
    // }

    worker.run().await.unwrap();
}
