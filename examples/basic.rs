use apalis::{layers::retry::RetryPolicy, prelude::*};
use apalis_pubsub::PubSubBackend;
use serde::{Deserialize, Serialize};

use google_cloud_pubsub::client::ClientConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage(usize);

async fn test_job(job: TestMessage, count: Data<usize>) {
    dbg!(job);
    dbg!(count);
}

#[tokio::main]
async fn main() {
    let config = ClientConfig::default().with_auth().await.unwrap();

    let mut ps = PubSubBackend::new_from_config(config, "test-topic".to_string()).await;
    // add some jobs
    ps.push(TestMessage(42)).await.unwrap();
    WorkerBuilder::new("rango-amigo")
        .backend(ps)
        .data(0usize)
        .retry(RetryPolicy::retries(5))
        .build(test_job)
        .run()
        .await
        .unwrap();
}
