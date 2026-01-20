/// Context for a Pub/Sub message containing acknowledgment data.
///
/// # Example
///
/// ```no_run
/// # use apalis_pubsub::PubSubTask;
/// # use serde::{Deserialize, Serialize};
/// #
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # struct MyJob { id: u64 }
/// #
/// async fn my_handler(job: MyJob, task: PubSubTask<MyJob>) {
///     // Process the job
///     println!("Processing: {:?}", job);
///
///     // Fetch pub/sub ack id
///     // task.parts.ctx.ack_id;
/// }
/// ```
#[derive(Clone, Debug, Default)]
pub struct PubSubContext {
    /// The acknowledgment ID for the message
    pub ack_id: String,
}

impl PubSubContext {
    /// Creates a new `PubSubContext` instance with the given parameters.
    pub fn new(ack_id: String) -> Self {
        Self { ack_id }
    }
}
