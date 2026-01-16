use std::sync::Arc;

pub type AckFn = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> + Send + Sync>;
pub type NackFn = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> + Send + Sync>;

/// Context for a Pub/Sub message containing acknowledgment operations.
///
/// This context is attached to each task and provides methods to acknowledge
/// or negatively acknowledge messages after processing.
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
///     // Manual ack/nack if not using AcknowledgeLayer
///     // task.parts.ctx.ack().await.unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct PubSubContext {
    /// The acknowledgment ID for the message
    pub ack_id: String,
    /// Function to call to acknowledge the message
    ack_fn: AckFn,
    /// Function to call to nack the message
    nack_fn: NackFn,
}

impl std::fmt::Debug for PubSubContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubContext")
            .field("ack_id", &self.ack_id)
            .finish()
    }
}

impl Default for PubSubContext {
    fn default() -> Self {
        Self {
            ack_id: String::new(),
            ack_fn: Arc::new(|| Box::pin(async { Ok(()) })),
            nack_fn: Arc::new(|| Box::pin(async { Ok(()) })),
        }
    }
}

impl PubSubContext {
    /// Creates a new `PubSubContext` instance with the given parameters.
    pub fn new(
        ack_id: String,
        ack_fn: AckFn,
        nack_fn: NackFn,
    ) -> Self {
        Self {
            ack_id,
            ack_fn,
            nack_fn,
        }
    }

    /// Acknowledges the message
    pub async fn ack(&self) -> Result<(), crate::PubSubError> {
        (self.ack_fn)()
            .await
            .map_err(|e| crate::PubSubError::AckFailed(e))
    }

    /// Negatively acknowledges the message (requeue for redelivery)
    pub async fn nack(&self) -> Result<(), crate::PubSubError> {
        (self.nack_fn)()
            .await
            .map_err(|e| crate::PubSubError::AckFailed(e))
    }
}
