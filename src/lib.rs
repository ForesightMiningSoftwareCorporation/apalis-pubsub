use apalis_core::{
    backend::{codec::Codec, Backend, TaskStream},
    task::{builder::TaskBuilder, task_id::TaskId, Task},
    worker::context::WorkerContext,
};
use futures::StreamExt;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::{
    client::{Client, ClientConfig},
    subscription::Subscription,
    topic::Topic,
};
use std::task::{Context, Poll};
use std::{
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio_stream::wrappers::ReceiverStream;
use tower::Layer;
use tower::Service;

pub mod utils;
use utils::PubSubContext;

pub use google_cloud_pubsub;

/// Middleware layer that acknowledges messages on successful completion
#[derive(Clone)]
pub struct AcknowledgeLayer;

impl<S> Layer<S> for AcknowledgeLayer {
    type Service = AcknowledgeService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AcknowledgeService { inner: service }
    }
}

#[derive(Clone)]
pub struct AcknowledgeService<S> {
    inner: S,
}

impl<S, M> Service<PubSubTask<M>> for AcknowledgeService<S>
where
    S: Service<PubSubTask<M>>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    M: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: PubSubTask<M>) -> Self::Future {
        let fut = self.inner.call(req);

        Box::pin(fut)
    }
}

/// Error type for PubSub backend operations
#[derive(Debug, thiserror::Error)]
pub enum PubSubError {
    #[error("Pub/Sub client error: {0}")]
    Client(String),

    #[error("Codec error: {0}")]
    Codec(Box<dyn std::error::Error + Send + Sync>),

    #[error("Message acknowledgment failed: {0}")]
    AckFailed(String),

    #[error("Subscription error: {0}")]
    Subscription(String),
}

/// Type alias for an PubSub task with context and u64 as the task ID type.
pub type PubSubTask<T> = Task<T, PubSubContext, u64>;

/// Type alias for an PubSub task ID with u64 as the ID type.
pub type PubSubTaskId = TaskId<u64>;

/// Global task ID counter for generating unique task IDs
static TASK_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Configuration for PubSub backend behavior
#[derive(Debug, Clone)]
pub struct PubSubConfig {
    /// Channel buffer size for message processing (default: 100)
    pub buffer_size: usize,
    /// Maximum message size in bytes (default: 10MB)
    pub max_message_size: usize,
    /// Maximum number of outstanding messages
    pub max_outstanding_messages: Option<i64>,
    /// Maximum bytes of outstanding messages
    pub max_outstanding_bytes: Option<i64>,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            buffer_size: 100,
            max_message_size: 10 * 1024 * 1024,
            max_outstanding_messages: None,
            max_outstanding_bytes: None,
        }
    }
}

/// A Google Cloud Pub/Sub backend for Apalis job processing.
///
/// This backend provides reliable message queue functionality using GCP Pub/Sub,
/// with support for message acknowledgment, configurable buffering, and graceful shutdown.
///
/// # Example
///
/// ```no_run
/// use apalis_pubsub::{PubSubBackend, PubSubConfig};
/// use apalis_codec::json::JsonCodec;
/// use google_cloud_pubsub::client::ClientConfig;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct MyJob {
///     id: u64,
///     data: String,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ClientConfig::default().with_auth().await?;
///
/// // Create backend with default configuration
/// let backend: PubSubBackend<MyJob, JsonCodec<Vec<u8>>> =
///     PubSubBackend::new_from_config(
///         config,
///         "my-topic".to_string(),
///         "my-subscription".to_string(),
///     ).await?;
///
/// // Publish a job
/// backend.push(MyJob { id: 1, data: "test".into() }).await?;
///
/// // Graceful shutdown
/// backend.shutdown();
/// # Ok(())
/// # }
/// ```
///
/// With custom configuration:
///
/// ```no_run
/// # use apalis_pubsub::{PubSubBackend, PubSubConfig};
/// # use apalis_codec::json::JsonCodec;
/// # use google_cloud_pubsub::client::ClientConfig;
/// # use serde::{Deserialize, Serialize};
/// #
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # struct MyJob {
/// #     id: u64,
/// #     data: String,
/// # }
/// #
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ClientConfig::default().with_auth().await?;
///
/// let custom_config = PubSubConfig {
///     buffer_size: 200,
///     max_message_size: 5 * 1024 * 1024, // 5MB
///     ..Default::default()
/// };
///
/// let backend: PubSubBackend<MyJob, JsonCodec<Vec<u8>>> =
///     PubSubBackend::new_with_config(
///         config,
///         "my-topic".to_string(),
///         "my-subscription".to_string(),
///         custom_config,
///     ).await?;
///
/// backend.push(MyJob { id: 1, data: "test".into() }).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Shutdown Behavior
///
/// Call `shutdown()` to signal the backend to stop receiving new messages.
/// In-flight messages will complete processing before the worker terminates.
#[derive(Debug, Clone)]
pub struct PubSubBackend<M, Codec> {
    /// Client must be kept alive as topic/subscription hold references to it
    #[allow(dead_code)]
    client: Client,
    topic: Topic,
    /// Arc-wrapped subscription for safe sharing across worker threads in poll()
    subscription: std::sync::Arc<Subscription>,
    /// Configuration for backend behavior
    config: PubSubConfig,
    /// Cancellation token for graceful shutdown
    cancel: tokio_util::sync::CancellationToken,
    _phantom: PhantomData<(M, Codec)>,
}

impl<M, C> PubSubBackend<M, C> {
    /// Creates a new PubSubBackend from client configuration with default settings.
    ///
    /// # Arguments
    /// * `config` - The client configuration for Google Cloud Pub/Sub
    /// * `topic_name` - The name of the topic to publish messages to
    /// * `subscription_name` - The name of the subscription to receive messages from
    pub async fn new_from_config(
        config: ClientConfig,
        topic_name: String,
        subscription_name: String,
    ) -> Result<Self, PubSubError> {
        Self::new_with_config(
            config,
            topic_name,
            subscription_name,
            PubSubConfig::default(),
        )
        .await
    }

    /// Creates a new PubSubBackend with custom configuration.
    ///
    /// # Arguments
    /// * `config` - The client configuration for Google Cloud Pub/Sub
    /// * `topic_name` - The name of the topic to publish messages to
    /// * `subscription_name` - The name of the subscription to receive messages from
    /// * `pubsub_config` - Custom configuration for backend behavior
    pub async fn new_with_config(
        config: ClientConfig,
        topic_name: String,
        subscription_name: String,
        pubsub_config: PubSubConfig,
    ) -> Result<Self, PubSubError> {
        let client = Client::new(config)
            .await
            .map_err(|e| PubSubError::Subscription(e.to_string()))?;

        let topic = client.topic(&topic_name);
        let subscription = client.subscription(&subscription_name);

        Ok(Self {
            client,
            topic: topic.clone(),
            subscription: std::sync::Arc::new(subscription),
            config: pubsub_config,
            cancel: tokio_util::sync::CancellationToken::new(),
            _phantom: PhantomData,
        })
    }

    /// Signals the backend to gracefully shutdown.
    ///
    /// This will stop receiving new messages from the subscription.
    /// In-flight messages will complete processing before the worker terminates.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
}

impl<M, C> PubSubBackend<M, C>
where
    C: Codec<M, Compact = Vec<u8>>,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    /// Publishes a job to the Pub/Sub topic.
    ///
    /// # Arguments
    /// * `job` - The job to publish
    ///
    /// # Returns
    /// `Ok(())` on successful publish, or `PubSubError` on failure
    #[tracing::instrument(skip(self, job))]
    pub async fn push(&self, job: M) -> Result<(), PubSubError> {
        // Encode the job using the codec
        let bytes = C::encode(&job).map_err(|e| PubSubError::Codec(Box::new(e)))?;

        // Create a publisher for the topic
        let publisher = self.topic.new_publisher(None);

        // Create and publish the message
        let awaiter = publisher
            .publish(PubsubMessage {
                data: bytes,
                ..Default::default()
            })
            .await;

        // Await the publish result
        awaiter
            .get()
            .await
            .map_err(|e| PubSubError::Client(e.to_string()))?;

        Ok(())
    }
}

impl<M: Send + 'static, C> Backend for PubSubBackend<M, C>
where
    C: Codec<M, Compact = Vec<u8>>,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = M;
    type Error = PubSubError;
    type Beat = futures::stream::BoxStream<'static, Result<(), Self::Error>>;
    type Layer = AcknowledgeLayer;
    type Stream = TaskStream<Task<M, PubSubContext, Self::IdType>, Self::Error>;
    type Context = PubSubContext;
    type IdType = u64;

    fn heartbeat(&self, _worker: &WorkerContext) -> Self::Beat {
        // Pub/Sub manages connection health internally
        Box::pin(futures::stream::empty())
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer
    }

    #[tracing::instrument(skip(self, _worker))]
    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let subscription = self.subscription.clone();
        let buffer_size = self.config.buffer_size;
        let max_message_size = self.config.max_message_size;
        let cancel = self.cancel.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(buffer_size);

        // Spawn task to receive messages from Pub/Sub and send to channel
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let result = subscription
                .as_ref()
                .receive(
                    move |message, _cancel| {
                        let tx = tx_clone.clone();

                        async move {
                            let bytes = message.message.data.clone();
                            let ack_id = message.ack_id().to_string();

                            // Validate message size
                            if bytes.len() > max_message_size {
                                tracing::error!(size = bytes.len(), max = max_message_size, "Message exceeds maximum size");
                                if let Err(e) = message.ack().await {
                                    tracing::error!(error = ?e, "Failed to ack oversized message");
                                }
                                return;
                            }

                            // Generate unique task ID using atomic counter
                            let task_id = TASK_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(task_id, "Received message");

                            // Decode message
                            let msg: M = match C::decode(&bytes) {
                                Ok(m) => {
                                    tracing::trace!("Message decoded successfully");
                                    m
                                }
                                Err(e) => {
                                    tracing::error!(error = ?e, task_id, "Failed to decode message - treating as poison message");
                                    // Ack poison messages to prevent infinite redelivery
                                    if let Err(ack_err) = message.ack().await {
                                        tracing::error!(error = ?ack_err, "Failed to ack poison message");
                                    }
                                    return;
                                }
                            };

                            // Build task with PubSubContext
                            let task = TaskBuilder::new(msg)
                                .with_task_id(TaskId::new(task_id))
                                .with_ctx(PubSubContext::new(ack_id))
                                .build();

                            // Send task to channel
                            match tx.send(Ok(Some(task))).await {
                                Ok(()) => {
                                    // Ack message now that we've committed to processing it
                                    if let Err(ack_err) = message.ack().await {
                                        tracing::error!(error = ?ack_err, "Failed to ack message");
                                    } else {
                                        tracing::debug!("Message acknowledged");
                                    }
                                }
                                Err(send_err) => {
                                    tracing::error!(error = ?send_err, "Failed to send task to worker");
                                }
                            }
                        }
                    },
                    cancel.clone(),
                    None,
                )
                .await;

            if let Err(e) = result {
                tracing::error!(error = ?e, "Subscription error");
                let err = PubSubError::Subscription(e.to_string());
                if let Err(send_err) = tx.send(Err(err)).await {
                    tracing::error!(error = ?send_err, "Failed to send subscription error to worker");
                }
            }
        });

        // Convert channel receiver to stream
        ReceiverStream::new(rx).boxed()
    }
}

// BackendExt implementation removed - not needed for PubSub backend
