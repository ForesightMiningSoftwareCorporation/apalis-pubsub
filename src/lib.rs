use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, BoxStream, TaskStream, codec::Codec},
    task::{Task, builder::TaskBuilder, task_id::TaskId},
    worker::context::WorkerContext,
};
use google_cloud_pubsub::{
    client::{Client, ClientConfig},
    topic::Topic,
};
use pin_project::pin_project;
use std::marker::PhantomData;
use utils::PubSubContext;

mod sink;
mod utils;

/// Type alias for an PubSub task with context and u64 as the task ID type.
pub type PubSubTask<T> = Task<T, PubSubContext, u64>;

/// Type alias for an PubSub task ID with u64 as the ID type.
pub type PubSubTaskId = TaskId<u64>;

#[derive(Debug, Clone)]
/// A wrapper around a GCP Pub/Sub `topic` that implements message queuing functionality.
#[pin_project]
pub struct PubSubBackend<M, Codec> {
    client: Client,
    topic: Topic,
    message_type: PhantomData<M>,
    #[pin]
    sink: sink::PubSubSink<M, Codec>,
}

impl<M, C> PubSubBackend<M, C> {
    pub async fn new_from_config(config: ClientConfig, topic: String) -> Self {
        // Create pubsub client.
        let client = Client::new(config).await.unwrap();
        // Get the topic to subscribe to.
        let topic = client.topic(&topic);
        Self {
            client,
            topic,
            message_type: PhantomData,
            sink: sink::PubSubSink::new(),
        }
    }
}

impl<M: Send + 'static, C> Backend for PubSubBackend<M, C>
where
    C: Codec<M, Compact = Vec<u8>>,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = M;
    type Error = Error;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    // type Layer = AcknowledgeLayer<Self>;
    type Stream = TaskStream<Task<M, PubSubContext, Self::IdType>, Self::Error>;
    type Context = PubSubContext;
    type IdType = u64;
    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let _ = worker;
        unimplemented!()
    }

    fn middleware(&self) -> Self::Layer {
        unimplemented!()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        unimplemented!()
    }
}

impl<M, C: Send + 'static> BackendExt for PubSubBackend<M, C>
where
    Self: Backend<Args = M, IdType = u64, Context = AmqpContext, Error = lapin::Error>,
    C: Codec<M, Compact = Vec<u8>> + Send + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
    M: Send + 'static + Unpin,
{
    type Codec = C;
    type Compact = Vec<u8>;
    type CompactStream = TaskStream<AmqpTask<Self::Compact>, Error>;

    fn get_queue(&self) -> Queue {
        Queue::from_str(self.queue.name().as_str()).expect("Queue should be a string")
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_delivery(worker)
            .map_ok(move |item| {
                let bytes = item.data;
                let tag = item.delivery_tag;

                let task = TaskBuilder::new(bytes)
                    .with_task_id(TaskId::new(tag))
                    .with_ctx(AmqpContext::new(DeliveryTag::new(tag), item.properties))
                    .build();
                Some(task)
            })
            .boxed()
    }
}
