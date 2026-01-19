use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    future::{try_join_all, BoxFuture},
    FutureExt, Sink,
};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;

use crate::{PubSubBackend, PubSubCompact, PubSubError, PubSubTask};

/// The type of the future that the sink polls when attempting to flush data
type SinkFlushFuture = BoxFuture<'static, Result<(), PubSubError>>;

/// Message sink for [`PubSubBackend`]
///
/// Consumes messages and sends them to the pub/sub backend
pub struct PubSubSink<M, Codec> {
    buffer: Vec<PubSubTask<PubSubCompact>>,
    flush_future: Option<SinkFlushFuture>,
    _marker: PhantomData<(M, Codec)>,
}

impl<M, Codec> Clone for PubSubSink<M, Codec> {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
            flush_future: None,
            _marker: PhantomData,
        }
    }
}

impl<M, Codec> PubSubSink<M, Codec> {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            flush_future: None,
            _marker: PhantomData,
        }
    }
}

impl<M, Codec> Sink<PubSubTask<PubSubCompact>> for PubSubBackend<M, Codec>
where
    M: Unpin,
    Codec: Unpin,
{
    type Error = PubSubError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: PubSubTask<PubSubCompact>,
    ) -> Result<(), Self::Error> {
        self.get_mut().sink.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = self.get_mut();

        if me.sink.flush_future.is_none() && me.sink.buffer.is_empty() {
            // No running future, and nothing to flush from the buffer: Nothing to do
            return Poll::Ready(Ok(()));
        }

        if me.sink.flush_future.is_none() && !me.sink.buffer.is_empty() {
            // No running flush future, and there's tasks in the buffer to send
            // Make the future to flush out the buffer and send them to pub/sub
            let buffer = std::mem::take(&mut me.sink.buffer);
            let publisher = me.topic.new_publisher(None);

            let fut = async move {
                let futures = buffer.into_iter().map(|task| {
                    // Send each task off to the backend
                    let publisher = publisher.clone();
                    async move {
                        // Note: this publish function is also buffered, so this whole chain is actually double-buffered
                        let awaiter = publisher
                            .publish(PubsubMessage {
                                data: task.args,
                                ..Default::default()
                            })
                            .await;

                        // Await the publish result
                        awaiter
                            .get()
                            .await
                            .inspect(|id| tracing::debug!("Message published:\n\tPub/sub id: {id}"))
                            .map_err(|e| PubSubError::Client(e.to_string()))
                    }
                });

                // Await the sends concurrently
                // This is, like, the whole point of buffered sending
                try_join_all(futures).await?;

                Ok::<_, PubSubError>(())
            };

            me.sink.flush_future = Some(fut.boxed());
        }

        if let Some(fut) = me.sink.flush_future.as_mut() {
            // Currently flushing tasks to the backend
            // Poll the future
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    // All done!
                    me.sink.flush_future = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    // Something wen't wrong :(
                    tracing::error!("Failed to send tasks to pub/sub backend: {e}");
                    me.sink.flush_future = None;
                    Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    // Future is still working, so we're still working
                    Poll::Pending
                }
            }
        } else {
            unreachable!()
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
