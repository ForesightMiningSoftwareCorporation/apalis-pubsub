use crate::{PubSubBackend, PubSubTask};
use apalis_core::backend::codec::Codec;
use futures::{
    FutureExt, Sink,
    future::{BoxFuture, Shared},
};
use pin_project::pin_project;
use std::error::Error;
use std::fmt::Debug;
use std::{collections::VecDeque, sync::Arc};

#[pin_project]
#[derive(Debug)]
pub(super) struct PubSubSink<T, C> {
    items: VecDeque<PubSubTask<Vec<u8>>>,
    pending_sends: VecDeque<PendingSend>,
    _codec: std::marker::PhantomData<(T, C)>,
}

impl<T, C> Clone for PubSubSink<T, C> {
    fn clone(&self) -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: std::marker::PhantomData,
        }
    }
}

impl<T, C> PubSubSink<T, C> {
    pub(crate) fn new() -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: std::marker::PhantomData,
        }
    }
}

struct PendingSend {
    future: Shared<BoxFuture<'static, Result<Arc<()>, Arc<dyn Error>>>>,
}

impl Debug for PendingSend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingSend").finish()
    }
}
