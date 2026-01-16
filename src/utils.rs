/// The context of a message
#[derive(Clone, Debug, Default)]
pub struct PubSubContext {}

impl PubSubContext {
    /// Creates a new `Context` instance with the given parameters.
    pub fn new() -> Self {
        Self {}
    }
}
