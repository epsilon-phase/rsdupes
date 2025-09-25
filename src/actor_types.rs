use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub type ActorSender<T> = UnboundedSender<T>;
pub type ActorReceiver<T> = UnboundedReceiver<T>;