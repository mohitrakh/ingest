use crate::types::IngestEvent;
use tokio::sync::mpsc;

pub type EventSender = mpsc::Sender<IngestEvent>;
pub type EventReceiver = mpsc::Receiver<IngestEvent>;

pub fn create_buffer(size: usize) -> (EventSender, EventReceiver) {
    mpsc::channel(size)
}
