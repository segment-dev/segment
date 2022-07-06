use tokio::sync::broadcast;

/// Listens for a shutdown event by polling broadcast::Receiver
/// we use this to shutdown the read loop and close the connection.
#[derive(Debug)]
pub struct ShutdownListener {
    shutdown: bool,
    shutdown_notification_receiver: broadcast::Receiver<()>,
}

impl ShutdownListener {
    pub fn new(shutdown_notification_receiver: broadcast::Receiver<()>) -> Self {
        ShutdownListener {
            shutdown: false,
            shutdown_notification_receiver,
        }
    }

    pub fn shutdown(&self) -> bool {
        self.shutdown
    }

    pub async fn listen(&mut self) {
        if self.shutdown {
            return;
        }

        // Listen for shutdown event
        let _ = self.shutdown_notification_receiver.recv().await;
        self.shutdown = true
    }
}
