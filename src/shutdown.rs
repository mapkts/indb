use tokio::sync::broadcast;

/// Listens for a shutdown signal from the server.
#[derive(Debug)]
pub struct Shutdown {
    /// Whether the shutdown signal has been received.
    shutdown: bool,
    /// The receive half of the channel used to listen for shutdown.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    pub async fn recv(&mut self) {
        // Returns immediately if the shutdown signal has already been received.
        if self.shutdown {
            return;
        }

        // Ignore "lag error" as only one value is ever sent.
        let _ = self.notify.recv().await;

        self.shutdown = true;
    }
}
