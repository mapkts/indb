//! Server implementation.
use crate::{Command, Connection, Db, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// Server listener state.
#[derive(Debug)]
struct Listener {
    /// Shared database handle.
    ///
    /// This is a wrapper around an `Arc`, which allow `db` to be cloned and
    /// passed into the per connection state.
    db: Db,

    /// TCP listener.
    listener: TcpListener,

    /// Limit the max number of connections.
    limit_connections: Arc<Semaphore>,

    /// Broadcast a shutdown signal to all active connections.
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
}

/// Per-connection handler. Reads requests from `connection` and applies the commands to the `db`.
#[derive(Debug)]
struct Handler {
    /// Shared database handle.
    db: Db,

    /// The TCP connection.
    connection: Connection,

    /// Max connection semaphore.
    ///
    /// When the handler is dropped, a permit is returned to this semaphore.
    limit_connections: Arc<Semaphore>,

    /// Listen for shutdown notifications.
    shutdown: Shutdown,

    /// Not used directly.
    _shutdown_complete: mpsc::Sender<()>,
}

/// Maximum number of concurrent connections the redis server will accept.
const MAX_CONNECTIONS: usize = 250;

/// Run the server.
///
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task in spawned to handle that connection. The server runs until the `shutdown`
/// future completes, at which point the server shuts down gracefully.
pub async fn run(listener: TcpListener, shutdown: impl Future) -> crate::Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // Initialize the listener.
    let mut server = Listener {
        listener,
        db: Db::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    // The server task runs until an error is encountered, or the `shutdown` signal is received.
    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down");
        }
    }

    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have subscribed will receive the shutdown
    // signal and can exit.
    drop(notify_shutdown);
    // Drop the final `Sender` so `Receiver` can complete.
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing.
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl Listener {
    /// Run the server.
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // Wait for a permit to become available.
            //
            // `unwrap()` here is safe because `acquire()` only returns `Err` when the semaphore
            // has been closed and we don't ever close the semaphore.
            self.limit_connections.acquire().await.unwrap().forget();

            // Accept a new socket.
            let socket = self.accept().await?;

            let mut handler = Handler {
                db: self.db.clone(),
                connection: Connection::new(socket),
                limit_connections: self.limit_connections.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // Spawn a new task to process the connections.
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }

    /// Accepts an inbound connection.
    ///
    /// Errors are handled by a exponential backoff strategy. First failed task
    /// waits for 1 second, and second failure waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // try to accept a few times.
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the backoff period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the backoff.
            backoff *= 2;
        }
    }
}

impl Handler {
    /// Process a single connection.
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // Read new request frames until the shutdown signal has been received.
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(())
                }
            };

            // If `None` is returned then the peer has closed the socket.
            // There is no further work to do and the task can be terminated.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Convert the frame into a command.
            let cmd = Command::from_frame(frame)?;

            debug!(?cmd);

            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Add a permit back to the semaphore.
        self.limit_connections.add_permits(1);
    }
}
