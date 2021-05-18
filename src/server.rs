//! Server implementation.

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};

/// Server listener state.
#[derive(Debug)]
struct Listener {

}
