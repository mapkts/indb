//! A dead simple and very incomplete implementation of a Redis server and client.

mod db;
pub use db::Db;

mod frame;
pub use frame::Frame;

mod connection;
pub use connection::Connection;

mod shutdown;
pub use shutdown::Shutdown;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

pub mod cmd;
pub use cmd::Command;

/// Default port that a redis server listens on.
pub const DEFAULT_PORT: &str = "6379";

/// Error returned by most functions.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for Redis operation.
pub type Result<T> = std::result::Result<T, Error>;
