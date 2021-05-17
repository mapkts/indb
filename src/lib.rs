//! A dead simple and very incomplete implementation of a Redis server and client.

mod db;
mod frame;

pub mod server;

#[doc(inline)]
pub use db::Db;

#[doc(inline)]
pub use frame::Frame;

/// Default port that a redis server listens on.
pub const DEFAULT_PORT: &str = "6379";

/// Error returned by most functions.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for Redis operation.
pub type Result<T> = std::result::Result<T, Error>;
