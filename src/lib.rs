//! A minimal and incomplete implementation of a Redis server and client.

pub mod server;

mod db;

#[doc(inline)]
pub use db::Db;
