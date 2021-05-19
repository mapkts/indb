//! Provides a type representing a Redis protocol frame.
//!
//! The Redis protocol can be found at <https://redis.io/topics/protocol>

use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// A frame in the Redis protocol.
#[derive(Debug, Clone)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

const ERROR_INVALID_FRAME: &str = "protocol error: invalid frame format";

impl Frame {
    /// Returns an empty array frame.
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Pushes a "bulk" frame into the array.
    ///
    /// # Panics
    ///
    /// Panics if `self` is not an array.
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => vec.push(Frame::Bulk(bytes)),
            _ => panic!("not an array frame"),
        }
    }

    /// Pushes an "integer" frame into the array.
    ///
    /// # Panics
    ///
    /// Panics if `self` is not an array.
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Checks if an entire message can be decoded from `src`.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match eat_u8(src)? {
            // check simple frame
            //
            // "+OK\r\n"
            b'+' => {
                eat_line(src)?;
                Ok(())
            }
            // check error frame
            //
            // "-Error message\r\n"
            b'-' => {
                eat_line(src)?;
                Ok(())
            }
            // check integer frame
            //
            // ":1000\r\n"
            b':' => {
                eat_decimal(src)?;
                Ok(())
            }
            // check bulk frame
            //
            // "$-1\r\n" (Null)
            // "$6\r\nfoobar\r\n"
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // skip '-1\r\n'
                    skip(src, 4)
                } else {
                    // Read the bulk string
                    let len: usize = eat_decimal(src)?.try_into()?;

                    // skip the number of bytes + 2 (\r\n)
                    skip(src, len + 2)
                }
            }
            // check array frame
            //
            // *5\r\n
            // :1\r\n
            // :2\r\n
            // :3\r\n
            // :4\r\n
            // $6\r\n
            // foobar\r\n
            b'*' => {
                let len = eat_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            other => Err(format!("protocol error: invalid frame type `{}`", other).into()),
        }
    }

    /// Parses the message into a `Frame`.
    ///
    /// The message should be valivated with `check()` before calling this function.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match eat_u8(src)? {
            // parse simple frame
            //
            // "+OK\r\n"
            b'+' => {
                let line = eat_line(src)?.to_vec();
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            // parse error frame
            //
            // "-Error message\r\n"
            b'-' => {
                let line = eat_line(src)?.to_vec();
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            // parse integer frame
            //
            // ":1000\r\n"
            b':' => {
                let int = eat_decimal(src)?;
                Ok(Frame::Integer(int))
            }
            // parse bulk frame
            //
            // "$-1\r\n" (Null)
            // "$6\r\nfoobar\r\n"
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = eat_line(src)?;

                    if line != b"-1" {
                        return Err(ERROR_INVALID_FRAME.into());
                    }

                    Ok(Frame::Null)
                } else {
                    // Read the bulk string
                    let len: usize = eat_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // skip the number of bytes + 2 (\r\n)
                    skip(src, len + 2)?;

                    Ok(Frame::Bulk(data))
                }
            }
            // parse array frame
            //
            // *5\r\n
            // :1\r\n
            // :2\r\n
            // :3\r\n
            // :4\r\n
            // $6\r\n
            // foobar\r\n
            b'*' => {
                let len: usize = eat_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.chunk()[0])
}

fn eat_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn eat_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // found a line, update the position to be **after** the \n
            src.set_position((i + 2) as u64);

            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

fn eat_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = eat_line(src)?;

    atoi::<u64>(line).ok_or_else(|| ERROR_INVALID_FRAME.into())
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        ERROR_INVALID_FRAME.into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        ERROR_INVALID_FRAME.into()
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " ")?;
                        part.fmt(fmt)?;
                    }
                }

                Ok(())
            }
        }
    }
}
