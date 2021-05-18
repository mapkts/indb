use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// Utility for parsing a command.
///
/// Commands are represented as array frames. Each entry in the frame is a "token".
#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>,
}

/// Error encountered while parsing a frame.
///
/// Currently only `EndOfStream` errors are handled at runtime. All other errors result in the
/// connection being terminated.
#[derive(Debug)]
pub(crate) enum ParseError {
    /// Failed to extract a value due to the frame being fully consumed.
    EndOfStream,

    /// All other errors.
    Other(crate::Error),
}

impl Parse {
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error: expected array, got {:?}", frame).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// Returns the next frame.
    pub(crate) fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// Returns the next frame as a string.
    ///
    /// Only `Simple` and `Bulk` frames can be represented as strings. While errors are stored as
    /// strings, they are considered separate types.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error: invalid string".into()),
            other => Err(format!(
                "protocol error: expected simple frame or bulk frame, got {:?}",
                other
            )
            .into()),
        }
    }

    /// Returns the next frame as raw bytes.
    ///
    /// Only `Simple` and `Bulk` frames can be represented as raw bytes. Although errors are stored
    /// as strings and could be represented as raw bytes, they are considered separate types.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            other => Err(format!(
                "protocol error: expected simple frame or bulk frame, got {:?}",
                other
            )
            .into()),
        }
    }

    /// Returns the next frame as an integer.
    ///
    /// This includes `Simple`, `Bulk` and `Integer` frames. `Simple` and `Bulk` frames are parsed.
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "protocol error: invalid number";

        match self.next()? {
            Frame::Integer(v) => Ok(v),
            Frame::Simple(s) => atoi::<u64>(s.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            other => Err(format!("protocol error: expected integer frame but got {:?}", other).into()),
        }
    }

    /// Ensure there are no more entries in the array.
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error: expected end of frame, but there was more".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        ParseError::Other(src.to_string().into())
    }
}

impl std::error::Error for ParseError {}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error: unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}
