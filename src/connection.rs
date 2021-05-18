use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// Send and receive `Frame`s from a remote peer.
#[derive(Debug)]
pub struct Connection {
    /// The `TcpStream`. It uses `BufWriter` for write level buffering.
    stream: BufWriter<TcpStream>,
    /// The internal buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    /// Create a new `Connection`.
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Tries to parse a frame from the buffer.
    ///
    /// # Returns
    /// 
    /// If the buffer contains enough data, the frame is returned and the data removed from the
    /// buffer. If not enough data has been buffered yet, `Ok(None)` is returned. If the buffered
    /// data does not represent a valid frame, `Err` is returned.
    pub fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        let mut buf = Cursor::new(&self.buffer[..]);

        // check if enough data has been buffered to parse a single frame.
        match Frame::check(&mut buf) {
            Ok(_) => {
                // remember the length of the frame.
                let len = buf.position() as usize;

                // reset the position to zero.
                buf.set_position(0);

                // parse the frame from the buffer.
                let frame = Frame::parse(&mut buf)?;

                // remove the parsed data from the buffer.
                self.buffer.advance(len);

                Ok(Some(frame))
            },
            // There is not enough data present in the read buffer to parse a single frame.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame.
            Err(e) => Err(e.into()),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream` is closed in a way that
    /// doesn't break a frame in half, `None` is returned. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // Attempt to read a frame from the buffered data.
            // If enough data has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // `0` indicates "end of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    // `TcpStream` is closed in a way that doesn't break a frame in half.
                    return Ok(None);
                } else {
                    // `TcpStream` is closed unexpectedly.
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            // Arrays are encoded by encoding each entry.
            // Encoding recursive frame structures is not supported yet.
            Frame::Array(val) => {
                // encode the array frame prefix.
                self.stream.write_u8(b'*').await?;

                // encode the length of the aray.
                self.write_decimal(val.len() as u64).await?;

                // iterate and encode each entry in the array frame.
                for entry in val {
                   self.write_value(entry).await?; 
                }
            }
            _ => self.write_value(frame).await?,
        }

        // ensure the encoded frame is written to the socket.
        self.stream.flush().await
    }

    /// Write a decimal frame into the stream.
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        // write the value as a string.
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }

    /// Write a frame literal into the stream.
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            // async fn do not support recursion in general.
            Frame::Array(_val) => {
                unreachable!()
            }
        }

        Ok(())
    }
}
