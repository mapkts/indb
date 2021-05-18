use crate::{Connection, Db, Frame, Parse, ParseError};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

/// Set key to hold the string value.
///
/// If key already holds a value, it is overwritten, regardless of its type. Any previous time to
/// live associated with the key is discarded on successful SET operation.
#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
    options: Opts,
}

#[derive(Debug)]
struct Opts {
    expire: Option<Duration>,
    nx: bool,
    xx: bool,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            options: Opts {
                expire,
                nx: false,
                xx: false,
            },
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn expire(&self) -> Option<Duration> {
        self.options.expire
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        // Read the key to set. This is required.
        let key = parse.next_string()?;

        // Read the value to set. This is required.
        let value = parse.next_bytes()?;

        // optional fields.
        let mut expire = None;
        let mut nx = false;
        let mut xx = false;

        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // Expire time is given in seconds. The next value is an integer.
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // Expire time is given in milliseconds. The next value is an integer.
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            Ok(s) if s.to_uppercase() == "NX" => {
                nx = true;
            }
            Ok(s) if s.to_uppercase() == "XX" => {
                xx = true;
            }
            Ok(s) => return Err(format!("SET command error: unsupported option {}", s).into()),
            Err(ParseError::EndOfStream) => {}
            // All other errors result in the connection being terminated.
            Err(err) => return Err(err.into()),
        }

        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // Expire time is given in seconds. The next value is an integer.
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // Expire time is given in milliseconds. The next value is an integer.
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            Ok(s) if s.to_uppercase() == "NX" => {
                nx = true;
            }
            Ok(s) if s.to_uppercase() == "XX" => {
                xx = true;
            }
            Ok(s) => return Err(format!("SET command error: unsupported option {}", s).into()),
            Err(ParseError::EndOfStream) => {}
            // All other errors result in the connection being terminated.
            Err(err) => return Err(err.into()),
        }

        // `NX` and `XX` can not be set at the same time.
        if nx && xx {
            return Err("SET command error: `NX` and `XX` cannot be given at the same time".into());
        }

        Ok(Set {
            key,
            value,
            options: Opts { expire, nx, xx },
        })
    }

    /// Apply the `Set` command to the specific `Db` instance and write the response to `dst`.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        if self.options.nx && db.get(&self.key).is_some()
            || self.options.xx && db.get(&self.key).is_none()
        {
            // Return `Null` if `nx` or `xx` condition was not met.
            let response = Frame::Null;
            debug!(?response);
            dst.write_frame(&response).await?;
        } else {
            db.set(self.key, self.value, self.options.expire);

            let response = Frame::Simple("OK".to_string());
            debug!(?response);
            dst.write_frame(&response).await?;
        }

        Ok(())
    }
}
