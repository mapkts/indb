use crate::cmd::{Parse, ParseError, Unknown};
use crate::{Command, Connection, Db, Frame, Shutdown};

use async_stream::stream;
use bytes::Bytes;
use std::pin::Pin;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// Subscribes the client to the specified channels.
///
/// Once the client enters the subscribed state it is not supposed to issue any
/// other commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE,
/// PUNSUBSCRIBE, PING, RESET and QUIT commands.
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// Unsubscribes the client from the given channels, or from all of them if none is given.
///
/// When no channels are specified, the client is unsubscribed from all the previously
/// subscribed channels. In this case, a message for every unsubscribed channel will be
/// sent to the client.
#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// Stream of messages.
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// Creates a `Subscribe` instance from a received frame.
    pub(crate) fn new(channels: &[String]) -> Subscribe {
        Subscribe {
            channels: channels.to_vec(),
        }
    }

    /// Parses a `Subscribe` instance from a received frame.
    ///
    /// # Returns
    ///
    /// On success, the `Subscribe` value is returned. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// ```text
    /// SUBSCRIBE channel [channel ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        // Extract the first string.
        let mut channels = vec![parse.next_string()?];

        // The `SUBSCRIBE` string has already been consumed.
        // Consume the remaining strings if any.
        loop {
            match parse.next_string() {
                // A string is found, push it into the list of channels to subscribe to.
                Ok(s) => channels.push(s),
                // `EndOfStream` indicates there is no further data to parse.
                Err(ParseError::EndOfStream) => break,
                // Bubble up all other errors, resulting in the connection being terminated.
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // A client may subscribe to mutiple channels and may dynamically add and remove channels
        // from its subscription list. To handle this, we use a `StreamMap` to track active
        // subscription.
        let mut subscriptions = StreamMap::new();

        loop {
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            tokio::select! {
                // Received messages from one of the subscribed channels.
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                // Received a shutdown signal.
                _ = shutdown.recv() => {
                    return Ok(())
                }
                // Received a subscribe or a unsubscribe command from the client.
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        // happen if the remote client has disconnected.
                        None => return Ok(()),
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;

                }
            }
        }
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

impl Unsubscribe {
    pub(crate) fn new(channels: &[String]) -> Self {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        // There may be no channels listed, so start with an empty vec.
        let mut channels = vec![];

        // Each entry in the frame must be a string or the frame is malformed.
        loop {
            match parse.next_string() {
                // A string has been consumed, push it into the list the channels to be unsubscribe
                // from.
                Ok(s) => channels.push(s),
                // No further data to parse.
                Err(ParseError::EndOfStream) => break,
                // Bubble up all other errors, resulting in the connection being terminated.
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    let rx = Box::pin(stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // If we lagged in consuming messages, just resume.
                Err(broadcast::error::RecvError::Lagged(_)) => {},
                Err(_) => break,
            }
        }
    });

    // Track subscription in client's subscription set.
    subscriptions.insert(channel_name.clone(), rx);

    let response = make_subscribe_frame(channel_name.clone(), subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

async fn handle_command(
    frame: Frame,
    channels: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::Result<()> {
    // Only `SUBSCRIBE` and `UNSUBSCRIBE` commands are permitted.
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            channels.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // If no channels are specified, unsubscribing from all channels.
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_owned())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        other => {
            let cmd = Unknown::new(other.get_name());
            cmd.apply(dst).await?;
        }
    }

    Ok(())
}
