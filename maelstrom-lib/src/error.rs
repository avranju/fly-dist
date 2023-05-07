use std::io::Error as IoError;

use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{MaelstromError, Message};

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] IoError),

    #[error("JSON serialisation error")]
    Json(#[from] serde_json::Error),

    #[error("Reply channel send error")]
    ReplySendError(#[from] mpsc::error::SendError<(Message, String, Option<JsonValue>)>),

    #[error("Parse error {0}")]
    Parse(String),

    #[error("Maelstrom error. [{code}] {text:?}")]
    Maelstrom { code: i32, text: Option<String> },

    #[error("Sending to channel failed with {0}")]
    ChannelSend(String),

    #[error("Receiving from channel failed with {0}")]
    ChannelRecv(String),

    #[error("Service {0} was not found on the node")]
    ServiceNotFound(&'static str),

    #[error("Unknown error")]
    Unknown,
}

impl From<MaelstromError> for Error {
    fn from(value: MaelstromError) -> Self {
        Error::Maelstrom {
            code: value.code,
            text: value.text,
        }
    }
}
