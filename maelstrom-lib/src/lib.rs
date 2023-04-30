use std::{collections::HashMap, future::Future, pin::Pin};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Unknown error")]
    Unknown,
}

#[derive(Serialize, Deserialize)]
pub struct Body<T> {
    #[serde(rename = "type")]
    pub type_: String,
    pub msg_id: u64,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub body: Option<T>,
}

#[derive(Serialize, Deserialize)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: Body<T>,
}

#[async_trait]
pub trait MessageHandler<Body: Serialize + DeserializeOwned + Send> {
    async fn handle(&self, msg: Message<Body>) -> Result<(), Error>;
}

/// Blanket impl of `MessageHandler` for all functions where signature
/// matches `MessageHandler::handle`.
impl<F, B, U> MessageHandler<B> for F
where
    U: Future<Output = Result<(), Error>> + Send + 'static,
    F: Fn(Message<B>) -> U,
    B: Serialize + DeserializeOwned + Send,
{
    fn handle<'life0, 'async_trait>(
        &'life0 self,
        msg: Message<B>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(self(msg))
    }
}

pub struct Node<B> {
    handlers: HashMap<String, Box<dyn MessageHandler<B>>>,
}

impl<B> Node<B>
where
    B: Serialize + DeserializeOwned + Send,
{
    pub fn new() -> Self {
        Node {
            handlers: HashMap::new(),
        }
    }

    pub fn handle<T: MessageHandler<B> + 'static>(&mut self, type_: &str, handler: T) {
        self.handlers.insert(type_.to_string(), Box::new(handler));
    }
}

impl<B> Default for Node<B>
where
    B: Serialize + DeserializeOwned + Send,
{
    fn default() -> Self {
        Self::new()
    }
}
