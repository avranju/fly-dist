use std::{
    cell::RefCell, collections::HashMap, future::Future, io::Error as IoError, pin::Pin, rc::Rc,
};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{self, AsyncBufReadExt, BufReader};

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] IoError),

    #[error("JSON serialisation error")]
    Json(#[from] serde_json::Error),

    #[error("Unknown error")]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Body<T> {
    #[serde(rename = "type")]
    pub type_: String,
    pub msg_id: u64,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub body: Option<T>,
}

#[derive(Serialize, Deserialize)]
pub struct Reply<T> {
    pub in_reply_to: u64,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub body: Option<T>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Message<T>
where
    T: Clone,
{
    pub src: String,
    pub dest: String,
    pub body: Body<T>,
}

#[derive(Clone)]
pub struct Context<B> {
    node: Node<B>,
}

impl<B> Context<B>
where
    B: Serialize + DeserializeOwned + Send + Clone,
{
    pub async fn reply_to<R: Serialize>(&self, src: Message<B>, body: R) -> Result<(), Error> {
        self.node.reply_to(src, body).await
    }
}

#[async_trait]
pub trait MessageHandler<Body: Serialize + DeserializeOwned + Send + Clone> {
    async fn handle(&self, ctx: Context<Body>, msg: Message<Body>) -> Result<(), Error>;
}

/// Blanket impl of `MessageHandler` for all functions where signature
/// matches `MessageHandler::handle`.
impl<F, B, U> MessageHandler<B> for F
where
    U: Future<Output = Result<(), Error>> + Send + 'static,
    F: Fn(Context<B>, Message<B>) -> U,
    B: Serialize + DeserializeOwned + Send + Clone,
{
    fn handle<'life0, 'async_trait>(
        &'life0 self,
        ctx: Context<B>,
        msg: Message<B>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(self(ctx, msg))
    }
}

pub struct Node<B> {
    handlers: Rc<RefCell<HashMap<String, Box<dyn MessageHandler<B>>>>>,
}

impl<B> Clone for Node<B> {
    fn clone(&self) -> Self {
        Self {
            handlers: self.handlers.clone(),
        }
    }
}

impl<B> Node<B>
where
    B: Serialize + DeserializeOwned + Send + Clone,
{
    pub fn new() -> Self {
        Node {
            handlers: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn handle<T: MessageHandler<B> + 'static>(&mut self, type_: &str, handler: T) {
        self.handlers
            .borrow_mut()
            .insert(type_.to_string(), Box::new(handler));
    }

    pub async fn reply_to<R: Serialize>(&self, src: Message<B>, body: R) -> Result<(), Error> {
        Ok(())
    }

    pub async fn run(self) -> Result<(), Error> {
        // each message is sent to us as a single line
        let stdin = io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();
        let ctx = Context { node: self.clone() };

        while let Some(line) = lines.next_line().await? {
            let msg: Message<B> = serde_json::from_str(&line)?;
            let handlers = self.handlers.borrow();

            let f = handlers
                .iter()
                .map(|(_, h)| h.handle(ctx.clone(), msg.clone()));
        }

        Ok(())
    }
}

impl<B> Default for Node<B>
where
    B: Serialize + DeserializeOwned + Send + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}
