use std::{collections::HashMap, future::Future, io::Error as IoError, pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures::future::join_all;
use log::trace;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::{mpsc, Mutex},
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] IoError),

    #[error("JSON serialisation error")]
    Json(#[from] serde_json::Error),

    #[error("Reply channel send error")]
    ReplySendError(#[from] mpsc::error::SendError<(Message, String, Option<JsonValue>)>),

    #[error("Parse error")]
    Parse(String),

    #[error("Unknown error")]
    Unknown,
}

#[derive(Serialize, Deserialize)]
pub struct Reply<B> {
    pub in_reply_to: u64,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub body: Option<B>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Body {
    #[serde(rename = "type")]
    pub type_: String,
    pub msg_id: u64,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub body: Option<JsonValue>,
}

impl Body {
    pub fn parse<B: DeserializeOwned>(&self) -> Result<Option<B>, Error> {
        Ok(self
            .body
            .as_ref()
            .map(|json| serde_json::from_value(json.clone()))
            .transpose()?)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Clone)]
pub struct Context {
    node_id: String,
    node: Node,
    reply_queue_tx: mpsc::Sender<(Message, String, Option<JsonValue>)>,
}

impl Context {
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn reply_to(&self, src: Message, type_: String) -> Result<(), Error> {
        Ok(self.reply_queue_tx.send((src, type_, None)).await?)
    }

    pub async fn reply_to_with<R: Serialize>(
        &self,
        src: Message,
        type_: String,
        body: Option<R>,
    ) -> Result<(), Error> {
        Ok(self
            .reply_queue_tx
            .send((
                src,
                type_,
                body.map(|b| serde_json::to_value(b)).transpose()?,
            ))
            .await?)
    }

    pub async fn send<R: Serialize>(
        &self,
        node_id: String,
        type_: String,
        body: Option<R>,
    ) -> Result<(), Error> {
        self.node.send_value(node_id, type_, body).await
    }
}

#[async_trait]
pub trait MessageHandler {
    async fn handle(&self, ctx: Context, msg: Message) -> Result<(), Error>;
}

/// Blanket impl of `MessageHandler` for all functions where signature
/// matches `MessageHandler::handle`.
impl<F, U> MessageHandler for F
where
    U: Future<Output = Result<(), Error>> + Send + 'static,
    F: Fn(Context, Message) -> U,
{
    fn handle<'life0, 'async_trait>(
        &'life0 self,
        ctx: Context,
        msg: Message,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(self(ctx, msg))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Init {
    node_id: String,
    node_ids: Vec<String>,
}

async fn handle_init(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(init) = msg.body.parse::<Init>()? {
        trace!("Received init message for node {:?}.", init);

        let mut state = ctx.node.state.lock().await;
        state.id = init.node_id;
        state.node_ids = init.node_ids;

        ctx.reply_to(msg, "init_ok".to_string()).await?;
    }

    Ok(())
}

#[derive(Clone)]
pub struct Node {
    #[allow(clippy::type_complexity)]
    handlers: Arc<Mutex<HashMap<String, Vec<Box<dyn MessageHandler + Send>>>>>,
    state: Arc<Mutex<State>>,
}

impl Node {
    pub async fn id(&self) -> String {
        self.state.lock().await.id.clone()
    }

    pub async fn node_ids(&self) -> Vec<String> {
        self.state.lock().await.node_ids.clone()
    }
}

struct State {
    id: String,
    node_ids: Vec<String>,
    msg_id_counter: usize,
    reply_queue_tx: mpsc::Sender<(Message, String, Option<JsonValue>)>,
    reply_queue_rx: Option<mpsc::Receiver<(Message, String, Option<JsonValue>)>>,
}

impl Node {
    pub async fn new() -> Self {
        let (reply_queue_tx, reply_queue_rx) = mpsc::channel(256);

        let mut node = Node {
            handlers: Arc::new(Mutex::new(HashMap::new())),
            state: Arc::new(Mutex::new(State {
                id: String::new(),
                node_ids: vec![],
                msg_id_counter: 1,
                reply_queue_tx,
                reply_queue_rx: Some(reply_queue_rx),
            })),
        };

        node.handle("init", handle_init).await;

        node
    }

    pub async fn handle<T: MessageHandler + Send + 'static>(&mut self, type_: &str, handler: T) {
        let handler = Box::new(handler);
        let handlers = &mut self.handlers.lock().await;

        if let Some(handlers) = handlers.get_mut(type_) {
            handlers.push(handler);
        } else {
            handlers.insert(type_.to_string(), vec![handler]);
        }
    }

    async fn next_msg_id(&self) -> u64 {
        let mut state = self.state.lock().await;

        let msg_id = state.msg_id_counter as u64;
        state.msg_id_counter += 1;

        msg_id
    }

    pub async fn send_value<R: Serialize>(
        &self,
        node_id: String,
        type_: String,
        body: Option<R>,
    ) -> Result<(), Error> {
        self.send(
            node_id,
            type_,
            body.map(|b| serde_json::to_value(&b)).transpose()?,
        )
        .await
    }

    pub async fn send(
        &self,
        node_id: String,
        type_: String,
        body: Option<JsonValue>,
    ) -> Result<(), Error> {
        let (src, msg_id) = tokio::join!(self.id(), self.next_msg_id());

        let msg = Message {
            src,
            dest: node_id,
            body: Body {
                msg_id,
                type_,
                body,
            },
        };

        println!("{}", serde_json::to_string(&msg)?);

        Ok(())
    }

    pub async fn reply_to(
        &self,
        src: Message,
        type_: String,
        body: Option<JsonValue>,
    ) -> Result<(), Error> {
        let reply = Reply {
            in_reply_to: src.body.msg_id,
            body,
        };

        let (node_id, msg_id) = tokio::join!(self.id(), self.next_msg_id());

        let msg = Message {
            src: node_id,
            dest: src.src,
            body: Body {
                msg_id,
                type_,
                body: Some(serde_json::to_value(reply)?),
            },
        };

        println!("{}", serde_json::to_string(&msg)?);

        Ok(())
    }

    pub async fn run(self) -> Result<(), Error> {
        // each message is sent to us as a single line
        let stdin = io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();
        let (reply_queue_tx, mut reply_queue_rx) = {
            let mut state = self.state.lock().await;
            (
                state.reply_queue_tx.clone(),
                state
                    .reply_queue_rx
                    .take()
                    .expect("Reply queue receiver must have been set"),
            )
        };

        let mut ctx = Context {
            node_id: String::new(),
            node: self.clone(),
            reply_queue_tx,
        };

        loop {
            tokio::select! {
                Some((msg, type_, body)) = reply_queue_rx.recv() => {
                    // if the "type_" is "init_ok" then we have a valid node ID
                    // now; store it in the ctx
                    if type_ == "init_ok" {
                        ctx.node_id = self.state.lock().await.id.clone();
                    }

                    self.reply_to(msg, type_, body).await?;
                },

                Ok(Some(line)) = lines.next_line() => {
                    let msg: Message = serde_json::from_str(&line)?;
                    if let Some(handlers) = self.handlers.lock().await.get(&msg.body.type_) {
                        let _ = join_all(handlers.iter().map(|h| h.handle(ctx.clone(), msg.clone()))).await;
                    }
                },
            }
        }
    }
}
