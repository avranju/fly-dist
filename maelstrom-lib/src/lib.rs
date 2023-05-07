use std::{any::Any, collections::HashMap, future::Future, pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures::future::join_all;
use log::{error, trace};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::{mpsc, oneshot, Mutex},
};

mod error;
mod seq;

pub use error::Error;
pub use seq::SeqKv;

#[derive(Serialize, Deserialize)]
pub struct Reply<B> {
    pub in_reply_to: Option<u64>,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub body: Option<B>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Body {
    #[serde(rename = "type")]
    pub type_: String,
    pub msg_id: Option<u64>,

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
    pub fn node(&self) -> &Node {
        &self.node
    }

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
    ) -> Result<u64, Error> {
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MaelstromError {
    pub code: i32,
    pub text: Option<String>,
}

async fn handle_error(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some((Some(in_reply_to), err)) = msg
        .body
        .parse::<Reply<MaelstromError>>()?
        .and_then(|b| b.body.map(|err| (b.in_reply_to, err)))
    {
        error!(
            "Received error message. Code: {}, Text: {:?}, Reply To: {}",
            err.code, err.text, in_reply_to
        );

        if let Some(tx) = ctx
            .node
            .state
            .lock()
            .await
            .error_notify
            .remove(&in_reply_to)
        {
            if !tx.is_closed() {
                tx.send(err).map_err(|_| {
                    error!("handle_error: sending error to channel failed.");
                    Error::ChannelSend("Sending error to notify channel failed.".to_string())
                })?;
            }
        }
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

    error_notify: HashMap<u64, oneshot::Sender<MaelstromError>>,

    services: HashMap<&'static str, Box<dyn Any + Send>>,
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
                error_notify: HashMap::new(),
                services: HashMap::new(),
            })),
        };

        node.add_service("seqkv", Box::new(SeqKv::new(node.clone())))
            .await;

        node.handle("init", handle_init).await;
        node.handle("error", handle_error).await;

        node
    }

    pub async fn add_service(&self, name: &'static str, svc: Box<dyn Any + Send>) {
        let mut state = self.state.lock().await;
        if let Some(e) = state.services.get_mut(name) {
            *e = svc;
        } else {
            state.services.insert(name, svc);
        }
    }

    pub fn for_service<T, F, R>(&self, name: &'static str, action: F) -> Result<R, Error>
    where
        T: 'static,
        F: Fn(&T) -> Result<R, Error>,
    {
        self.state
            .blocking_lock()
            .services
            .get(name)
            .and_then(|svc| svc.downcast_ref::<T>())
            .map(action)
            .unwrap_or(Err(Error::ServiceNotFound(name)))
    }

    pub fn for_service_mut<T, F, R>(&self, name: &'static str, action: F) -> Result<R, Error>
    where
        T: 'static,
        F: Fn(&mut T) -> Result<R, Error>,
    {
        self.state
            .blocking_lock()
            .services
            .get_mut(name)
            .and_then(|svc| svc.downcast_mut::<T>())
            .map(action)
            .unwrap_or(Err(Error::ServiceNotFound(name)))
    }

    pub async fn notify_error(&self, msg_id: u64, tx: oneshot::Sender<MaelstromError>) {
        let mut state = self.state.lock().await;
        if let Some(e) = state.error_notify.get_mut(&msg_id) {
            *e = tx;
        } else {
            state.error_notify.insert(msg_id, tx);
        }
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
    ) -> Result<u64, Error> {
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
    ) -> Result<u64, Error> {
        let (src, msg_id) = tokio::join!(self.id(), self.next_msg_id());

        let msg = Message {
            src,
            dest: node_id,
            body: Body {
                msg_id: Some(msg_id),
                type_,
                body,
            },
        };

        println!("{}", serde_json::to_string(&msg)?);

        Ok(msg_id)
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
                msg_id: Some(msg_id),
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
