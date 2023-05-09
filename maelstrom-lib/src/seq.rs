use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::Future;
use log::error;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::{
    sync::{oneshot, Mutex, MutexGuard},
    time,
};

use crate::{Context, Error, Message, Node, Reply, Service};

/// Amount of time we'll wait before giving up on a call to the seq-kv service.
const CALL_TIMEOUT_MS: u64 = 400;

struct State {
    node: Node,
    read_notify: HashMap<u64, oneshot::Sender<JsonValue>>,
    write_notify: HashMap<u64, oneshot::Sender<()>>,
    cas_notify: HashMap<u64, oneshot::Sender<()>>,
}

#[derive(Clone)]
pub struct SeqKv {
    state: Arc<Mutex<State>>,
}

impl Service for SeqKv {
    const NAME: &'static str = "seq-kv";
}

impl SeqKv {
    pub async fn new(mut node: Node) -> Self {
        node.handle("read_ok", handle_read_ok).await;
        node.handle("write_ok", handle_write_ok).await;
        node.handle("cas_ok", handle_cas_ok).await;

        SeqKv {
            state: Arc::new(Mutex::new(State {
                node,
                read_notify: HashMap::new(),
                write_notify: HashMap::new(),
                cas_notify: HashMap::new(),
            })),
        }
    }

    pub async fn read<T: DeserializeOwned>(&mut self, key: &str) -> Result<T, Error> {
        // send out the read key request
        let msg_id = self
            .state
            .lock()
            .await
            .node
            .send_value(
                SeqKv::name(),
                "read".to_string(),
                Some(Read {
                    key: key.to_string(),
                }),
            )
            .await?;

        let (error_tx, error_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();

        // register to be notified of error/result
        self.notify_read(msg_id, result_tx).await;
        self.state
            .lock()
            .await
            .node
            .notify_error(msg_id, error_tx)
            .await;

        let timeout = time::sleep(Duration::from_millis(CALL_TIMEOUT_MS));

        // race on the 3 futures and see what we get first
        tokio::select! {
            val = result_rx => {
                Ok(val
                    .map_err(|err| Error::ChannelRecv(format!("{:?}", err)))
                    .and_then(|val| serde_json::from_value(val).map_err(Error::Json))?)
            },

            Ok(err) = error_rx => {
                Err(err.into())
            },

            _ = timeout => {
                error!("'read' on seq-kv service timed out.");
                Err(Error::Timeout("'read' on seq-kv service timed out.".into()))
            }
        }
    }

    pub async fn write<T: Serialize>(&mut self, key: &str, value: T) -> Result<(), Error> {
        self.mut_op(
            "write",
            Write {
                key: key.to_string(),
                value,
            },
            |mut this, msg_id, tx| async move { this.notify_write(msg_id, tx).await },
        )
        .await
    }

    pub async fn cas<T: Serialize>(
        &mut self,
        key: &str,
        from: T,
        to: T,
        create_if_not_exists: bool,
    ) -> Result<(), Error> {
        self.mut_op(
            "cas",
            Cas {
                key: key.to_string(),
                from,
                to,
                create_if_not_exists,
            },
            |mut this, msg_id, tx| async move {
                this.notify_cas(msg_id, tx).await;
            },
        )
        .await
        .map_err(|err| match &err {
            Error::Maelstrom { code, .. } => match code {
                20 => Error::KvKeyNotFound(key.to_string()),
                22 => Error::KvCasMismatch,
                _ => err,
            },
            _ => err,
        })
    }

    async fn notify_read(&mut self, msg_id: u64, tx: oneshot::Sender<JsonValue>) {
        let mut state = self.state.lock().await;
        if let Some(e) = state.read_notify.get_mut(&msg_id) {
            *e = tx;
        } else {
            state.read_notify.insert(msg_id, tx);
        }
    }

    async fn notify_write(&mut self, msg_id: u64, tx: oneshot::Sender<()>) {
        let mut state = self.state.lock().await;
        if let Some(e) = state.write_notify.get_mut(&msg_id) {
            *e = tx;
        } else {
            state.write_notify.insert(msg_id, tx);
        }
    }

    async fn notify_cas(&mut self, msg_id: u64, tx: oneshot::Sender<()>) {
        let mut state = self.state.lock().await;
        if let Some(e) = state.cas_notify.get_mut(&msg_id) {
            *e = tx;
        } else {
            state.cas_notify.insert(msg_id, tx);
        }
    }

    async fn mut_op<T, F, U>(
        &mut self,
        op_name: &'static str,
        body: T,
        notifier: F,
    ) -> Result<(), Error>
    where
        T: Serialize,
        U: Future<Output = ()>,
        F: Fn(SeqKv, u64, oneshot::Sender<()>) -> U,
    {
        // send out the cas/write request
        let msg_id = {
            self.state
                .lock()
                .await
                .node
                .send_value(SeqKv::name(), op_name.to_string(), Some(body))
                .await?
        };

        let (error_tx, error_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();

        // register to be notified of error/result
        notifier(self.clone(), msg_id, result_tx).await;
        self.state
            .lock()
            .await
            .node
            .notify_error(msg_id, error_tx)
            .await;

        let timeout = time::sleep(Duration::from_millis(CALL_TIMEOUT_MS));

        // race on the 3 futures and see what we get first
        tokio::select! {
            val = result_rx => {
                Ok(val
                    .map_err(|err| Error::ChannelRecv(format!("{:?}", err)))?)
            },

            Ok(err) = error_rx => {
                Err(err.into())
            },

            _ = timeout => {
                error!("'{op_name}' on seq-kv service timed out.");
                Err(Error::Timeout(format!("'{op_name}' on seq-kv service timed out.")))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Read {
    key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ReadOk {
    value: JsonValue,
}

async fn handle_read_ok(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some((Some(in_reply_to), read_ok)) = msg
        .body
        .parse::<Reply<ReadOk>>()?
        .and_then(|b| b.body.map(|read_ok| (b.in_reply_to, read_ok)))
    {
        ctx.node
            .for_service(move |svc: SeqKv| {
                async move {
                    svc.state
                        .lock()
                        .await
                        .read_notify
                        .remove(&in_reply_to)
                        .filter(|tx| !tx.is_closed())
                        .map(move |tx| {
                            tx.send(read_ok.value).map_err(|_| {
                                error!("handle_read_ok: sending read result to channel failed");
                                Error::ChannelSend(
                                    "Sending read result to SeqKv channel failed.".to_string(),
                                )
                            })
                        })
                        // if we don't find a message id that's waiting for a response in
                        // read_notify, we just drop the read_ok message
                        .unwrap_or(Ok(()))
                }
            })
            .await
    } else {
        Ok(())
    }
}

async fn handle_mut_op_ok<F>(
    ctx: Context,
    msg: Message,
    map_get: F,
    op_name: &'static str,
) -> Result<(), Error>
where
    F: Fn(MutexGuard<'_, State>, u64) -> Option<oneshot::Sender<()>>,
{
    if let Some(in_reply_to) = msg.body.parse::<Reply<()>>()?.and_then(|b| b.in_reply_to) {
        ctx.node
            .for_service(|svc: SeqKv| {
                async move {
                    map_get(svc.state.lock().await, in_reply_to)
                        .filter(|tx| !tx.is_closed())
                        .map(|tx| {
                            tx.send(()).map_err(|_| {
                                error!(
                                    "handle_{op_name}_ok: sending {op_name} result to channel failed"
                                );
                                Error::ChannelSend(format!(
                                    "Sending {op_name} result to SeqKv channel failed."
                                ))
                            })
                        })
                        // if we don't find a message id that's waiting for a response in
                        // write_notify/cas_notify, we just drop the *_ok message
                        .unwrap_or(Ok(()))
                    }
            })
            .await
    } else {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Write<T: Serialize> {
    key: String,
    value: T,
}

async fn handle_write_ok(ctx: Context, msg: Message) -> Result<(), Error> {
    handle_mut_op_ok(
        ctx,
        msg,
        |mut svc, msg_id| svc.write_notify.remove(&msg_id),
        "write",
    )
    .await
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Cas<T: Serialize> {
    key: String,
    from: T,
    to: T,
    create_if_not_exists: bool,
}

async fn handle_cas_ok(ctx: Context, msg: Message) -> Result<(), Error> {
    handle_mut_op_ok(
        ctx,
        msg,
        |mut svc, msg_id| svc.cas_notify.remove(&msg_id),
        "cas",
    )
    .await
}
