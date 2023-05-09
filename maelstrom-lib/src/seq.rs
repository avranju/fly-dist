use std::collections::HashMap;

use log::error;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::oneshot;

use crate::{Context, Error, Message, Node, Reply, Service};

pub struct SeqKv {
    node: Node,
    read_notify: HashMap<u64, oneshot::Sender<JsonValue>>,
    write_notify: HashMap<u64, oneshot::Sender<()>>,
    cas_notify: HashMap<u64, oneshot::Sender<()>>,
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
            node,
            read_notify: HashMap::new(),
            write_notify: HashMap::new(),
            cas_notify: HashMap::new(),
        }
    }

    pub fn notify_read(&mut self, msg_id: u64, tx: oneshot::Sender<JsonValue>) {
        if let Some(e) = self.read_notify.get_mut(&msg_id) {
            *e = tx;
        } else {
            self.read_notify.insert(msg_id, tx);
        }
    }

    pub fn notify_write(&mut self, msg_id: u64, tx: oneshot::Sender<()>) {
        if let Some(e) = self.write_notify.get_mut(&msg_id) {
            *e = tx;
        } else {
            self.write_notify.insert(msg_id, tx);
        }
    }

    pub fn notify_cas(&mut self, msg_id: u64, tx: oneshot::Sender<()>) {
        if let Some(e) = self.cas_notify.get_mut(&msg_id) {
            *e = tx;
        } else {
            self.cas_notify.insert(msg_id, tx);
        }
    }

    pub async fn read<T: DeserializeOwned>(&mut self, key: &str) -> Result<T, Error> {
        // send out the read key request
        let msg_id = self
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
        self.notify_read(msg_id, result_tx);
        self.node.notify_error(msg_id, error_tx).await;

        //TODO: Also select on a timeout future below.

        // race on the 2 futures and see what we get first
        tokio::select! {
            val = result_rx => {
                Ok(val
                    .map_err(|err| Error::ChannelRecv(format!("{:?}", err)))
                    .and_then(|val| serde_json::from_value(val).map_err(Error::Json))?)
            },
            Ok(err) = error_rx => {
                Err(err.into())
            }
        }
    }

    pub async fn write<T: Serialize>(&mut self, key: &str, value: T) -> Result<(), Error> {
        // send out the write key/value request
        let msg_id = self
            .node
            .send_value(
                SeqKv::name(),
                "write".to_string(),
                Some(Write {
                    key: key.to_string(),
                    value,
                }),
            )
            .await?;

        let (error_tx, error_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();

        // register to be notified of error/result
        self.notify_write(msg_id, result_tx);
        self.node.notify_error(msg_id, error_tx).await;

        //TODO: Also select on a timeout future below.

        // race on the 2 futures and see what we get first
        tokio::select! {
            val = result_rx => {
                Ok(val
                    .map_err(|err| Error::ChannelRecv(format!("{:?}", err)))?)
            },
            Ok(err) = error_rx => {
                Err(err.into())
            }
        }
    }

    pub async fn cas<T: Serialize>(&mut self, key: &str, from: T, to: T) -> Result<(), Error> {
        // send out the cas request
        let msg_id = self
            .node
            .send_value(
                SeqKv::name(),
                "cas".to_string(),
                Some(Cas {
                    key: key.to_string(),
                    from,
                    to,
                }),
            )
            .await?;

        let (error_tx, error_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();

        // register to be notified of error/result
        self.notify_cas(msg_id, result_tx);
        self.node.notify_error(msg_id, error_tx).await;

        //TODO: Also select on a timeout future below.

        // race on the 2 futures and see what we get first
        tokio::select! {
            val = result_rx => {
                Ok(val
                    .map_err(|err| Error::ChannelRecv(format!("{:?}", err)))?)
            },
            Ok(err) = error_rx => {
                Err(err.into())
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
        ctx.node.for_service_mut::<SeqKv, _, ()>(|svc| {
            svc.read_notify
                .remove(&in_reply_to)
                .map(|tx| {
                    if !tx.is_closed() {
                        tx.send(read_ok.value.clone()).map_err(|_| {
                            error!("handle_read_ok: sending read result to channel failed");
                            Error::ChannelSend(
                                "Sending read result to SeqKv channel failed.".to_string(),
                            )
                        })
                    } else {
                        Ok(())
                    }
                })
                // if we don't find a message id that's waiting for a response in
                // read_notify, we just drop the read_ok message
                .unwrap_or(Ok(()))
        })
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
    F: Fn(&mut SeqKv) -> &mut HashMap<u64, oneshot::Sender<()>>,
{
    if let Some(in_reply_to) = msg.body.parse::<Reply<()>>()?.and_then(|b| b.in_reply_to) {
        ctx.node.for_service_mut::<SeqKv, _, ()>(|svc| {
            map_get(svc)
                .remove(&in_reply_to)
                .map(|tx| {
                    if !tx.is_closed() {
                        tx.send(()).map_err(|_| {
                            error!(
                                "handle_{op_name}_ok: sending {op_name} result to channel failed"
                            );
                            Error::ChannelSend(format!(
                                "Sending {op_name} result to SeqKv channel failed."
                            ))
                        })
                    } else {
                        Ok(())
                    }
                })
                // if we don't find a message id that's waiting for a response in
                // read_notify/cas_notify, we just drop the *_ok message
                .unwrap_or(Ok(()))
        })
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
    handle_mut_op_ok(ctx, msg, |svc| &mut svc.write_notify, "write").await
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Cas<T: Serialize> {
    key: String,
    from: T,
    to: T,
}

async fn handle_cas_ok(ctx: Context, msg: Message) -> Result<(), Error> {
    handle_mut_op_ok(ctx, msg, |svc| &mut svc.cas_notify, "cas").await
}
