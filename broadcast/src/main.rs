use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::Result;
use futures::future::join_all;
use log::{trace, warn};
use maelstrom_lib::{Context, Error, Message, Node};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use tokio::{sync::watch, time::sleep};

// Duration we wait for an ack for a message store request before
// retrying the request.
const SEND_TIMEOUT_MS: u64 = 2000;

#[derive(Debug)]
struct SendState {
    timestamp: Instant,
    node_id: String,
    message: i64,
}

impl SendState {
    fn new(timestamp: Instant, dst: String, message: i64) -> Self {
        SendState {
            timestamp,
            node_id: dst,
            message,
        }
    }
}

#[derive(Debug)]
struct State {
    messages: HashSet<i64>,
    neighbours: Vec<String>,    // node ids of neighbours
    send_queue: Vec<SendState>, // message sends that need to be tracked for delivery
    send_watch_tx: watch::Sender<()>,
    send_watch_rx: Option<watch::Receiver<()>>,
}

fn state() -> &'static Mutex<State> {
    static INSTANCE: OnceCell<Mutex<State>> = OnceCell::new();

    let (send_watch_tx, send_watch_rx) = watch::channel(());

    INSTANCE.get_or_init(|| {
        Mutex::new(State {
            messages: HashSet::new(),
            neighbours: vec![],
            send_queue: vec![],
            send_watch_tx,
            send_watch_rx: Some(send_watch_rx),
        })
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stderr).init();

    let mut node = Node::new().await;
    node.handle("broadcast", handle_broadcast).await;
    node.handle("read", handle_read).await;
    node.handle("topology", handle_topology).await;
    node.handle("store", handle_store).await;
    node.handle("store_ok", handle_store_ok).await;

    // kick off the send watch task
    tokio::spawn(send_scan(node.clone()));

    node.run().await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Broadcast {
    message: i64,
}

async fn send_scan(node: Node) {
    let mut send_watch_rx = state()
        .lock()
        .unwrap()
        .send_watch_rx
        .take()
        .expect("Send watch receiver must not be none.");

    let send_timeout = Duration::from_millis(SEND_TIMEOUT_MS);

    while send_watch_rx.changed().await.is_ok() {
        let now = Instant::now();
        let send_futures = {
            state()
                .lock()
                .unwrap()
                .send_queue
                .iter_mut()
                .filter(|e| now - e.timestamp >= send_timeout)
                .map(|e| {
                    // update the timestamp when the retry was done
                    e.timestamp = now;

                    trace!(
                        "Retrying store message for node {} with message {}.",
                        e.node_id,
                        e.message
                    );

                    node.send_value(
                        e.node_id.clone(),
                        "store".to_string(),
                        Some(Broadcast { message: e.message }),
                    )
                })
                .collect::<Vec<_>>()
        };

        // send the message to our neighbours
        if !send_futures.is_empty() {
            let _ = join_all(send_futures).await;

            // schedule another scan for retries after timeout
            schedule_send_scan();
        }
    }
}

fn schedule_send_scan() {
    tokio::spawn(async move {
        sleep(Duration::from_millis(SEND_TIMEOUT_MS)).await;

        // notify send watch task that there are things to look at
        let _ = state()
            .lock()
            .unwrap()
            .send_watch_tx
            .send(())
            .map_err(|err| warn!("Notifying watch task failed with {err:?}."));
    });
}

async fn handle_broadcast(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(broadcast) = msg.body.parse::<Broadcast>()? {
        trace!("Received broadcast: {}", broadcast.message);

        // we treat all other nodes as our direct neighbour
        let mut neighbours = ctx.node().node_ids().await;
        neighbours.retain(|nid| nid != ctx.node_id());

        let f = {
            let mut st = state().lock().unwrap();

            // if this message hasn't been seen before then we add it to our
            // vec and forward it along to our neighbours
            st.messages.insert(broadcast.message).then(|| {
                // add all the broadcasts to the send queue so we can track
                // its receipt and retry send if necessary
                let now = Instant::now();
                st.send_queue.extend(
                    neighbours
                        .iter()
                        .filter(|node_id| {
                            // don't send the message back to the node that sent it to us
                            **node_id != msg.src
                        })
                        .map(|node_id| SendState::new(now, node_id.clone(), broadcast.message)),
                );

                // schedule a scan of pending sends SEND_TIMEOUT_MS in the future
                schedule_send_scan();

                // send this message to all of our neighbours
                neighbours
                    .into_iter()
                    .filter(|node_id| {
                        // don't send the message back to the node that sent it to us
                        *node_id != msg.src
                    })
                    .map(|node_id| ctx.send(node_id, "store".to_string(), Some(broadcast.clone())))
            })
        };

        if let Some(f) = f {
            // send the message to our neighbours
            let _ = join_all(f).await;
        }

        ctx.reply_to(msg, "broadcast_ok".to_string()).await?;
    }

    Ok(())
}

async fn handle_store(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(broadcast) = msg.body.parse::<Broadcast>()? {
        trace!("Received store: {}", broadcast.message);

        {
            let mut st = state().lock().unwrap();

            // if this message hasn't been seen before then we add it to our
            // vec and forward it along to our neighbours
            st.messages.insert(broadcast.message);
        }
        ctx.reply_to_with(msg, "store_ok".to_string(), Some(broadcast))
            .await?;
    }

    Ok(())
}

async fn handle_store_ok(_ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(broadcast) = msg.body.parse::<Broadcast>()? {
        trace!(
            "Received store_ok from {} for message {}",
            msg.src,
            broadcast.message
        );

        // remove the entry for this send from track queue
        let mut st = state().lock().unwrap();
        if let Some((i, _)) = st
            .send_queue
            .iter()
            .enumerate()
            .find(|(_, e)| e.node_id == msg.src && e.message == broadcast.message)
        {
            st.send_queue.swap_remove(i);
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct ReadResponse {
    messages: HashSet<i64>,
}

async fn handle_read(ctx: Context, msg: Message) -> Result<(), Error> {
    trace!("Received read");

    let messages = { state().lock().unwrap().messages.clone() };
    ctx.reply_to_with(msg, "read_ok".to_string(), Some(ReadResponse { messages }))
        .await
}

#[derive(Debug, Serialize, Deserialize)]
struct Topology {
    topology: HashMap<String, Vec<String>>,
}

async fn handle_topology(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(neighbours) = msg.body.parse::<Topology>()?.and_then(|mut topology| {
        trace!("Received topology: {topology:?}");
        topology.topology.remove(ctx.node_id())
    }) {
        state().lock().unwrap().neighbours = neighbours;
    };

    ctx.reply_to(msg, "topology_ok".to_string()).await
}
