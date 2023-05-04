use std::{collections::HashMap, sync::Mutex};

use anyhow::Result;
use futures::future::join_all;
use log::trace;
use maelstrom_lib::{Context, Error, Message, Node};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct State {
    messages: Vec<i64>,
    neighbours: Vec<String>, // node ids of neighbours
}

fn state() -> &'static Mutex<State> {
    static INSTANCE: OnceCell<Mutex<State>> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        Mutex::new(State {
            messages: vec![],
            neighbours: vec![],
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
    node.run().await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Broadcast {
    message: i64,
}

async fn handle_broadcast(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(broadcast) = msg.body.parse::<Broadcast>()? {
        trace!("Received broadcast: {}", broadcast.message);

        let f = {
            let mut state = state().lock().unwrap();

            // if this message hasn't been seen before then we add it to our
            // vec and forward it along to our neighbours
            state.messages.contains(&broadcast.message).then(|| {
                state.messages.push(broadcast.message);

                // send this message to all of our neighbours
                state.neighbours.clone().into_iter().map(|node_id| {
                    ctx.send(node_id, "broadcast".to_string(), Some(broadcast.clone()))
                })
            })
        };

        if let Some(f) = f {
            // send the message to our neighbours
            let _ = join_all(f).await;
        }

        // inter-server messages don't have a message id and don't need a response
        if msg.body.msg_id.is_some() {
            ctx.reply_to(msg, "broadcast_ok".to_string()).await?;
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct ReadResponse {
    messages: Vec<i64>,
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
