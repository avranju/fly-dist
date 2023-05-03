use std::sync::Mutex;

use anyhow::Result;
use log::trace;
use maelstrom_lib::{Context, Error, Message, Node};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct State {
    messages: Vec<i64>,
}

fn state() -> &'static Mutex<State> {
    static INSTANCE: OnceCell<Mutex<State>> = OnceCell::new();
    INSTANCE.get_or_init(|| Mutex::new(State { messages: vec![] }))
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

#[derive(Serialize, Deserialize)]
struct Broadcast {
    message: i64,
}

async fn handle_broadcast(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(broadcast) = msg.body.parse::<Broadcast>()? {
        trace!("Received broadcast: {}", broadcast.message);

        {
            state().lock().unwrap().messages.push(broadcast.message);
        }

        ctx.reply_to(msg, "broadcast_ok".to_string()).await?;
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Read {
    messages: Vec<i64>,
}

async fn handle_read(ctx: Context, msg: Message) -> Result<(), Error> {
    trace!("Received read");

    let messages = { state().lock().unwrap().messages.clone() };
    ctx.reply_to_with(msg, "read_ok".to_string(), Some(Read { messages }))
        .await
}

async fn handle_topology(ctx: Context, msg: Message) -> Result<(), Error> {
    trace!("Received topology");

    ctx.reply_to(msg, "topology_ok".to_string()).await
}
