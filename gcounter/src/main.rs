use std::sync::Mutex;

use anyhow::Result;
use maelstrom_lib::{Context, Error, Message, Node, SeqKv};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

const COUNTER_KEY: &'static str = "gcounter";

#[derive(Debug)]
struct State {
    counter: i64,
}

fn state() -> &'static Mutex<State> {
    static INSTANCE: OnceCell<Mutex<State>> = OnceCell::new();

    INSTANCE.get_or_init(|| Mutex::new(State { counter: 0 }))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stderr).init();

    let mut node = Node::new().await;
    node.handle("add", handle_add).await;
    node.handle("read", handle_read).await;

    node.run().await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Add {
    delta: i64,
}

async fn handle_add(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(add) = msg.body.parse::<Add>()? {
        ctx.node()
            .for_service(move |mut seqkv: SeqKv| async move {
                let counter = { state().lock().unwrap().counter };
                seqkv
                    .cas(COUNTER_KEY, counter, counter + add.delta, true)
                    .await?;

                Ok(())
            })
            .await?;
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Read {
    value: i64,
}

async fn handle_read(_ctx: Context, _msg: Message) -> Result<(), Error> {
    Ok(())
}
