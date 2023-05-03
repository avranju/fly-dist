use std::sync::Mutex;

use anyhow::{anyhow, Result};
use log::{error, trace};
use maelstrom_lib::{Context, Error, Message, Node};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

// Implementation ported from: https://www.callicoder.com/distributed-unique-id-sequence-number-generator/

// Custom Epoch (January 1, 2015 Midnight UTC = 2015-01-01T00:00:00Z)
const CUSTOM_EPOCH: i64 = 1420070400000;

const NODE_ID_BITS: u64 = 10;
const SEQUENCE_BITS: u64 = 12;

#[derive(Debug)]
struct State {
    last_timestamp: i64,
    sequence: u64,
    node_id: Option<u16>,
    max_sequence: u64,
}

fn state() -> &'static Mutex<State> {
    static INSTANCE: OnceCell<Mutex<State>> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        Mutex::new(State {
            last_timestamp: 0,
            sequence: 0,
            node_id: None,
            max_sequence: u64::pow(2, SEQUENCE_BITS as u32) - 1,
        })
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stderr).init();

    let mut node = Node::new().await;
    node.handle("generate", handle_generate).await;
    node.run().await?;

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct UniqueId {
    id: u64,
}

async fn handle_generate(ctx: Context, msg: Message) -> Result<(), Error> {
    // this artificial code block exists because the mutex guard returned
    // by Mutex::lock cannot cross an await point (i.e., it does not implement
    // Send).
    let id = {
        trace!("[{}] Generating unique ID.", ctx.node_id());

        let node_id = ctx.node_id();
        let mut state = state().lock().unwrap();

        // initialise node id if we need to
        if state.node_id.is_none() {
            state.node_id = Some(
                parse_node_id(node_id)
                    .map_err(|_| Error::Parse(format!("Invalid node ID {node_id}")))?,
            );
        }

        let node_id = state
            .node_id
            .expect("Node ID must have been initialised by now.");
        let mut current_timestamp = timestamp();
        if current_timestamp < state.last_timestamp {
            // invalid system clock
            error!("[{}] Invalid system clock. current_timestamp = {current_timestamp}, last_timestamp = {}", ctx.node_id(), state.last_timestamp);
            return Err(Error::Unknown);
        }

        if current_timestamp == state.last_timestamp {
            state.sequence = (state.sequence + 1) & state.max_sequence;
            if state.sequence == 0 {
                current_timestamp = wait_next_ms(current_timestamp, state.last_timestamp);
            }
        } else {
            state.sequence = 0;
        }

        state.last_timestamp = current_timestamp;

        let mut id = UniqueId {
            id: (current_timestamp as u64) << (NODE_ID_BITS + SEQUENCE_BITS),
        };
        id.id |= (node_id as u64) << SEQUENCE_BITS;
        id.id |= state.sequence;

        trace!("[{}] Generated unique ID {}", node_id, id.id);

        id
    };

    ctx.reply_to_with(msg, "generate_ok".to_string(), Some(id))
        .await
}

fn wait_next_ms(mut current_timestamp: i64, last_timestamp: i64) -> i64 {
    while current_timestamp == last_timestamp {
        current_timestamp = timestamp();
    }

    current_timestamp
}

fn parse_node_id(node_id: &str) -> Result<u16> {
    // node ids start with 'n' followed by some number; we parse
    // that out and return
    if node_id.len() < 2 {
        Err(anyhow!("Invalid node ID {}", node_id))
    } else {
        Ok(node_id[1..].parse()?)
    }
}

fn timestamp() -> i64 {
    chrono::Utc::now().timestamp_millis() - CUSTOM_EPOCH
}
