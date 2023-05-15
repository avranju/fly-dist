use std::collections::HashMap;

use anyhow::Result;
use maelstrom_lib::{Context, Error, Message, Node};
use serde::{Deserialize, Serialize};
use state::state;

mod log;
mod state;

use crate::log::{LogEntry, Offset};

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stderr).init();

    let mut node = Node::new().await;
    node.handle("send", handle_send).await;
    node.handle("poll", handle_poll).await;
    node.handle("commit_offsets", handle_commit_offsets).await;
    node.handle("list_committed_offsets", handle_list_committed_offsets)
        .await;

    node.run().await?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Send {
    key: String,
    msg: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SendOk {
    offset: Offset,
}

async fn handle_send(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(send) = msg.body.parse::<Send>()? {
        let offset = state().lock().unwrap().append(send.key, send.msg);
        ctx.reply_to_with(msg, "send_ok".into(), Some(SendOk { offset }))
            .await?;
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Poll {
    offsets: HashMap<String, Offset>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PollOk {
    msgs: HashMap<String, Vec<LogEntry>>,
}

async fn handle_poll(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(poll) = msg.body.parse::<Poll>()? {
        let poll_ok = {
            let state = state().lock().unwrap();
            let msgs = poll
                .offsets
                .iter()
                .filter_map(|(key, offset)| state.poll(key, *offset).map(|res| (key.clone(), res)))
                .collect();
            PollOk { msgs }
        };

        ctx.reply_to_with(msg, "poll_ok".into(), Some(poll_ok))
            .await?;
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CommitOffsets {
    offsets: HashMap<String, Offset>,
}

async fn handle_commit_offsets(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(co) = msg.body.parse::<CommitOffsets>()? {
        {
            let state = state().lock().unwrap();
            for (key, offset) in co.offsets {
                state.commit(&key, offset);
            }
        }

        ctx.reply_to(msg, "commit_offsets_ok".into()).await?;
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ListCommittedOffsets {
    keys: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CommittedOffsets {
    offsets: HashMap<String, Offset>,
}

async fn handle_list_committed_offsets(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(lco) = msg.body.parse::<ListCommittedOffsets>()? {
        let offsets = {
            let state = state().lock().unwrap();
            lco.keys
                .iter()
                .filter_map(|key| {
                    state
                        .committed_offset(key)
                        .map(|offset| (key.clone(), offset))
                })
                .collect()
        };

        ctx.reply_to_with(
            msg,
            "list_committed_offsets_ok".into(),
            Some(CommittedOffsets { offsets }),
        )
        .await?;
    }

    Ok(())
}
