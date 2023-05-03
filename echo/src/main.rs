use anyhow::Result;
use log::trace;
use maelstrom_lib::{Context, Error, Message, Node};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stderr).init();

    let mut node = Node::new().await;
    node.handle("echo", handle_echo).await;
    node.run().await?;

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Echo {
    echo: String,
}

async fn handle_echo(ctx: Context, msg: Message) -> Result<(), Error> {
    if let Some(echo) = msg.body.parse::<Echo>()? {
        trace!("Received echo: {}", echo.echo);

        ctx.reply_to_with(msg, "echo_ok".to_string(), Some(echo))
            .await?;
    }

    Ok(())
}
