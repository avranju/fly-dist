use std::time::Instant;

#[cfg(feature = "seqkv")]
#[cfg(feature = "seqkv")]
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::Result;

#[cfg(feature = "seqkv")]
use async_recursion::async_recursion;

use log::trace;
use maelstrom_lib::{Context, Error, Message, Node};

#[cfg(feature = "seqkv")]
use maelstrom_lib::SeqKv;

#[cfg(feature = "redis")]
use once_cell::sync::OnceCell;
#[cfg(feature = "redis")]
use redis::{aio::Connection as RedisConnection, AsyncCommands, Client as RedisClient};
#[cfg(feature = "redis")]
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};

const COUNTER_KEY: &str = "gcounter";

#[cfg(feature = "seqkv")]
#[derive(Debug)]
struct State {
    counter: AtomicI64,
}

#[cfg(feature = "seqkv")]
impl State {
    fn load(&self) -> i64 {
        self.counter.load(Ordering::SeqCst)
    }

    fn add(&self, delta: i64) -> i64 {
        self.counter.fetch_add(delta, Ordering::SeqCst)
    }

    fn set(&self, val: i64) {
        self.counter.store(val, Ordering::SeqCst);
    }
}

#[cfg(feature = "seqkv")]
fn state() -> &'static State {
    static INSTANCE: State = State {
        counter: AtomicI64::new(0),
    };

    &INSTANCE
}

#[cfg(feature = "redis")]
struct RedisState {
    client: RedisClient,
    connection: Option<RedisConnection>,
}

#[cfg(feature = "redis")]
impl RedisState {
    async fn connection(&mut self) -> &mut RedisConnection {
        if self.connection.is_none() {
            self.connection = Some(
                self.client
                    .get_async_connection()
                    .await
                    .expect("Could not connect to redis."),
            );
        }

        self.connection
            .as_mut()
            .expect("Redis connection must exist")
    }
}

#[cfg(feature = "redis")]
fn redis() -> &'static Mutex<RedisState> {
    static INSTANCE: OnceCell<Mutex<RedisState>> = OnceCell::new();

    INSTANCE.get_or_init(|| {
        let client =
            RedisClient::open("redis://127.0.0.1:6379/").expect("Could not create redis client");

        Mutex::new(RedisState {
            client,
            connection: None,
        })
    })
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

#[cfg(feature = "seqkv")]
#[async_recursion]
async fn do_add(
    mut seqkv: SeqKv,
    mut counter: i64,
    delta: i64,
    ctx: Context,
    msg: Message,
) -> Result<(), Error> {
    match seqkv.cas(COUNTER_KEY, counter, counter + delta, true).await {
        Ok(_) => {
            state().add(delta);
            ctx.reply_to(msg, "add_ok".to_string()).await?;
            Ok(())
        }
        Err(Error::KvCasMismatch) => {
            counter = seqkv.read(COUNTER_KEY).await?;
            state().set(counter);

            // retry the add request by sending it to ourselves
            Ok(do_add(seqkv, counter, delta, ctx, msg).await?)
        }
        Err(err) => Err(err),
    }
}

async fn handle_add(ctx: Context, msg: Message) -> Result<(), Error> {
    let _trace = TraceCallTime::new(format!("[{}] handle_add", msg.body.msg_id.unwrap_or(0)));

    if let Some(add) = msg.body.parse::<Add>()? {
        #[cfg(feature = "seqkv")]
        {
            let node = ctx.node().clone();
            node.for_service(move |seqkv: SeqKv| async move {
                let counter = state().load();

                trace!("handle_add: counter = {}, delta = {}", counter, add.delta);
                do_add(seqkv, counter, add.delta, ctx, msg).await
            })
            .await?;
        }

        #[cfg(feature = "redis")]
        {
            let mut redis = redis().lock().await;
            let connection = redis.connection().await;
            let counter: i64 = connection
                .incr(COUNTER_KEY, add.delta)
                .await
                .expect("Could not increment counter in redis");
            trace!(
                "handle_add: counter = {}, delta = {}",
                counter - add.delta,
                add.delta
            );
            ctx.reply_to(msg, "add_ok".to_string()).await?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Read {
    value: i64,
}

async fn handle_read(ctx: Context, msg: Message) -> Result<(), Error> {
    let _trace = TraceCallTime::new(format!("[{}] handle_read", msg.body.msg_id.unwrap_or(0)));

    #[cfg(feature = "seqkv")]
    {
        let node = ctx.node().clone();
        node.for_service(|mut seqkv: SeqKv| async move {
            let mut counter = state().load();

            trace!(
                "handle_read: calling seqkv.cas for msg {:?}.",
                msg.body.msg_id
            );

            match seqkv.cas(COUNTER_KEY, counter, counter, true).await {
                Ok(_) => {
                    trace!(
                        "handle_read: sending read_ok for msg {:?}.",
                        msg.body.msg_id
                    );
                    ctx.reply_to_with(msg, "read_ok".to_string(), Some(Read { value: counter }))
                        .await?;
                    Ok(())
                }
                Err(Error::KvCasMismatch) => {
                    trace!(
                        "handle_read: got cas mismatch. calling seqkv.read for msg {:?}.",
                        msg.body.msg_id
                    );
                    counter = seqkv.read(COUNTER_KEY).await?;
                    state().set(counter);

                    trace!(
                        "handle_read: got new value. sending read_ok for for msg {:?}.",
                        msg.body.msg_id
                    );
                    ctx.reply_to_with(msg, "read_ok".to_string(), Some(Read { value: counter }))
                        .await?;
                    Ok(())
                }
                Err(err) => {
                    trace!("handle_read: got error for msg {:?}.", msg.body.msg_id);
                    Err(err)
                }
            }
        })
        .await?;
    }

    #[cfg(feature = "redis")]
    {
        let mut redis = redis().lock().await;
        let connection = redis.connection().await;
        let counter: i64 = connection
            .get(COUNTER_KEY)
            .await
            .expect("Could not get counter value from redis.");
        ctx.reply_to_with(msg, "read_ok".to_string(), Some(Read { value: counter }))
            .await?;
    }

    Ok(())
}

struct TraceCallTime {
    name: String,
    start: Instant,
}

impl TraceCallTime {
    fn new(name: String) -> Self {
        TraceCallTime {
            name,
            start: Instant::now(),
        }
    }
}

impl Drop for TraceCallTime {
    fn drop(&mut self) {
        trace!(
            "TraceCallTime: '{}' took {} ms.",
            self.name,
            self.start.elapsed().as_millis()
        );
    }
}
