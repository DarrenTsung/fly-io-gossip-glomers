use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use crate::protocol::*;
use std::fmt::Debug;
use std::io::{self, BufRead, StdoutLock, Write};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

pub struct MessageWriter<'a> {
    msg_id: MessageID,
    stdout: StdoutLock<'a>,
    node_id: NodeID,
}

impl<'a> MessageWriter<'a> {
    fn write_message<TPayload: Serialize>(
        &mut self,
        message: &Message<TPayload>,
    ) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.stdout, message)
            .context("Failed to serialize message to stdout")?;
        self.stdout
            .write_all(b"\n")
            .context("Failed to write trailing newline")?;
        self.stdout.flush().context("Could not flush to stdout")?;
        *self.msg_id += 1;
        Ok(())
    }

    pub fn reply_to<TPayload: Serialize>(
        &mut self,
        received_message: &Message<TPayload>,
        payload: TPayload,
    ) -> anyhow::Result<MessageID> {
        let message_id = self.msg_id;
        self.write_message(&Message {
            src: self.node_id.clone(),
            dst: received_message.src.clone(),
            body: MessageBody {
                msg_id: Some(self.msg_id),
                in_reply_to: received_message.body.msg_id,
                payload,
            },
        })?;
        Ok(message_id)
    }

    pub fn send_to<TPayload: Serialize>(
        &mut self,
        node_id: &NodeID,
        payload: TPayload,
    ) -> anyhow::Result<MessageID> {
        let message_id = self.msg_id;
        self.write_message(&Message {
            src: self.node_id.clone(),
            dst: node_id.clone(),
            body: MessageBody {
                // Send() sends a fire-and-forget message and doesn't expect a response.
                // As such, it does not attach a message ID.
                msg_id: Some(self.msg_id),
                in_reply_to: None,
                payload,
            },
        })?;
        Ok(message_id)
    }
}

pub trait App {
    type Payload;

    fn new(node_id: NodeID, node_ids: Vec<NodeID>) -> Self;
    fn handle<'a>(
        &mut self,
        message: Message<Self::Payload>,
        writer: &mut MessageWriter<'a>,
    ) -> anyhow::Result<()>;
    fn tick<'a>(&mut self, writer: &mut MessageWriter<'a>) -> anyhow::Result<()>;
}

struct AppContext<'a, TApp> {
    app: TApp,
    writer: MessageWriter<'a>,
}

pub fn event_loop<
    TApp: App<Payload = TPayload>,
    TPayload: 'static + Send + Serialize + DeserializeOwned + Debug,
>() -> anyhow::Result<()> {
    let (message_sender, message_receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let stdin = io::stdin().lock();
        for line in stdin.lines() {
            let line = line.expect("can read line");
            if message_sender.send(line).is_err() {
                eprintln!("Message thread could not send message (receiver gone?). Exiting.");
                break;
            }
        }
    });

    let tick_rate = Duration::from_millis(10);
    let mut context: Option<AppContext<TApp>> = None;
    let mut last_tick = Instant::now();
    loop {
        let message = match message_receiver.recv_timeout(tick_rate) {
            Ok(message) => message,
            Err(RecvTimeoutError::Disconnected) => {
                eprintln!("Message thread finished unexpectedly? Closing event loop.");
                break;
            }
            Err(RecvTimeoutError::Timeout) => {
                if let Some(context) = &mut context {
                    if last_tick.elapsed() >= tick_rate {
                        context
                            .app
                            .tick(&mut context.writer)
                            .context("App failed to tick")?;
                        last_tick = Instant::now();
                    }
                }
                continue;
            }
        };

        eprintln!("Received message: {message:#?}.");
        let Some(context) = &mut context else {
            let message = serde_json::from_str::<Message<InitPayload>>(&message)
                .context("Couldn't deserialize Message")?;
            let InitPayload::Init { node_id, node_ids } = &message.body.payload else {
                anyhow::bail!("Did not get Init message as first message, got: {message:?}!");
            };
            let mut inner_context = AppContext {
                writer: MessageWriter { msg_id: 0.into(), stdout: io::stdout().lock(), node_id: node_id.clone() },
                app: TApp::new(node_id.clone(), node_ids.clone())
            };
            inner_context.writer.reply_to(&message, InitPayload::InitOk)?;
            context = Some(inner_context);
            continue;
        };

        let message = serde_json::from_str::<Message<TPayload>>(&message)
            .context("Couldn't deserialize Message")?;
        context
            .app
            .handle(message, &mut context.writer)
            .context("App failed to handle message")?;

        if last_tick.elapsed() >= tick_rate {
            context
                .app
                .tick(&mut context.writer)
                .context("App failed to tick")?;
            last_tick = Instant::now();
        }
    }

    Ok(())
}
