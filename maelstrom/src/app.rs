use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use tokio::task::JoinHandle;

use crate::protocol::*;
use std::fmt::Debug;
use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct MessageWriter {
    msg_id: Arc<AtomicU32>,
    msg_sender: UnboundedSender<String>,
    node_id: NodeID,
}

impl MessageWriter {
    fn write_message<TPayload: Serialize>(
        &mut self,
        message: &Message<TPayload>,
    ) -> anyhow::Result<()> {
        self.msg_sender
            .send(serde_json::to_string(&message).context("Failed to serialize Message")?)
            .context("Could not send to msg_writer task!")?;
        Ok(())
    }

    pub fn reply_to<TPayload: Serialize>(
        &mut self,
        received_message: &Message<TPayload>,
        payload: TPayload,
    ) -> anyhow::Result<MessageID> {
        let message_id = self.msg_id.fetch_add(1, Ordering::SeqCst).into();
        self.write_message(&Message {
            src: self.node_id.clone(),
            dst: received_message.src.clone(),
            body: MessageBody {
                msg_id: Some(message_id),
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
        let message_id = self.msg_id.fetch_add(1, Ordering::SeqCst).into();
        self.write_message(&Message {
            src: self.node_id.clone(),
            dst: node_id.clone(),
            body: MessageBody {
                msg_id: Some(message_id),
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
        writer: &mut MessageWriter,
    ) -> anyhow::Result<()>;
    fn tick<'a>(&mut self, writer: &mut MessageWriter) -> anyhow::Result<()>;
}

pub async fn event_loop<
    TApp: App<Payload = TPayload> + Send + 'static,
    TPayload: 'static + Send + Serialize + DeserializeOwned + Debug,
>() -> anyhow::Result<()> {
    let (message_sender, mut message_receiver) = mpsc::unbounded_channel();
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

    let init_message = message_receiver
        .recv()
        .await
        .context("Failed to receive first message!")?;
    let init_message = serde_json::from_str::<Message<InitPayload>>(&init_message)
        .context("Couldn't deserialize init Message")?;
    let InitPayload::Init { node_id, node_ids } = &init_message.body.payload else {
        anyhow::bail!("Did not get Init message as first message, got: {init_message:?}!");
    };

    let (msg_writer_sender, mut msg_writer_receiver) = mpsc::unbounded_channel::<String>();
    let writer_task_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        while let Some(message) = msg_writer_receiver.recv().await {
            let mut stdout_lock = io::stdout().lock();
            stdout_lock
                .write_all(message.as_bytes())
                .context("Failed to write message to stdout")?;
            stdout_lock
                .write_all(b"\n")
                .context("Failed to write trailing newline")?;
            stdout_lock.flush().context("Could not flush to stdout")?;
        }
        Ok(())
    });

    let mut writer = MessageWriter {
        msg_id: Arc::new(AtomicU32::new(0)),
        msg_sender: msg_writer_sender,
        node_id: node_id.clone(),
    };
    let mut app = TApp::new(node_id.clone(), node_ids.clone());
    writer.reply_to(&init_message, InitPayload::InitOk)?;

    let (app_message_sender, mut app_message_receiver) = mpsc::unbounded_channel::<String>();
    let app_task_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let tick_rate = Duration::from_millis(10);
        let mut last_tick = Instant::now();
        loop {
            let message = match timeout(tick_rate, app_message_receiver.recv()).await {
                Ok(Some(message)) => message,
                Ok(None) => {
                    eprintln!("Message thread finished unexpectedly? Closing event loop.");
                    break;
                }
                Err(_elapsed) => {
                    if last_tick.elapsed() >= tick_rate {
                        app.tick(&mut writer).context("App failed to tick")?;
                        last_tick = Instant::now();
                    }
                    continue;
                }
            };

            eprintln!("Received message: {message:#?}.");
            let message = serde_json::from_str::<Message<TPayload>>(&message)
                .context("Couldn't deserialize Message")?;
            app.handle(message, &mut writer)
                .context("App failed to handle message")?;

            if last_tick.elapsed() >= tick_rate {
                app.tick(&mut writer).context("App failed to tick")?;
                last_tick = Instant::now();
            }
        }
        Ok(())
    });

    while let Some(message) = message_receiver.recv().await {
        // TODO: handle other types of messages (e.g. services) here.
        app_message_sender
            .send(message)
            .context("Failed to send Message to app task!")?;
    }

    app_task_handle.await??;
    writer_task_handle.await??;

    Ok(())
}
