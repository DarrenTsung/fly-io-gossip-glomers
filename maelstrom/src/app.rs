use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use crate::protocol::*;
use std::fmt::Debug;
use std::io::{self, StdoutLock, Write};

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
        payload: impl Into<MessagePayload<TPayload>>,
    ) -> anyhow::Result<()> {
        self.write_message(&Message {
            src: self.node_id.clone(),
            dst: received_message.src.clone(),
            body: MessageBody {
                msg_id: Some(self.msg_id),
                in_reply_to: received_message.body.msg_id,
                payload: payload.into(),
            },
        })?;
        Ok(())
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
}

struct AppContext<'a, TApp> {
    app: TApp,
    writer: MessageWriter<'a>,
}

pub fn event_loop<TApp: App<Payload = TPayload>, TPayload: Serialize + DeserializeOwned + Debug>(
) -> anyhow::Result<()> {
    let messages =
        serde_json::Deserializer::from_reader(io::stdin().lock()).into_iter::<Message<TPayload>>();

    let mut context = None;
    for message in messages {
        let message = message.context("Couldn't deserialize Message from stdin")?;
        eprintln!("Received message: {message:#?}.");
        let Some(context) = &mut context else {
            let MessagePayload::Shared(SharedMessagePayload::Init { node_id, node_ids }) = &message.body.payload else {
                anyhow::bail!("Did not get Init message as first message, got: {message:?}!");
            };
            let mut inner_context = AppContext {
                writer: MessageWriter { msg_id: 0.into(), stdout: io::stdout().lock(), node_id: node_id.clone() },
                app: TApp::new(node_id.clone(), node_ids.clone())
            };
            inner_context.writer.reply_to(&message, MessagePayload::Shared(SharedMessagePayload::InitOk))?;
            context = Some(inner_context);
            continue;
        };

        context
            .app
            .handle(message, &mut context.writer)
            .context("App failed to handle message")?;
    }

    Ok(())
}
