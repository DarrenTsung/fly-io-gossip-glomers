#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct Echo {}

#[async_trait::async_trait]
impl maelstrom::App for Echo {
    type Payload = EchoPayload;

    fn new(_node_id: maelstrom::NodeID, _node_ids: Vec<maelstrom::NodeID>) -> Self {
        Self {}
    }

    async fn handle(
        &mut self,
        message: maelstrom::Message<Self::Payload>,
        writer: &maelstrom::MessageWriter,
    ) -> Result<(), anyhow::Error> {
        match &message.body.payload {
            EchoPayload::Echo { echo } => {
                writer.reply_to(&message, EchoPayload::EchoOk { echo: echo.clone() })?;
            }
            _ => {
                eprintln!("Ignoring non-relevant payload: {message:?}.");
                return Ok(());
            }
        }

        Ok(())
    }

    async fn tick(&mut self, _writer: &maelstrom::MessageWriter) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    maelstrom::event_loop::<Echo, EchoPayload>().await
}
