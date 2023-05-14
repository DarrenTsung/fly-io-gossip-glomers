#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct Echo {}

impl maelstrom::App for Echo {
    type Payload = EchoPayload;

    fn new(_node_id: maelstrom::NodeID, _node_ids: Vec<maelstrom::NodeID>) -> Self {
        Self {}
    }

    fn handle(
        &mut self,
        message: maelstrom::Message<Self::Payload>,
        writer: &mut maelstrom::MessageWriter,
    ) -> Result<(), anyhow::Error> {
        let maelstrom::MessagePayload::App(app_payload) = &message.body.payload else {
            eprintln!("Ignoring non-app payload, got: {message:?}!");
            return Ok(());
        };

        match app_payload {
            EchoPayload::Echo { echo } => {
                writer.reply_to(&message, EchoPayload::EchoOk { echo: echo.clone() })?;
            }
            _ => {
                eprintln!("Ignoring non-relevant payload: {app_payload:?}.");
                return Ok(());
            }
        }

        Ok(())
    }

    fn tick<'a>(&mut self, _writer: &mut maelstrom::MessageWriter<'a>) -> anyhow::Result<()> {
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    maelstrom::event_loop::<Echo, EchoPayload>()
}
