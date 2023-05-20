use uuid::Uuid;

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum UniqueIdsPayload {
    Generate,
    GenerateOk { id: Uuid },
}

struct UniqueIds {}

impl maelstrom::App for UniqueIds {
    type Payload = UniqueIdsPayload;

    fn new(_node_id: maelstrom::NodeID, _node_ids: Vec<maelstrom::NodeID>) -> Self {
        Self {}
    }

    fn handle(
        &mut self,
        message: maelstrom::Message<Self::Payload>,
        writer: &mut maelstrom::MessageWriter,
    ) -> Result<(), anyhow::Error> {
        match &message.body.payload {
            UniqueIdsPayload::Generate => {
                writer.reply_to(
                    &message,
                    UniqueIdsPayload::GenerateOk { id: Uuid::new_v4() },
                )?;
            }
            _ => {
                eprintln!("Ignoring non-relevant payload: {message:?}.");
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
    maelstrom::event_loop::<UniqueIds, UniqueIdsPayload>()
}
