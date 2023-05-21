use maelstrom::*;

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GCounterPayload {
    Add { delta: u32 },
    AddOk,
    Read,
    ReadOk { value: u32 },
}

struct GCounter {}

#[async_trait::async_trait]
impl maelstrom::App for GCounter {
    type Payload = GCounterPayload;

    fn new(_node_id: maelstrom::NodeID, _node_ids: Vec<maelstrom::NodeID>) -> Self {
        Self {}
    }

    async fn handle(
        &mut self,
        _message: maelstrom::Message<Self::Payload>,
        _writer: &maelstrom::MessageWriter,
    ) -> Result<(), anyhow::Error> {
        let kv = SeqKV::new(_writer);
        let v: Option<String> = kv.read("hello").await?;
        eprintln!("read - hello: {v:?}.");
        kv.write("hello", "world").await?;
        let v: Option<String> = kv.read("hello").await?;
        eprintln!("2nd read - hello: {v:?}.");
        Ok(())
    }

    fn tick(&mut self, _writer: &maelstrom::MessageWriter) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    maelstrom::event_loop::<GCounter, GCounterPayload>().await
}
