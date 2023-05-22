use maelstrom::*;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: u32 },
    AddOk,
    Read,
    ReadOk { value: u32 },
}

struct GCounter {
    last_read_time: Instant,
    last_read: u32,
    unconfirmed_delta: u32,
}

#[async_trait::async_trait]
impl maelstrom::App for GCounter {
    type Payload = Payload;

    fn new(_node_id: maelstrom::NodeID, _node_ids: Vec<maelstrom::NodeID>) -> Self {
        Self {
            last_read_time: Instant::now(),
            last_read: 0,
            unconfirmed_delta: 0,
        }
    }

    async fn handle(
        &mut self,
        message: maelstrom::Message<Self::Payload>,
        writer: &maelstrom::MessageWriter,
    ) -> Result<(), anyhow::Error> {
        match message.body.payload {
            Payload::Add { delta } => {
                self.unconfirmed_delta += delta;
                writer.reply_to(&message, Payload::AddOk)?;
            }
            Payload::Read => {
                writer.reply_to(
                    &message,
                    Payload::ReadOk {
                        value: self.last_read + self.unconfirmed_delta,
                    },
                )?;
            }
            _ => {
                eprintln!("Ignoring non-relevant payload: {message:?}.");
                return Ok(());
            }
        }
        Ok(())
    }

    async fn tick(&mut self, writer: &maelstrom::MessageWriter) -> anyhow::Result<()> {
        let kv = SeqKV::new(writer);

        if self.unconfirmed_delta > 0 {
            self.last_read = kv.read("counter").await?.unwrap_or_default();
            let swap_succeeded = kv
                .compare_and_swap(
                    "counter",
                    self.last_read,
                    self.last_read + self.unconfirmed_delta,
                )
                .await?;
            if swap_succeeded {
                self.last_read = self.last_read + self.unconfirmed_delta;
                self.last_read_time = Instant::now();
                self.unconfirmed_delta = 0;
            }
        } else if self.last_read_time.elapsed() >= Duration::from_millis(500) {
            self.last_read = kv.read("counter").await?.unwrap_or_default();
            self.last_read_time = Instant::now();
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    maelstrom::event_loop::<GCounter, Payload>().await
}
