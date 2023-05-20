use maelstrom::{MessageID, NodeID};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    time::{Duration, Instant},
};

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast {
        message: u32,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<u32>,
    },
    Topology {
        topology: HashMap<NodeID, Vec<NodeID>>,
    },
    TopologyOk,
}

struct AckContext {
    message_id: MessageID,
    time_sent: Instant,
}

struct Broadcast {
    messages_seen: HashSet<u32>,
    neighbor_messages_not_acked: HashMap<NodeID, HashMap<u32, AckContext>>,
    neighbors: Vec<NodeID>,
    /// Determines whether to always broadcast to neighbors or only when
    /// receiving a message from a client.
    always_broadcast: bool,
}

impl Broadcast {
    fn send_to_neighbor(
        &mut self,
        writer: &mut maelstrom::MessageWriter,
        neighbor: &NodeID,
        message: u32,
    ) -> anyhow::Result<()> {
        let message_id = writer.send_to(neighbor, BroadcastPayload::Broadcast { message })?;
        match self
            .neighbor_messages_not_acked
            .entry(neighbor.clone())
            .or_insert_with(HashMap::new)
            .entry(message)
        {
            Entry::Occupied(mut entry) => {
                entry.get_mut().time_sent = Instant::now();
                entry.get_mut().message_id = message_id;
            }
            Entry::Vacant(entry) => {
                entry.insert(AckContext {
                    time_sent: Instant::now(),
                    message_id,
                });
            }
        }

        Ok(())
    }
}

impl maelstrom::App for Broadcast {
    type Payload = BroadcastPayload;

    fn new(node_id: maelstrom::NodeID, node_ids: Vec<maelstrom::NodeID>) -> Self {
        let chunks = node_ids.chunks(node_ids.len() / 5).collect::<Vec<_>>();
        let Some(chunk_index) = node_ids.chunks(node_ids.len() / 5).position(|c| c.contains(&node_id)) else {
            panic!("Expected node_id ({node_id:?}) to be in list of node_ids ({node_ids:?})!");
        };
        let chunk = &chunks[chunk_index];

        let index_in_chunk = chunk.iter().position(|n| n == &node_id).expect("exists");
        let (neighbors, always_broadcast) = if index_in_chunk == 0 {
            let next_chunk = &chunks[(chunk_index + 1) % chunks.len()];
            (next_chunk, true)
        } else {
            (chunk, false)
        };

        Self {
            messages_seen: HashSet::new(),
            neighbor_messages_not_acked: HashMap::new(),
            // Don't want to include self in neighbors.
            neighbors: neighbors
                .iter()
                .filter(|n| *n != &node_id)
                .cloned()
                .collect(),
            always_broadcast,
        }
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
            BroadcastPayload::Broadcast {
                message: message_to_broadcast,
            } => {
                let inserted = self.messages_seen.insert(*message_to_broadcast);
                writer.reply_to(&message, BroadcastPayload::BroadcastOk)?;
                // Broadcast to neighbors if this was newly seen. Servers only broadcast
                // messages received from clients (unless they are configured to always
                // broadcast, i.e. when they are a link to the next chunk of servers).
                if inserted && (message.src.is_client() || self.always_broadcast) {
                    for neighbor in self.neighbors.clone() {
                        self.send_to_neighbor(writer, &neighbor, *message_to_broadcast)?;
                    }
                }
            }
            BroadcastPayload::BroadcastOk => {
                let neighbor_messages = self
                    .neighbor_messages_not_acked
                    .entry(message.src)
                    .or_insert_with(HashMap::new);
                let mut message_found = None;
                for (message_key, ack_context) in neighbor_messages.iter_mut() {
                    if message.body.in_reply_to == Some(ack_context.message_id) {
                        message_found = Some(*message_key);
                        break;
                    }
                }
                if let Some(message_found) = message_found {
                    neighbor_messages.remove(&message_found);
                }
            }
            BroadcastPayload::ReadOk { messages } => {
                let neighbor_messages = self
                    .neighbor_messages_not_acked
                    .entry(message.src)
                    .or_insert_with(HashMap::new);
                for message in messages {
                    neighbor_messages.remove(message);
                }
            }
            BroadcastPayload::Read => {
                writer.reply_to(
                    &message,
                    BroadcastPayload::ReadOk {
                        messages: self.messages_seen.iter().copied().collect(),
                    },
                )?;
            }
            BroadcastPayload::Topology { topology: _ } => {
                // Ignore topology, we constructed our own topology at initialization.
                writer.reply_to(&message, BroadcastPayload::TopologyOk)?;
            }
            _ => {
                eprintln!("Ignoring non-relevant payload: {app_payload:?}.");
                return Ok(());
            }
        }

        Ok(())
    }

    fn tick<'a>(&mut self, writer: &mut maelstrom::MessageWriter<'a>) -> anyhow::Result<()> {
        let mut resend = vec![];
        for (neighbor, messages_not_acked) in &self.neighbor_messages_not_acked {
            for (message, ack_context) in messages_not_acked {
                if ack_context.time_sent.elapsed() >= Duration::from_millis(500) {
                    resend.push((neighbor.clone(), *message));
                }
            }
        }
        for (neighbor, message) in resend {
            self.send_to_neighbor(writer, &neighbor, message)?;
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    maelstrom::event_loop::<Broadcast, BroadcastPayload>()
}
