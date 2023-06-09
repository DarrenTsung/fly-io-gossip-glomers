use anyhow::Context;
use serde::de::DeserializeOwned;

/// A locally-unique ID for the message, e.g. 5.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Copy,
    shrinkwraprs::Shrinkwrap,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
#[shrinkwrap(mutable)]
pub struct MessageID(pub u32);

impl From<u32> for MessageID {
    fn from(value: u32) -> Self {
        MessageID(value)
    }
}

/// The ID of a node, e.g. "n1".
#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Clone,
    shrinkwraprs::Shrinkwrap,
    serde_derive::Serialize,
    serde_derive::Deserialize,
)]
pub struct NodeID(String);

impl<T: Into<String>> From<T> for NodeID {
    fn from(value: T) -> Self {
        NodeID(value.into())
    }
}

impl NodeID {
    /// "Nodes n1, n2, n3, etc. are instances of the binary you pass to Maelstrom. These
    /// nodes implement whatever distributed algorithm you're trying to build: for
    /// instance, a key-value store. You can think of these as servers, in that they
    /// accept requests from clients and send back responses."
    ///
    /// https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#nodes-and-networks
    pub fn is_server(&self) -> bool {
        self.starts_with("n")
    }

    /// "Nodes c1, c2, c3, etc. are Maelstrom's internal clients. Clients send requests to
    /// servers and expect responses back, via a simple asynchronous RPC protocol."
    ///
    /// https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#nodes-and-networks
    pub fn is_client(&self) -> bool {
        self.starts_with("c")
    }
}

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct Message<TPayload> {
    pub src: NodeID,
    #[serde(rename = "dest")]
    pub dst: NodeID,
    pub body: MessageBody<TPayload>,
}

impl Message<serde_json::Value> {
    pub fn into_payload<TOtherPayload: DeserializeOwned>(
        self,
    ) -> anyhow::Result<Message<TOtherPayload>> {
        Ok(Message {
            src: self.src,
            dst: self.dst,
            body: self.body.into_payload()?,
        })
    }
}

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct MessageBody<TPayload> {
    pub msg_id: Option<MessageID>,
    pub in_reply_to: Option<MessageID>,

    #[serde(flatten)]
    pub payload: TPayload,
}

impl MessageBody<serde_json::Value> {
    pub fn into_payload<TOtherPayload: DeserializeOwned>(
        self,
    ) -> anyhow::Result<MessageBody<TOtherPayload>> {
        Ok(MessageBody {
            msg_id: self.msg_id,
            in_reply_to: self.in_reply_to,
            payload: serde_json::from_value::<TOtherPayload>(self.payload)
                .context("Couldn't convert payload type!")?,
        })
    }
}

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    Init {
        node_id: NodeID,
        node_ids: Vec<NodeID>,
    },
    InitOk,
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn it_works_with_init_example() {
        let message_json = indoc!(
            r##"
			{
				"src": "c1",
				"dest": "n3",
				"body": {
					"type":     "init",
					"msg_id":   1,
					"node_id":  "n3",
					"node_ids": ["n1", "n2", "n3"]
				}
			}
		"##
        );
        let message: Message<InitPayload> = serde_json::from_str(&message_json).expect("works");
        assert_eq!(
            message,
            Message {
                src: "c1".into(),
                dst: "n3".into(),
                body: MessageBody {
                    msg_id: Some(1.into()),
                    in_reply_to: None,
                    payload: InitPayload::Init {
                        node_id: "n3".into(),
                        node_ids: vec!["n1".into(), "n2".into(), "n3".into()]
                    }
                }
            }
        )
    }
}
