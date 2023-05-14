/// A locally-unique ID for the message, e.g. 5.
#[derive(
    Debug,
    PartialEq,
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

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct Message<TPayload> {
    pub src: NodeID,
    #[serde(rename = "dest")]
    pub dst: NodeID,
    pub body: MessageBody<TPayload>,
}

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
pub struct MessageBody<TPayload> {
    pub msg_id: Option<MessageID>,
    pub in_reply_to: Option<MessageID>,

    #[serde(flatten)]
    pub payload: MessagePayload<TPayload>,
}

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(untagged)]
pub enum MessagePayload<TPayload> {
    Shared(SharedMessagePayload),
    App(TPayload),
}

impl<TPayload> From<TPayload> for MessagePayload<TPayload> {
    fn from(value: TPayload) -> Self {
        Self::App(value)
    }
}

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum SharedMessagePayload {
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
        let message: Message<()> = serde_json::from_str(&message_json).expect("works");
        assert_eq!(
            message,
            Message {
                src: "c1".into(),
                dst: "n3".into(),
                body: MessageBody {
                    msg_id: Some(1.into()),
                    in_reply_to: None,
                    payload: MessagePayload::Shared(SharedMessagePayload::Init {
                        node_id: "n3".into(),
                        node_ids: vec!["n1".into(), "n2".into(), "n3".into()]
                    })
                }
            }
        )
    }
}
