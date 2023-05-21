use crate::MessageWriter;
use serde::{de::DeserializeOwned, *};

pub struct SeqKV<'a> {
    message_writer: &'a MessageWriter,
}

impl<'a> SeqKV<'a> {
    const SEQ_KV_NODE_ID: &str = "seq-kv";

    pub fn new(message_writer: &'a MessageWriter) -> Self {
        Self { message_writer }
    }

    pub async fn read<K: Serialize, V: Serialize + DeserializeOwned>(
        &self,
        key: K,
    ) -> anyhow::Result<Option<V>> {
        let response = self
            .message_writer
            .send_and_receive::<_, KVPayload<(), V>>(
                &Self::SEQ_KV_NODE_ID.into(),
                KVPayload::<K, ()>::Read { key },
            )
            .await?;
        Ok(match response.body.payload {
            // key-does-not-exist
            KVPayload::Error { code, text: _ } if code == 20 => None,
            KVPayload::ReadOk { value } => Some(value),
            _ => anyhow::bail!("Expected ReadOk in response to Read."),
        })
    }

    pub async fn write<K: Serialize, V: Serialize>(&self, key: K, value: V) -> anyhow::Result<()> {
        let response = self
            .message_writer
            .send_and_receive::<KVPayload<K, V>, KVPayload<(), ()>>(
                &Self::SEQ_KV_NODE_ID.into(),
                KVPayload::Write { key, value },
            )
            .await?;
        let KVPayload::WriteOk = response.body.payload else {
            anyhow::bail!("Expected WriteOk in response to Write.");
        };
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum KVPayload<K, V> {
    Read {
        key: K,
    },
    ReadOk {
        value: V,
    },
    Write {
        key: K,
        value: V,
    },
    WriteOk,
    #[serde(rename = "cas")]
    CompareAndSet {
        key: K,
        from: V,
        to: V,
        create_if_not_exists: Option<bool>,
    },
    #[serde(rename = "cas_ok")]
    CompareAndSetOk,
    Error {
        code: u32,
        text: String,
    },
}
