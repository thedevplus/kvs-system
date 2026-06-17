use crate::Result;
use crate::kvs::KvCommand;
use log::debug;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};
use std::process;

pub struct KvStream {
    pub command: KvCommand,
    pub key: String,
    pub value: Option<String>,
}

impl KvStream {
    pub fn build_from(command: KvCommand, key: String, value: Option<String>) -> Self {
        Self {
            command,
            key,
            value,
        }
    }
}

pub fn create_protocol_stream(kv_stream: &KvStream) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(kv_stream)?)
}

impl Serialize for KvStream {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let stream = match self.command {
            KvCommand::Set => {
                let stream = String::from("s");
                stream
                    + self.key.as_ref()
                    + "\t\t"
                    + self
                        .value
                        .as_ref()
                        .ok_or("Value error")
                        .map_err(serde::ser::Error::custom)?
            }
            KvCommand::Get => {
                let stream = String::from("g");
                stream + self.key.as_ref()
            }
            KvCommand::Rm => {
                let stream = String::from("r");
                stream + self.key.as_ref()
            }
        };
        serializer.serialize_bytes(stream.as_bytes())
    }
}

pub fn parse_protocol_stream(kv_stream: &[u8]) -> Result<KvStream> {
    debug!("Inside parse function");
    Ok(serde_json::from_slice(kv_stream)?)
}

struct DeStream {}

impl<'de> Visitor<'de> for DeStream {
    type Value = KvStream;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "Need a vector of u8 stream")
    }
    fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        debug!("Inside visitor parse before");
        let stream = String::from_utf8_lossy(v).to_string();
        debug!("{stream}");
        let mut iter = stream.split("\t\t");
        let mut kv_stream = KvStream::build_from(KvCommand::Get, String::from(""), None);
        if let Some(value) = iter.next() {
            match &value[0..1] {
                cmd @ ("s" | "g" | "r") => {
                    if cmd == "g" {
                        kv_stream.command = KvCommand::Get;
                    } else if cmd == "r" {
                        kv_stream.command = KvCommand::Rm;
                    } else {
                        kv_stream.command = KvCommand::Set;
                        if let Some(value) = iter.next() {
                            kv_stream.value = Some(value.to_string());
                        } else {
                            process::exit(1);
                        }
                    }
                    if !value[1..].is_empty() {
                        kv_stream.key = value[1..].to_string();
                    } else {
                        process::exit(1);
                    }
                }
                _ => process::exit(1),
            }
        }
        debug!("Inside visitor parse after");
        Ok(kv_stream)
    }
}

impl<'de> Deserialize<'de> for KvStream {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        debug!("Inside deserialize parse");
        deserializer.deserialize_bytes(DeStream {})
    }
}
