use crate::Result;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::process;

#[derive(Clone, Copy, Debug)]
pub enum StreamCommand {
    St,
    Gt,
    Rm,
    Se,
    Ge,
    Gn,
    Re,
}

#[derive(Clone, Debug)]
pub struct KvStream {
    pub command: StreamCommand,
    pub key: String,
    pub value: Option<String>,
}

impl KvStream {
    pub fn build_from(command: StreamCommand, key: String, value: Option<String>) -> Self {
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
            StreamCommand::St => {
                let stream = String::from("st");
                stream
                    + self.key.as_ref()
                    + "\t\t"
                    + self
                        .value
                        .as_ref()
                        .ok_or("Value error")
                        .map_err(serde::ser::Error::custom)?
            }
            other => {
                let mut stream = String::from("");
                if let StreamCommand::Gt = other {
                    stream += "gt";
                } else if let StreamCommand::Rm = other {
                    stream += "rm";
                } else if let StreamCommand::Se = other {
                    stream += "se";
                } else if let StreamCommand::Ge = other {
                    stream += "ge";
                } else if let StreamCommand::Gn = other {
                    stream += "gn";
                } else {
                    stream += "re";
                }
                stream + self.key.as_ref()
            }
        };
        serializer.serialize_str(&stream)
    }
}

pub fn parse_protocol_stream(kv_stream: &[u8]) -> Result<KvStream> {
    Ok(serde_json::from_slice(kv_stream)?)
}

struct DeStream {}

impl<'de> Visitor<'de> for DeStream {
    type Value = KvStream;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "Need a vector of u8 stream")
    }
    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let mut iter = v.split("\t\t");
        let mut kv_stream = KvStream::build_from(StreamCommand::Gt, String::from(""), None);
        if let Some(value) = iter.next()
            && let cmd @ ("st" | "gt" | "rm" | "se" | "ge" | "gn" | "re") = &value[..2]
        {
            if cmd == "gt" {
                kv_stream.command = StreamCommand::Gt;
            } else if cmd == "rm" {
                kv_stream.command = StreamCommand::Rm;
            } else if cmd == "se" {
                kv_stream.command = StreamCommand::Se;
            } else if cmd == "ge" {
                kv_stream.command = StreamCommand::Ge;
            } else if cmd == "gn" {
                kv_stream.command = StreamCommand::Gn;
            } else if cmd == "re" {
                kv_stream.command = StreamCommand::Re;
            } else {
                kv_stream.command = StreamCommand::St;
                if let Some(value) = iter.next() {
                    kv_stream.value = Some(value.to_string());
                } else {
                    process::exit(1);
                }
            }
            kv_stream.key = value[2..].to_string();
        }
        Ok(kv_stream)
    }
}

impl<'de> Deserialize<'de> for KvStream {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(DeStream {})
    }
}
