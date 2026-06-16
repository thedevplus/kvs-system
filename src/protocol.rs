use crate::kvs::KvCommand;
use serde::{Serialize, Serializer, Deserialize};
use crate::Result;

struct KvStream<'a> {
    command: &'a KvCommand,
    key: &'a String,
    value: &'a Option<String>,
}

impl<'a> KvStream<'a> {
    fn build_from(command: &'a KvCommand, key: &'a String, value: &'a Option<String>) -> Self {
        Self {
            command,
            key,
            value,
        }
    }
}

fn create_protocol_stream<'a>(command: &'a KvCommand, key: &'a String, value: &'a Option<String>) -> Result<Vec<u8>> {
    let kv_stream = KvStream::build_from(command, key, value);
    Ok(serde_json::to_vec(&kv_stream)?)
}

impl<'a> Serialize for KvStream<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        let stream = match self.command {
            KvCommand::Set => {
                let stream = String::from("s");
                stream + self.key + "\t\t" + &self.value.clone().ok_or("Value error").map_err(serde::ser::Error::custom)? + "\r\n"
            },
            KvCommand::Get => {
                let stream = String::from("g");
                stream + self.key + "\r\n"
            },
            KvCommand::Rm => {
                let stream = String::from("r");
                stream + self.key + "\r\n"
            },
        };
        serializer.serialize_bytes(stream.as_bytes())
    }
}
