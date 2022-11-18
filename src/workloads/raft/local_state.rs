use std::collections::HashMap;

use crate::protocol::{raft::*, ErrorCode, ErrorData};

pub struct LocalState {
    map: HashMap<Key, Value>,
}

impl LocalState {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn read(&self, data: &ReadData) -> Result<ReadOkData, ErrorData> {
        match self.map.get(&data.key) {
            Some(value) => Ok(ReadOkData {
                value: value.clone(),
            }),
            None => Err(key_does_not_exist_error()),
        }
    }

    pub fn write(&mut self, data: &WriteData) {
        self.map.insert(data.key.clone(), data.value.clone());
    }

    pub fn cas(&mut self, data: &CasData) -> Result<(), ErrorData> {
        let cur = self
            .map
            .get_mut(&data.key)
            .ok_or_else(|| key_does_not_exist_error())?;
        if cur == &data.from {
            *cur = data.to.clone();
            Ok(())
        } else {
            Err(ErrorData::new(
                "CAS from value doesn't match the current value".to_owned(),
                ErrorCode::PreconditionFailed,
            ))
        }
    }
}

fn key_does_not_exist_error() -> ErrorData {
    ErrorData::new("Key does not exist".to_owned(), ErrorCode::KeyDoesNotExist)
}
