use crate::table::field::{FieldValue};

pub struct Entry {
    pub(crate) data: Vec<FieldValue>
}

impl Entry {

    pub fn to_bytes(&self) -> Vec<u8>{
        let mut raw_bytes = Vec::<u8>::new();
        for item in &self.data {
            raw_bytes = [raw_bytes, item.clone().into()].concat();
        }
        raw_bytes
    }
}