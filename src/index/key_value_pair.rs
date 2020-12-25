use std::cmp::Ordering;

#[derive(Eq, PartialOrd, PartialEq)]
pub struct KeyValuePair {
    pub key: String,
    pub value: usize,
}

impl KeyValuePair {
    pub fn new(key: String, value: usize) -> KeyValuePair {
        KeyValuePair { key, value }
    }
}

impl Clone for KeyValuePair {
    fn clone(&self) -> KeyValuePair {
        KeyValuePair {
            key: self.key.clone(),
            value: self.value.clone(),
        }
    }
}

impl Ord for KeyValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.key.cmp(&other.key);
    }
}
