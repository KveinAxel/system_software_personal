use std::cmp::Ordering;

#[derive(Eq, PartialEq)]
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
            value: self.value,
        }
    }
}

impl PartialOrd for KeyValuePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
