use std::fs;
use crate::data_item::buffer::{LRUBuffer, Buffer};
use crate::page::pager::Pager;
use std::path::Path;
use crate::util::error::Error;
use crate::index::btree::BTree;
use crate::index::key_value_pair::KeyValuePair;

pub fn rm_test_file() {
    match fs::remove_file("metadata.db") {
        Ok(_) => (),
        Err(_) => (),
    };
    match fs::remove_file("test.db") {
        Ok(_) => (),
        Err(_) => (),
    };
}

pub fn gen_buffer() -> Result<Box<dyn Buffer>, Error> {
    return Ok(Box::new(LRUBuffer::new(4, "metadata.db".to_string())?));
}

pub fn gen_pager() -> Result<Box<Pager>, Error> {
    let mut buffer = Box::new(LRUBuffer::new(4, "metadata.db".to_string())?);
    buffer.add_file(Path::new("test.db"))?;
    buffer.fill_up_to("test.db", 10)?;

    return Ok(Pager::new("test.db".to_string(), buffer, 50)?);
}

pub fn gen_tree() -> Result<BTree, Error> {
    let pager = gen_pager()?;
    BTree::new(*pager, "test.db".to_string())
}

pub fn gen_2_kv() -> Result<(KeyValuePair, KeyValuePair), Error> {
    Ok((KeyValuePair::new("Hello".to_string(), "World".to_string()), KeyValuePair::new("Test".to_string(), "BTree".to_string())))
}

pub fn gen_kv () -> Result<KeyValuePair, Error> {
    Ok(KeyValuePair::new("Hello".to_string(), "World".to_string()))
}