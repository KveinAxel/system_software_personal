use std::fs;
use crate::data_item::buffer::{LRUBuffer, Buffer};
use crate::page::pager::Pager;
use crate::util::error::Error;
use crate::index::btree::BTree;
use crate::index::key_value_pair::KeyValuePair;
use std::path::Path;

#[allow(dead_code)]
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

#[allow(dead_code)]
pub fn gen_buffer() -> Result<Box<dyn Buffer>, Error> {
    let mut buffer = Box::new(LRUBuffer::new(4, "metadata.db".to_string())?);
    buffer.add_file(Path::new("test.db"))?;
    buffer.fill_up_to("test.db", 10)?;

    return Ok(buffer);
}

#[allow(dead_code)]
pub fn gen_pager(buffer: &mut Box<dyn Buffer>) -> Result<Box<Pager>, Error> {
    return Ok(Pager::new("test.db".to_string(), 50, buffer)?);
}

#[allow(dead_code)]
pub fn gen_tree(buffer: &mut Box<dyn Buffer>) -> Result<BTree, Error> {
    let pager = gen_pager(buffer)?;
    BTree::new(pager, "test.db".to_string(), buffer)
}

#[allow(dead_code)]
pub fn gen_2_kv() -> Result<(KeyValuePair, KeyValuePair), Error> {
    let value1= 4096usize;
    let value2 = 4096*2usize;
    Ok((KeyValuePair::new("Hello".to_string(), value1), KeyValuePair::new("Test".to_string(), value2)))
}

#[allow(dead_code)]
pub fn gen_kv() -> Result<KeyValuePair, Error> {
    Ok(KeyValuePair::new("Hello".to_string(), 4096usize))
}