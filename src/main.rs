use crate::buffer::LRUBuffer;
use std::path::Path;

pub mod btree;
pub mod error;
pub mod key_value_pair;
pub mod node;
pub mod page;
pub mod pager;
pub mod buffer;
pub mod table_manager;
pub mod table;
pub mod field;
pub mod booter;

fn main() {
    // 创建缓冲区
    let _sql_access_project_buffer = LRUBuffer::new(Path::new("SQLAccessProject"), 20);
    let _data_dict_buffer = LRUBuffer::new(Path::new("DataDict"), 20);
    let _data_process_buffer = LRUBuffer::new(Path::new("DataProcess"), 20);
    let _log_buffer = LRUBuffer::new(Path::new("Log"), 20);


    // 初始化完成
    println!("hello world!");
}