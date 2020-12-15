pub mod index;
pub mod util;
pub mod data_item;
pub mod page;
pub mod table;

use data_item::buffer::LRUBuffer;
use std::path::Path;


fn main() {
    // 创建缓冲区
    let _sql_access_project_buffer = LRUBuffer::new(Path::new("SQLAccessProject"), 20);
    let _data_dict_buffer = LRUBuffer::new(Path::new("DataDict"), 20);
    let _data_process_buffer = LRUBuffer::new(Path::new("DataProcess"), 20);
    let _log_buffer = LRUBuffer::new(Path::new("Log"), 20);


    // 初始化完成
    println!("hello world!");
}