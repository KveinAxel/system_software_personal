use crate::error::Error;
use crate::page::{Page, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use crate::buffer::Buffer;

/// 每个 Pager 管理一个文件
pub struct Pager {
    buffer: dyn Buffer,
}

impl Pager {
    pub fn new(buffer: &dyn Buffer) -> Result<Pager, Error> {
        Ok(Pager { buffer })
    }

    /// 读取一个页
    pub fn get_page(&mut self, offset: usize) -> Result<[u8; PAGE_SIZE], Error> {
        self.buffer.get_page(offset)
    }

    /// 向文件写入一个页
    pub fn write_page(&mut self, page: Page, offset: usize) -> Result<(), Error> {
        self.buffer.write_page(offset, page)
    }
}
