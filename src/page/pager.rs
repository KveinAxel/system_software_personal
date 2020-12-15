use crate::data_item::buffer::Buffer;
use crate::page::page_item::{Page, PAGE_SIZE};
use crate::util::error::Error;

/// 每个 Pager 管理一个文件
pub struct Pager {
    buffer: Box<dyn Buffer>,
}

impl Pager {
    pub fn new(buffer: Box<dyn Buffer>) -> Result<Box<Pager>, Error> {
        Ok(Box::new(Pager { buffer }))
    }

    pub fn fill_up_to(&mut self, size: usize) -> Result<(), Error> {
        self.buffer.fill_up_to(size)
    }

    /// 读取一个页
    pub fn get_page(&mut self, offset: &usize) -> Result<[u8; PAGE_SIZE], Error> {
        self.buffer.get_page(*offset)
    }

    /// 向文件写入一个页
    pub fn write_page(&mut self, page: Page, offset: &usize) -> Result<(), Error> {
        self.buffer.write_page(*offset, page)
    }
}

