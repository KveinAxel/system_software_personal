use crate::data_item::buffer::Buffer;
use crate::page::page_item::{Page, PAGE_SIZE};
use crate::util::error::Error;
use uuid::Uuid;

/// 每个 Pager 管理一个文件
pub struct Pager {
    buffer: Box<dyn Buffer>,
}

impl Pager {
    pub fn new(buffer: Box<dyn Buffer>) -> Result<Box<Pager>, Error> {
        Ok(Box::new(Pager { buffer }))
    }

    pub fn fill_up_to(&mut self, num_of_page: &usize) -> Result<(), Error> {
        self.buffer.fill_up_to(*num_of_page)
    }

    /// 读取一个页
    pub fn get_page(&mut self, page_num: &usize) -> Result<Page, Error> {
        self.buffer.get_page(*page_num)
    }

    /// 向文件写入一个页
    pub fn write_page(&mut self, page: Page) -> Result<(), Error> {
        self.buffer.write_page(page)
    }

    pub fn get_first_uuid(&self) -> Result<Uuid, Error> {
        self.buffer.get_first_uuid()
    }

    pub fn update_first_uuid(&mut self, uuid: Uuid) -> Result<(), Error> {
        self.buffer.update_first_uuid(uuid)
    }
}

