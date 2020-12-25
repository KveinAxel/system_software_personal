use crate::data_item::buffer::Buffer;
use crate::page::page_item::{Page};
use crate::util::error::Error;

/// 每个 Pager 管理一个文件
pub struct Pager {
    pub(crate) cnt: usize,
    max_size: usize,
    file_name: String
}

impl Clone for Pager {
    fn clone(&self) -> Self {
        Self {
            cnt: self.cnt,
            max_size: self.max_size,
            file_name: self.file_name.clone()
        }
    }
}

impl Pager {
    pub fn new(file_name: String, max_size: usize, buffer: &mut Box<dyn Buffer>) -> Result<Box<Pager>, Error> {
        let pager = Box::new(
            Pager {
                cnt: 0,
                max_size,
                file_name
            }
        );
        pager.fill_up_to(&max_size, buffer)?;
        Ok(pager)
    }

    /// 将文件大小扩充到指定页数
    pub fn fill_up_to(&self, num_of_page: &usize, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        buffer.fill_up_to(self.file_name.as_str(), *num_of_page)
    }

    /// 读取一个页
    pub fn get_page(&self, page_num: &usize, buffer: &mut Box<dyn Buffer>) -> Result<Page, Error> {
        buffer.get_page(self.file_name.as_str(), *page_num)
    }

    /// 向文件写入一个页
    pub fn write_page(&self, page: Page, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        buffer.write_page(page)
    }

    pub fn get_new_page(&mut self, buffer: &mut Box<dyn Buffer>) -> Result<Page, Error> {
        // 如果文件大小不够，则扩大文件
        if self.cnt >= self.max_size {
            self.fill_up_to(&(2 * self.max_size), buffer)?;
        }
        self.cnt += 1;
        self.get_page(&self.cnt.clone(), buffer)
    }

    pub fn insert_value(&mut self, bytes: &[u8], buffer: &mut Box<dyn Buffer>) -> Result<usize, Error> {
        // todo
        unimplemented!()
    }

    pub fn get_value(&self, offset:usize, size: usize, buffer: &mut Box<dyn Buffer>) -> Result<Vec<u8>, Error> {
        // todo
        unimplemented!()
    }
}
