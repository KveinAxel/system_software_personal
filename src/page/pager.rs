use crate::data_item::buffer::Buffer;
use crate::page::page::{Page, PAGE_SIZE};
use crate::util::error::Error;

/// 每个 Pager 管理一个文件
pub struct Pager {
    pub(crate) cnt: usize,
    max_size: usize,
    file_name: String,
    remain_size: Vec<(usize, usize)>
}

impl Clone for Pager {
    fn clone(&self) -> Self {
        Self {
            cnt: self.cnt,
            max_size: self.max_size,
            file_name: self.file_name.clone(),
            remain_size: self.remain_size.clone(),
        }
    }
}

impl Pager {
    pub fn new(file_name: String, max_size: usize, buffer: &mut Box<dyn Buffer>) -> Result<Box<Pager>, Error> {
        let mut vec = Vec::<(usize, usize)>::new();
        vec.push((0, 0));
        let mut pager = Box::new(
            Pager {
                cnt: 0,
                max_size,
                file_name,
                remain_size: vec,
            }
        );
        pager.fill_up_to(&max_size, buffer)?;
        Ok(pager)
    }

    /// 将文件大小扩充到指定页数
    pub fn fill_up_to(&mut self, num_of_page: &usize, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        self.max_size = *num_of_page;
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
        self.remain_size.push((PAGE_SIZE, 0));
        self.get_page(&self.cnt.clone(), buffer)
    }

    pub fn insert_value(&mut self, bytes: &[u8], buffer: &mut Box<dyn Buffer>) -> Result<usize, Error> {
        let len = bytes.len();
        for (i, (siz, offset)) in self.remain_size.clone().iter().enumerate() {
            if i == 0 {
                continue;
            }
            if *siz > len {
                let mut page = self.get_page(&i, buffer)?;
                page.write_bytes_at_offset(bytes, *offset, len)?;
                self.write_page(page, buffer)?;

                let new_siz = *siz - len;
                let new_offset = *offset + len;
                self.remain_size[i] = (new_siz, new_offset);
                return Ok(*offset + (i - 1) * PAGE_SIZE)
            }
        }

        let mut page = self.get_new_page(buffer)?;
        page.write_bytes_at_offset(bytes, 0, len)?;
        self.write_page(page, buffer)?;
        self.remain_size[self.cnt] = (PAGE_SIZE - len, len);
        Ok((self.cnt - 1) * PAGE_SIZE)
    }

    pub fn get_value(&self, offset:usize, size: usize, buffer: &mut Box<dyn Buffer>) -> Result<Vec<u8>, Error> {
        let page_num = offset / PAGE_SIZE + 1;
        let page_offset = offset % PAGE_SIZE;

        let page = self.get_page(&page_num, buffer)?;
        Ok(page.get_ptr_from_offset(page_offset, size).to_vec())
    }
}
