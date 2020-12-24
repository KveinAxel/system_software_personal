use crate::data_item::buffer::Buffer;
use crate::page::page_item::{Page};
use crate::util::error::Error;
use uuid::Uuid;

/// 每个 Pager 管理一个文件
pub struct Pager {
    buffer: Box<dyn Buffer>,
    cnt: usize,
    max_size: usize,
    file_name: String
}

impl Pager {
    pub fn new(file_name: String, buffer: Box<dyn Buffer>, max_size: usize) -> Result<Box<Pager>, Error> {
        let mut pager = Box::new(
            Pager {
                buffer,
                cnt: 0,
                max_size,
                file_name
            }
        );
        pager.fill_up_to(&max_size)?;
        Ok(pager)
    }

    /// 将文件大小扩充到指定页数
    pub fn fill_up_to(&mut self, num_of_page: &usize) -> Result<(), Error> {
        self.buffer.fill_up_to(self.file_name.as_str(), *num_of_page)
    }

    /// 读取一个页
    pub fn get_page(&mut self, page_num: &usize) -> Result<Page, Error> {
        self.buffer.get_page(self.file_name.as_str(), *page_num)
    }

    /// 向文件写入一个页
    pub fn write_page(&mut self, page: Page) -> Result<(), Error> {
        self.buffer.write_page(page)
    }

    pub fn get_first_uuid(&mut self) -> Result<Uuid, Error> {
        self.buffer.get_first_uuid()
    }

    pub fn update_first_uuid(&mut self, uuid: Uuid) -> Result<(), Error> {
        self.buffer.update_first_uuid(uuid)
    }

    pub fn get_new_page(&mut self) -> Result<Page, Error> {
        // 如果文件大小不够，则扩大文件
        if self.cnt >= self.max_size {
            self.fill_up_to(&(2 * self.max_size))?;
        }
        self.cnt += 1;
        self.get_page(&self.cnt.clone())
    }
}

#[cfg(test)]
mod test {
    use crate::util::error::Error;
    use crate::data_item::buffer::{LRUBuffer, Buffer};
    use std::fs;
    use std::path::Path;
    use crate::page::pager::Pager;

    #[test]
    fn test_get_new_page() -> Result<(), Error> {
        match fs::remove_file("metadata.db") {
            Ok(_) => (),
            Err(_) => (),
        };
        match fs::remove_file("test.db") {
            Ok(_) => (),
            Err(_) => (),
        };

        let mut buffer = Box::new(LRUBuffer::new(4, "metadata.db".to_string())?);
        buffer.add_file(Path::new("test.db"))?;
        buffer.fill_up_to("test.db", 10)?;

        let mut pager = Pager::new("test.db".to_string(), buffer, 50)?;
        assert_eq!(pager.cnt, 0);
        pager.get_new_page();
        assert_eq!(pager.cnt, 1);
        pager.get_new_page();
        assert_eq!(pager.cnt, 2);

        match fs::remove_file("metadata.db") {
            Ok(_) => (),
            Err(_) => (),
        };
        match fs::remove_file("test.db") {
            Ok(_) => (),
            Err(_) => (),
        };
        Ok(())
    }

}
