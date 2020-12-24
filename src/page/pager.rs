use crate::data_item::buffer::Buffer;
use crate::page::page_item::{Page};
use crate::util::error::Error;
use uuid::Uuid;

/// 每个 Pager 管理一个文件
pub struct Pager {
    cnt: usize,
    max_size: usize,
    file_name: String
}

impl Pager {
    pub fn new(file_name: String, max_size: usize, mut buffer: &Box<dyn Buffer>) -> Result<Box<Pager>, Error> {
        let mut pager = Box::new(
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
    pub fn fill_up_to(&mut self, num_of_page: &usize, mut buffer: &Box<dyn Buffer>) -> Result<(), Error> {
        buffer.fill_up_to(self.file_name.as_str(), *num_of_page)
    }

    /// 读取一个页
    pub fn get_page(&mut self, page_num: &usize, mut buffer: &Box<dyn Buffer>) -> Result<Page, Error> {
        buffer.get_page(self.file_name.as_str(), *page_num)
    }

    /// 向文件写入一个页
    pub fn write_page(&mut self, page: Page, mut buffer: &Box<dyn Buffer>) -> Result<(), Error> {
        buffer.write_page(page)
    }

    pub fn get_first_uuid(&mut self, mut buffer: &Box<dyn Buffer>) -> Result<Uuid, Error> {
        buffer.get_first_uuid()
    }

    pub fn update_first_uuid(&mut self, uuid: Uuid, mut buffer: &Box<dyn Buffer>) -> Result<(), Error> {
        buffer.update_first_uuid(uuid)
    }

    pub fn get_new_page(&mut self, mut buffer: &Box<dyn Buffer>) -> Result<Page, Error> {
        // 如果文件大小不够，则扩大文件
        if self.cnt >= self.max_size {
            self.fill_up_to(&(2 * self.max_size), buffer)?;
        }
        self.cnt += 1;
        self.get_page(&self.cnt.clone(), buffer)
    }
}

#[cfg(test)]
mod test {
    use crate::util::error::Error;
    use std::path::Path;
    use crate::page::pager::Pager;
    use crate::util::test_lib::{rm_test_file, gen_buffer};

    #[test]
    fn test_get_new_pager() -> Result<(), Error> {
        rm_test_file();

        let mut buffer = gen_buffer()?;
        buffer.add_file(Path::new("test.db"))?;
        buffer.fill_up_to("test.db", 10)?;

        let mut pager = Pager::new("test.db".to_string(), 50, &buffer)?;
        assert_eq!(pager.cnt, 0);
        pager.get_new_page(&buffer);
        assert_eq!(pager.cnt, 1);
        pager.get_new_page(&buffer);
        assert_eq!(pager.cnt, 2);

        rm_test_file();
        Ok(())
    }

}
