use crate::error::Error;
use crate::page::{Page, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// 每个 Pager 管理一个文件
pub struct Pager {
    file: File,
}

impl Pager {
    pub fn new(path: &Path) -> Result<Pager, Error> {
        // 创建并打开一个可读可写文件
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        Ok(Pager { file: fd })
    }

    /// 读取一个页
    pub fn get_page(&mut self, offset: usize) -> Result<[u8; PAGE_SIZE], Error> {
        let mut page: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(&mut page)?;
        Ok(page)
    }

    /// 向文件写入一个页
    pub fn write_page(&mut self, page: &Page, offset: &usize) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(*offset as u64))?;
        self.file.write_all(&page.get_data())?;
        Ok(())
    }
}
