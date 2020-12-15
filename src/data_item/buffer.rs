use std::collections::{HashMap, LinkedList};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;
use std::time::SystemTime;

use uuid::Uuid;

use crate::page::page_item::{Page, PAGE_SIZE};
use crate::util::error::Error;
use byteorder::{WriteBytesExt, ReadBytesExt};

/// 缓冲区自己管理的配置页的索引
pub const META_PAGE: usize = 0;

/// 保留的非数据页数(包括META_PAGE)
pub const NON_DATA_PAGE: usize = 4;

/// 全局配置文件的页数
pub const METADATA_FILE_PAGE_NUM: usize = 4;
pub const FIRST_UUID_OFFSET: usize = 0;

/// 初始化文件的页大小
pub const INIT_FILE_PAGE_NUM: usize = 4;

/// 文件页数所在页
pub const FILE_PAGE_NUM_PAGE_NUM: usize = 0;
/// 文件页数所在页的偏移
pub const FILE_PAGE_NUM_OFFSET: usize = 0;

/// 文件页表描述方式： 每次一个usize表示剩余空间
/// 文件页表所在页
pub const FILE_PAGE_TABLE_PAGE_NUM: usize = 0;
/// 文件页表偏移
pub const FILE_PAGE_TABLE_OFFSET: usize = size_of::<usize>();

pub struct Position {
    file_name: String,
    page_num: usize,
    offset: usize,
}

/// 缓冲区的trait，实现了通过缓冲区获取页、写入页、强制刷新页
pub trait Buffer {
    fn add_file(&mut self, path: &Path) -> Result<(), Error>;

    fn fill_up_to(&mut self, file_name: &str, num_of_page: usize) -> Result<(), Error>;

    fn get_page(&mut self, file_name: &str, page_num: usize) -> Result<Page, Error>;

    fn write_page(&mut self, page: Page) -> Result<(), Error>;

    fn flush(&mut self, file_name: &str, page_num: &usize) -> Result<(), Error>;

    fn get_first_uuid(&mut self) -> Result<Uuid, Error>;

    fn update_first_uuid(&mut self, uuid: Uuid) -> Result<(), Error>;

    fn insert_bytes(&mut self, file_name: &str, bytes: &[u8]) -> Result<Position, Error>;

    fn read_bytes(&mut self, pos: Position, size: usize) -> Result<&[u8], Error>;
}


/// LRU算法实现的Buffer
pub struct LRUBuffer {
    list: LinkedList<LRUBufferItem>,
    len: usize,
    buff_size: usize,
    file: HashMap<String, File>,
}

/// LRUBuffer中的每一项
struct LRUBufferItem {
    page: Page,
    time: SystemTime,
}

impl LRUBuffer {
    /// LRUBuffer的构造方法
    pub fn new(buff_size: usize) -> Result<LRUBuffer, Error> {
        let path = Path::new("metadata.db");
        let mut hashmap = HashMap::<String, File>::new();
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path);
        match fd {
            Ok(file) => {
                hashmap.insert(String::from("metadata.db"), file);
            }
            Err(_) => {
                let new_metadata = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(path)?;
                hashmap.insert(String::from("metadata.db"), new_metadata);
            }
        }
        let mut res = LRUBuffer {
            list: LinkedList::<LRUBufferItem>::new(),
            len: 0,
            buff_size,
            file: hashmap,
        };
        res.fill_up_to("metadata.db", METADATA_FILE_PAGE_NUM)?;
        Ok(res)
    }
}

impl Buffer for LRUBuffer {
    fn add_file(&mut self, path: &Path) -> Result<(), Error> {
        // 创建文件
        let mut fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        // 初始化文件大小
        fd.seek(SeekFrom::Start(0))?;
        fd.write_all(Vec::<u8>::with_capacity(INIT_FILE_PAGE_NUM * PAGE_SIZE).as_slice())?;

        // 填充文件头配置信息
        // 文件页数
        fd.seek(SeekFrom::Start(0));
        fd.write_u32(INIT_FILE_PAGE_NUM as u32);

        // 文件页表
        fd.write_u32(PAGE_SIZE as u32 - (32 * NON_DATA_PAGE + 32) as u32);
        fd.write_u32(PAGE_SIZE as u32);
        fd.write_u32(PAGE_SIZE as u32);
        fd.write_u32(PAGE_SIZE as u32);

        // 获取文件名
        let raw_file_name = path.to_str();
        let file_name = match raw_file_name {
            Some(file_name) => file_name,
            None => return Err(Error::FileNotFound)
        };

        // 文件保存在哈希表中
        self.file.insert(String::from(file_name), fd);
        Ok(())
    }

    /// 向文件填充占位符至指定页数
    /// todo 只填充不覆盖
    fn fill_up_to(&mut self, file_name: &str, num_of_page: usize) -> Result<(), Error> {
        // 查询文件fd
        let raw_file = self.file.get_mut(file_name);
        match raw_file {
            Some(file) => {
                if PAGE_SIZE - (INIT_FILE_PAGE_NUM + num_of_page + 1) * 32 < 0 {
                    return Err(Error::PageNumOutOfSize)
                }
                // 填充文件
                file.seek(SeekFrom::Start((NON_DATA_PAGE * PAGE_SIZE) as u64))?;
                file.write_all(Vec::<u8>::with_capacity(num_of_page * PAGE_SIZE).as_slice())?;

                // 更新文件头
                file.seek(SeekFrom::Start(0))?;
                file.write_u32(INIT_FILE_PAGE_NUM as u32 + num_of_page)?;

                // 第一页占用空间
                file.write_u32((PAGE_SIZE - (INIT_FILE_PAGE_NUM + num_of_page + 1) * 32) as u32);

                // 其余页占用空间
                for _i in 1..=num_of_page+3 {
                    file.write_u32(PAGE_SIZE as u32);
                }

                Ok(())
            }
            None => Err(Error::FileNotFound)
        }
    }

    /// 获取一个页
    /// 如果缓冲区有，直接从缓冲区拿
    /// 否则，加载一个磁盘页面到缓冲区
    /// 如果缓冲区已满，淘汰时间最早的页面
    fn get_page(&mut self, file_name: &str, page_num: usize) -> Result<Page, Error> {
        // 查询缓冲
        for i in self.list.iter_mut() {
            if i.page.file_name == file_name && i.page.page_num == page_num {
                i.time = SystemTime::now();
                return Ok(Page::new(i.page.get_data(), file_name, page_num));
            }
        }

        // 获取对应页数据
        let mut page: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        let file = self.file.get_mut(file_name).unwrap();
        file.seek(SeekFrom::Start(((page_num - 1) * PAGE_SIZE + NON_DATA_PAGE * PAGE_SIZE) as u64))?;
        file.read_exact(&mut page)?;

        // 更新缓冲
        // 如果缓冲没满
        if self.len < self.buff_size {
            self.list.push_back(LRUBufferItem {
                page: Page::new(page, file_name, page_num),
                time: SystemTime::now(),
            });
            self.len += 1;
            Ok(Page::new(page, file_name, page_num))
        } else {
            let mut min_time = SystemTime::now();
            let mut buffer_item: Option<&mut LRUBufferItem> = None;
            let mut min_time_page_num: Option<usize> = None;
            let mut min_time_file_name: Option<String> = None;

            // 寻找最旧页
            for i in self.list.iter() {
                if min_time > i.time {
                    min_time = i.time;
                    min_time_page_num = Some(i.page.page_num);
                    min_time_file_name = Some(i.page.file_name.clone());
                }
            }

            // 刷新最旧页
            match (min_time_page_num, min_time_file_name) {
                (Some(p_num), Some(f_name)) => {
                    self.flush(f_name.as_str(), &p_num)?
                }
                (_, _) => return Err(Error::UnexpectedError)
            }

            // 获取缓冲引用
            for i in self.list.iter_mut() {
                if min_time == i.time {
                    buffer_item = Some(i);
                    break;
                }
            }

            // 更新缓冲
            match buffer_item {
                Some(item) => {
                    item.page = Page::new(page, file_name, page_num);
                    item.time = SystemTime::now();
                    Ok(Page::new(page, file_name, page_num))
                }
                None => Err(Error::UnexpectedError)
            }
        }
    }

    /// 向缓冲区写入一个页面
    fn write_page(&mut self, page: Page) -> Result<(), Error> {
        // 查询缓冲
        for i in &mut self.list {
            if i.page.file_name == page.file_name && page.page_num == i.page.page_num {
                i.page = page;
                i.time = SystemTime::now();
                return Ok(());
            }
        }

        // 缓冲没命中，更新缓冲
        return if self.len < self.buff_size {
            // 缓冲没满
            self.list.push_back(LRUBufferItem {
                page,
                time: SystemTime::now(),
            });
            self.len += 1;
            Ok(())
        } else {
            let mut min_time = SystemTime::now();
            let mut buffer_item: Option<&mut LRUBufferItem> = None;
            let mut min_time_page_num: Option<usize> = None;
            let mut min_time_file_name: Option<String> = None;

            // 寻找最旧缓冲
            for i in self.list.iter() {
                if min_time > i.time {
                    min_time = i.time;
                    min_time_page_num = Some(i.page.page_num);
                    min_time_file_name = Some(i.page.file_name.clone());
                }
            }

            // 刷新最旧缓冲
            match (min_time_page_num, min_time_file_name) {
                (Some(p_num), Some(f_name)) => {
                    self.flush(f_name.as_str(), &p_num)?
                }
                (_, _) => return Err(Error::UnexpectedError)
            };

            // 获取缓冲引用
            for i in self.list.iter_mut() {
                if min_time == i.time {
                    buffer_item = Some(i);
                }
            }

            // 更新缓冲
            match buffer_item {
                Some(item) => {
                    item.page = page;
                    item.time = SystemTime::now();
                    Ok(())
                }
                None => Err(Error::UnexpectedError)
            }
        };
    }

    /// 强制刷新一个缓冲区的页面至磁盘
    /// 若页面不在缓冲区，则返回不在缓冲区异常
    fn flush(&mut self, file_name: &str, page_num: &usize) -> Result<(), Error> {
        for i in self.list.iter_mut() {
            if i.page.file_name == file_name && i.page.page_num == *page_num {
                i.time = SystemTime::now();
                let file = self.file.get_mut(file_name).unwrap();
                file.seek(SeekFrom::Start(((page_num - 1) * PAGE_SIZE + NON_DATA_PAGE * PAGE_SIZE) as u64))?;
                file.write_all(&i.page.get_data())?;
                return Ok(());
            }
        }
        Err(Error::NotInBufferError)
    }

    // 获取第一个uuid
    fn get_first_uuid(&mut self) -> Result<Uuid, Error> {
        // 获取uuid所在的页
        let page = self.get_page("metadata.db", METADATA_FILE_PAGE_NUM)?;
        // 获取对应字节数组
        let bytes = page.get_ptr_from_offset(FIRST_UUID_OFFSET, 16);
        let uuid = Uuid::from_slice(bytes);
        match uuid {
            Ok(uuid) => Ok(uuid),
            _ => Err(Error::UnexpectedError)
        }
    }

    // 更新第一个uuid
    fn update_first_uuid(&mut self, uuid: Uuid) -> Result<(), Error> {
        // 获取uuid所在页
        let mut page = self.get_page("metadata.db", METADATA_FILE_PAGE_NUM)?;
        // 写入对应的字节数组
        page.write_bytes_at_offset(uuid.as_bytes(), FIRST_UUID_OFFSET, 16)?;
        // 将页写回的缓冲池
        self.write_page(page)?;
        Ok(())
    }

    fn insert_bytes(&mut self, file_name: &str, bytes: &[u8]) -> Result<Position, Error> {
        let len = bytes.len();
        let raw_file = self.file.get_mut(file_name);

        let file = match raw_file {
            Some(file) => file,
            None => return Err(Error::FileNotFound)
        };

        file.seek(SeekFrom::Start(0))?;
        let page_num = file.read_u32();
        let offset = 32 * INIT_FILE_PAGE_NUM;
        for i in 0..&page_num as u64 {
            file.seek(SeekFrom::Start(offset as u64 + i * 32))?;
            let res = file.read_u32()?;
            if res > len as u32 {
                // 找到插入位置并插入
                file.seek(SeekFrom::Start((INIT_FILE_PAGE_NUM * PAGE_SIZE + i * PAGE_SIZE + PAGE_SIZE - res) as u64))?;
                file.write_all(bytes);

                // 更新文件头
                file.seek(SeekFrom::Start(offset as u64 + i * 32))?;
                file.write_u32(res - len);
                Ok(Position {
                    file_name: String::from(file_name),
                    page_num: i as usize,
                    offset: PAGE_SIZE - res,
                })
            }
        }
        // 如果文件不够大
        // 填充文件
        self.fill_up_to(file_name, 2 * page_num);
        // 重新插入
        self.insert_bytes(file_name, bytes)
    }

    fn read_bytes(&mut self, pos: Position, size: usize) -> Result<&[u8], Error> {
        let raw_file = self.file.get_mut(&pos.file_name);
        let file = match raw_file {
            Some(file) => file,
            None => return Err(Error::FileNotFound)
        };
        file.seek(SeekFrom::Start(0))?;
        let page_num = file.read_u32();
        if pos.page_num + INIT_FILE_PAGE_NUM > *page_num {
            return Err(Error::PageNumOutOfSize);
        }
        file.seek(SeekFrom::Start(((1 + INIT_FILE_PAGE_NUM + pos.page_num) * 32) as u64))?;
        let res = file.read_u32();
        if res + pos.offset > PAGE_SIZE {
            return Err(Error::UnexpectedError);
        }
        let page = [0; PAGE_SIZE];
        file.seek(SeekFrom::Start((INIT_FILE_PAGE_NUM * PAGE_SIZE + pos.page_num * PAGE_SIZE) as u64));
        file.read_exact(&page);

        Ok(&page[pos.offset..pos.offset + size])
    }
}

/// 采用时钟算法实现的Buffer
pub struct ClockBuffer {
    list: Vec<ClockBufferItem>,
    len: usize,
    file: HashMap<String, File>,
    cur: usize,
    buff_size: usize,
}

/// ClockBuffer中的每一项
struct ClockBufferItem {
    page: Page,
    access: u8,
}

impl ClockBuffer {
    fn new(buff_size: usize) -> Result<ClockBuffer, Error> {
        let path = Path::new("metadata.db");
        let mut hashmap = HashMap::<String, File>::new();
        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path);
        match fd {
            Ok(file) => {
                hashmap.insert(String::from("metadata.db"), file);
            }
            Err(_) => {
                let new_metadata = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(path)?;
                hashmap.insert(String::from("metadata.db"), new_metadata);
            }
        }
        let mut res = ClockBuffer {
            list: Vec::<ClockBufferItem>::new(),
            len: 0,
            buff_size,
            file: hashmap,
            cur: 0,
        };
        res.fill_up_to("metadata.db", METADATA_FILE_PAGE_NUM)?;
        Ok(res)
    }
}

impl Buffer for ClockBuffer {
    fn add_file(&mut self, path: &Path) -> Result<(), Error> {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let raw_file_name = path.to_str();
        let file_name = match raw_file_name {
            Some(file_name) => file_name,
            None => return Err(Error::FileNotFound)
        };
        self.file.insert(String::from(file_name), fd);
        Ok(())
    }

    /// 向文件填充占位符至指定页数
    fn fill_up_to(&mut self, file_name: &str, num_of_page: usize) -> Result<(), Error> {
        let raw_file = self.file.get_mut(file_name);
        match raw_file {
            Some(file) => {
                file.seek(SeekFrom::Start(0))?;
                file.write_all(Vec::<u8>::with_capacity(num_of_page * PAGE_SIZE + NON_DATA_PAGE * PAGE_SIZE).as_slice())?;
                Ok(())
            }
            None => Err(Error::FileNotFound)
        }
    }

    /// 根据偏移获取一个页面
    /// 如果页面在缓冲区，则直接返回，并更新access表示最近访问过
    /// 如果不在缓冲区，则加载一个磁盘页面至缓冲区
    /// 若缓冲区已满，则淘汰第一个遇到的access为0的页面，并将沿途access为1的页面置0，
    /// 新加载的页面的access置1
    fn get_page(&mut self, file_name: &str, page_num: usize) -> Result<Page, Error> {
        for i in self.list.iter_mut() {
            if i.page.file_name == file_name && i.page.page_num == page_num {
                i.access = 1;
                return Ok(Page::new(i.page.get_data(), file_name, page_num));
            }
        }

        let mut page: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        let file = self.file.get_mut(file_name).unwrap();
        file.seek(SeekFrom::Start(((page_num - 1) * PAGE_SIZE + NON_DATA_PAGE * PAGE_SIZE) as u64))?;
        file.read_exact(&mut page)?;

        if self.len < self.buff_size {
            self.len += 1;
            self.list.push(ClockBufferItem {
                page: Page::new(page, file_name, page_num),
                access: 1,
            });
        } else {
            let mut new_cur: Option<usize> = None;
            for i in 0..self.buff_size {
                let item = &mut self.list[(self.cur + i) % self.buff_size];
                if item.access == 1 {
                    item.access -= 1;
                } else {
                    new_cur = Some((self.cur + i) % self.buff_size);
                    break;
                }
            }
            self.cur = match new_cur {
                Some(ind) => {
                    ind
                }
                None => self.cur
            };
            let prev_page = &self.list[self.cur].page;
            let f_name = prev_page.file_name.clone();
            let p_num = prev_page.page_num;
            self.flush(f_name.as_str(), &p_num)?;
            self.list[self.cur] = ClockBufferItem {
                page: Page::new(page, file_name, page_num),
                access: 1,
            };
        }

        return Ok(Page::new(page, file_name, page_num));
    }

    /// 向缓冲区写入一个页面, 需要确保page.page_num正确
    fn write_page(&mut self, page: Page) -> Result<(), Error> {
        for i in &mut self.list {
            if i.page.page_num == page.page_num {
                i.page = page;
                return Ok(());
            }
        }
        return if self.len < self.buff_size {
            self.len += 1;
            self.list.push(ClockBufferItem {
                page,
                access: 1,
            });
            Ok(())
        } else {
            let mut new_cur: Option<usize> = None;
            for i in 0..self.buff_size {
                let item = &mut self.list[(self.cur + i) % self.buff_size];
                if item.access == 1 {
                    item.access -= 1;
                } else {
                    new_cur = Some((self.cur + i) % self.buff_size);
                    break;
                }
            }
            self.cur = match new_cur {
                Some(ind) => {
                    ind
                }
                None => self.cur
            };
            let prev_page = &self.list[self.cur].page;
            let f_name = prev_page.file_name.clone();
            let p_num = prev_page.page_num;
            self.flush(f_name.as_str(), &p_num)?;
            self.list[self.cur] = ClockBufferItem {
                page,
                access: 1,
            };
            Ok(())
        };
    }

    /// 强制刷新一个缓冲区的页面至磁盘
    /// 若页面不在缓冲区，则返回不在缓冲区异常
    fn flush(&mut self, file_name: &str, page_num: &usize) -> Result<(), Error> {
        for i in self.list.iter() {
            if i.page.file_name == file_name && i.page.page_num == *page_num {
                let file = self.file.get_mut(file_name).unwrap();
                file.seek(SeekFrom::Start(((page_num - 1) * PAGE_SIZE + NON_DATA_PAGE * PAGE_SIZE) as u64))?;
                file.write_all(&i.page.get_data())?;
                return Ok(());
            }
        }
        Err(Error::NotInBufferError)
    }

    fn get_first_uuid(&mut self) -> Result<Uuid, Error> {
        let page = self.get_page("metadata.db", METADATA_FILE_PAGE_NUM)?;
        let bytes = page.get_ptr_from_offset(FIRST_UUID_OFFSET, 16);
        let uuid = Uuid::from_slice(bytes);
        match uuid {
            Ok(uuid) => Ok(uuid),
            _ => Err(Error::UnexpectedError)
        }
    }

    fn update_first_uuid(&mut self, uuid: Uuid) -> Result<(), Error> {
        let mut page = self.get_page("metadata.db", METADATA_FILE_PAGE_NUM)?;
        page.write_bytes_at_offset(uuid.as_bytes(), FIRST_UUID_OFFSET, 16)?;
        self.write_page(page)?;
        Ok(())
    }

    fn insert_bytes(&mut self, file_name: &str, bytes: &[u8]) -> Result<Position, Error> {
        let len = bytes.len();
        let raw_file = self.file.get_mut(file_name);

        let file = match raw_file {
            Some(file) => file,
            None => return Err(Error::FileNotFound)
        };

        file.seek(SeekFrom::Start(0))?;
        let page_num = file.read_u32();
        let offset = 32 * INIT_FILE_PAGE_NUM;
        for i in 0..&page_num as u64 {
            file.seek(SeekFrom::Start(offset as u64 + i * 32))?;
            let res = file.read_u32()?;
            if res > len as u32 {
                // 找到插入位置并插入
                file.seek(SeekFrom::Start((INIT_FILE_PAGE_NUM * PAGE_SIZE + i * PAGE_SIZE + PAGE_SIZE - res) as u64))?;
                file.write_all(bytes);

                // 更新文件头
                file.seek(SeekFrom::Start(offset as u64 + i * 32))?;
                file.write_u32(res - len);
                Ok(Position {
                    file_name: String::from(file_name),
                    page_num: i as usize,
                    offset: PAGE_SIZE - res,
                })
            }
        }
        // 如果文件不够大
        // 填充文件
        self.fill_up_to(file_name, 2 * page_num);
        // 重新插入
        self.insert_bytes(file_name, bytes)
    }

    fn read_bytes(&mut self, pos: Position, size: usize) -> Result<&[u8], Error> {
        let raw_file = self.file.get_mut(&pos.file_name);
        let file = match raw_file {
            Some(file) => file,
            None => return Err(Error::FileNotFound)
        };
        file.seek(SeekFrom::Start(0))?;
        let page_num = file.read_u32();
        if pos.page_num + INIT_FILE_PAGE_NUM > *page_num {
            return Err(Error::PageNumOutOfSize);
        }
        file.seek(SeekFrom::Start(((1 + INIT_FILE_PAGE_NUM + pos.page_num) * 32) as u64))?;
        let res = file.read_u32();
        if res + pos.offset > PAGE_SIZE {
            return Err(Error::UnexpectedError);
        }
        let page = [0; PAGE_SIZE];
        file.seek(SeekFrom::Start((INIT_FILE_PAGE_NUM * PAGE_SIZE + pos.page_num * PAGE_SIZE) as u64));
        file.read_exact(&page);

        Ok(&page[pos.offset..pos.offset + size])
    }
}
