use crate::page::{PAGE_SIZE, Page};
use crate::error::Error;
use std::collections::LinkedList;
use std::time::SystemTime;
use std::fs::{File, OpenOptions};
use std::io::{SeekFrom, Seek, Read, Write};
use std::path::Path;

const BUFFER_NUM: usize = 20;
const BUFFER_SIZE: usize = PAGE_SIZE * BUFFER_NUM;


/// 缓冲区的trait，实现了通过缓冲区获取页、写入页、强制刷新页
pub trait Buffer {
    fn get_page(&mut self, offset: usize) -> Result<[u8; PAGE_SIZE], Error>;

    fn write_page(&mut self, offset: usize, page: Page) -> Result<(), Error>;

    fn flush(&mut self, offset: usize) -> Result<(), Error>;
}

/// LRU算法实现的Buffer
pub struct LRUBuffer {
    buff: Box<[u8; BUFFER_SIZE]>,
    list: LinkedList<LRUBufferItem>,
    len: usize,
    file: File,
}

/// LRUBuffer中的每一项
struct LRUBufferItem {
    offset: usize,
    page: Page,
    time: SystemTime,
}

impl LRUBuffer {
    /// LRUBuffer的构造方法
    fn new(path: &Path) -> LRUBuffer {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        LRUBuffer {
            buff: Box::new([0; BUFFER_SIZE]),
            list: LinkedList::<LRUBufferItem>::new(),
            len: 0,
            file: fd,
        }
    }
}

impl Buffer for LRUBuffer {
    /// 获取一个页
    /// 如果缓冲区有，直接从缓冲区拿
    /// 否则，加载一个磁盘页面到缓冲区
    /// 如果缓冲区已满，淘汰时间最早的页面
    fn get_page(&mut self, offset: usize) -> Result<[u8; PAGE_SIZE], Error> {
        for i in self.list.iter() {
            if *i.offset == offset {
                *i.time = SystemTime::now();
                return Ok(*i.page.get_data());
            }
        }
        let mut page: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(&mut page)?;
        if len < BUFFER_NUM {
            self.list.push_back(LRUBufferItem {
                offset,
                page: Page::new(page),
                time: SystemTime::now(),
            });
            self.len += 1;
            Ok(page)
        } else {
            let mut min_time = SystemTime::now();
            let mut buffer_item: Option<&mut LRUBufferItem> = None;
            for i in self.list.iter_mut() {
                if min_time > *i.time {
                    min_time = *i.time;
                    buffer_item = some(i);
                }
            }
            match buffer_item {
                Some(item) => {
                    self.flush(item.offset);
                    item.offset = offset;
                    item.page = Page::new(page);
                    item.time = SystemTime::now();
                    Ok(page)
                }
                None => Err(Error::UnexpectedError)
            }
        }
    }

    /// 向缓冲区写入一个页面
    fn write_page(&mut self, offset: usize, page: Page) -> Result<(), Error> {
        if len < BUFFER_NUM {
            self.list.push_back(LRUBufferItem {
                offset,
                page,
                time: SystemTime::now(),
            });
            self.len += 1;
            Ok(())
        } else {
            let mut min_time = SystemTime::now();
            let mut buffer_item: Option<&mut LRUBufferItem> = None;
            for i in self.list.iter_mut() {
                if min_time > *i.time {
                    min_time = *i.time;
                    buffer_item = some(i);
                }
            }
            match buffer_item {
                Some(item) => {
                    self.flush(item.offset);
                    item.offset = offset;
                    item.page = page;
                    item.time = SystemTime::now();
                    Ok(())
                }
                None => Err(Error::UnexpectedError)
            }
        }
    }

    /// 强制刷新一个缓冲区的页面至磁盘
    /// 若页面不在缓冲区，则返回不在缓冲区异常
    fn flush(&mut self, offset: usize) -> Result<(), Error> {
        for i in self.list.iter() {
            if *i.offset == offset {
                *i.time = SystemTime::now();
                self.file.seek(SeekFrom::Start(*offset as u64))?;
                self.file.write_all(*i.get_data())?;
                return Ok(())
            }
        }
        Err(Error::NotInBufferError)
    }
}

/// 采用时钟算法实现的Buffer
pub struct ClockBuffer {
    buff: Box<[u8; BUFFER_SIZE]>,
    list: Vec<ClockBufferItem>,
    len: usize,
    file: File,
    cur: usize,
}

/// ClockBuffer中的每一项
struct ClockBufferItem {
    offset: usize,
    page: Page,
    access: u8,
}

impl ClockBuffer {
    fn new(path: &Path) -> ClockBuffer {
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        ClockBuffer {
            buff: Box::new([0; BUFFER_SIZE]),
            list: Vec::<ClockBufferItem>::new(),
            len: 0,
            file: fd,
            cur: 0,
        }
    }
}

impl Buffer for ClockBuffer {
    /// 根据偏移获取一个页面
    /// 如果页面在缓冲区，则直接返回，并更新access表示最近访问过
    /// 如果不在缓冲区，则加载一个磁盘页面至缓冲区
    /// 若缓冲区已满，则淘汰第一个遇到的access为0的页面，并将沿途access为1的页面置0，
    /// 新加载的页面的access置1
    fn get_page(&mut self, offset: usize) -> Result<[u8; PAGE_SIZE], Error> {
        for i in self.list.iter() {
            if *i.offset == offset {
                *i.access = 1;
                return Ok(*i.page.get_data())
            }
        }

        let mut page: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(&mut page)?;

        if self.len < BUFFER_NUM {
            self.len += 1;
            self.list.push(ClockBufferItem {
                offset,
                page: Page::new(page),
                access: 1,
            });
        } else {
            let mut new_cur: Option<usize> = None;
            for i in 0..BUFFER_NUM {
                let item: &mut ClockBufferItem = &mut self.list[(cur + i) % BUFFER_NUM];
                if item.access == 1 {
                    item.access -= 1;
                } else {
                    new_cur = Some((cur + i) % BUFFER_NUM);
                    break;
                }
            }
            self.cur = match new_cur {
                Some(ind) => {
                    ind
                }
                None => self.cur
            };
            self.flush(self.cur);
            self.list[self.cur] = ClockBufferItem {
                page: Page::new(page),
                access: 1,
                offset,
            };
        }

        return Ok(page)

    }

    /// 向缓冲区写入一个页面
    fn write_page(&mut self, offset: usize, page: Page) -> Result<(), Error> {
        if self.len < BUFFER_NUM {
            self.len += 1;
            self.list.push(ClockBufferItem {
                offset,
                page,
                access: 1,
            });
            return Ok(())
        } else {
            let mut new_cur: Option<usize> = None;
            for i in 0..BUFFER_NUM {
                let item: &mut ClockBufferItem = &mut self.list[(cur + i) % BUFFER_NUM];
                if item.access == 1 {
                    item.access -= 1;
                } else {
                    new_cur = Some((cur + i) % BUFFER_NUM);
                    break;
                }
            }
            self.cur = match new_cur {
                Some(ind) => {
                    ind
                }
                None => self.cur
            };
            self.flush(self.cur);
            self.list[self.cur] = ClockBufferItem {
                page,
                access: 1,
                offset,
            };
            return Ok(())
        }
    }

    /// 强制刷新一个缓冲区的页面至磁盘
    /// 若页面不在缓冲区，则返回不在缓冲区异常
    fn flush(&mut self, offset: usize) -> Result<(), Error> {
        for i in self.list.iter() {
            if *i.offset == offset {
                *i.time = SystemTime::now();
                self.file.seek(SeekFrom::Start(*offset as u64))?;
                self.file.write_all(*i.get_data())?;
                return Ok(())
            }
        }
        Err(Error::NotInBufferError)
    }
}
