use std::convert::TryFrom;
use std::mem::size_of;

use crate::util::error::Error;

/// 一个页的大小
pub const PAGE_SIZE: usize = 4096;

/// PTR_SIZE 代表一个指针指向的数据的长度
pub const PTR_SIZE: usize = size_of::<usize>();

/// Value 结构体是对页内数据地址的包装
pub struct Value(usize);

/// 对单页内存数据的包装
/// 提供一些方便的接口
pub struct Page {
    pub(crate) file_name: String,
    pub(crate) page_num: usize,
    data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    pub fn new_phantom(data: [u8; PAGE_SIZE]) -> Page {
        Page {
            file_name: String::new(),
            page_num: 0, // 0为孤立页面，不放在缓冲池、磁盘内
            data: Box::new(data),
        }
    }

    pub fn new(data: [u8; PAGE_SIZE], file_name: &str, page_num: usize) -> Page {
        Page {
            file_name: String::from(file_name),
            page_num, // 0为孤立页面，不放在缓冲池、磁盘内
            data: Box::new(data),
        }
    }


    /// 向指定偏移写入一个值
    /// 覆盖指定偏移上的值
    pub fn write_value_at_offset(&mut self, offset: usize, value: usize) -> Result<(), Error> {
        if offset > PAGE_SIZE - PTR_SIZE {
            return Err(Error::UnexpectedError);
        }
        // 转换成字节数组后写入
        let bytes = value.to_be_bytes();
        self.data[offset..offset + PTR_SIZE].clone_from_slice(&bytes);
        Ok(())
    }

    /// 从指定偏移获取一个大端值，并转换成 usize
    /// 如果取出的值无法转换成usize就会报错
    pub fn get_value_from_offset(&self, offset: usize) -> Result<usize, Error> {
        let bytes = &self.data[offset..offset + PTR_SIZE];
        let Value(res) = Value::try_from(bytes)?;
        Ok(res)
    }

    /// 向 offset 到 end_offset 的每个偏移上插入大小为 size 的字节数组
    /// 腾出 offset 到 end_offset 的空间， 然后插入
    pub fn insert_bytes_at_offset(
        &mut self,
        bytes: &[u8],
        offset: usize,
        end_offset: usize,
        size: usize,
    ) -> Result<(), Error> {
        // 最后位置插入后不能超过页大小
        if end_offset + size > self.data.len() {
            return Err(Error::UnexpectedError);
        }
        for idx in (offset..=end_offset).rev() {
            self.data[idx + size] = self.data[idx]
        }
        self.data[offset..offset + size].clone_from_slice(&bytes);
        Ok(())
    }

    /// 写入从 offset 开始 size 大小的字节数组，覆盖原有数据
    pub fn write_bytes_at_offset(
        &mut self,
        bytes: &[u8],
        offset: usize,
        size: usize,
    ) -> Result<(), Error> {
        let siz = if bytes.len() < size {
            bytes.len()
        } else {
            size
        };
        self.data[offset..offset + siz].clone_from_slice(&bytes);
        Ok(())
    }

    /// 从 offset 开始获取 size 大小的字节数组
    pub fn get_ptr_from_offset(&self, offset: usize, size: usize) -> &[u8] {
        &self.data[offset..offset + size]
    }

    /// 获取整个 data 数组
    pub fn get_data(&self) -> [u8; PAGE_SIZE] {
        *self.data
    }
}

/// 将 PTR_SIZE 大小的字节数组转换成 Value 结构体
impl TryFrom<&[u8]> for Value {
    type Error = Error;

    fn try_from(arr: &[u8]) -> Result<Self, Self::Error> {
        if arr.len() > PTR_SIZE {
            return Err(Error::TryFromSliceError("未预期一场: 数组长度超过最大允许长度: 4096B."));
        }

        let mut truncated_arr = [0u8; PTR_SIZE];
        for (i, item) in arr.iter().enumerate() {
            truncated_arr[i] = *item;
        }

        Ok(Value(usize::from_be_bytes(truncated_arr)))
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_needed_here() {
        // todo
    }
}