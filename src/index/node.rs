use std::convert::TryFrom;
use std::str;
use std::sync::{Arc, RwLock};

use crate::index::btree::MAX_BRANCHING_FACTOR;
use crate::index::key_value_pair::KeyValuePair;
use crate::page::page_item::{Page, PAGE_SIZE, PTR_SIZE};
use crate::page::pager::Pager;
use crate::util::error::Error;
use crate::data_item::buffer::Buffer;

/// 通用的节点头的格式 (共计 10 个字节)
const IS_ROOT_SIZE: usize = 1;
const IS_ROOT_OFFSET: usize = 0;
const NODE_TYPE_SIZE: usize = 1;
const NODE_TYPE_OFFSET: usize = 1;
const PARENT_POINTER_SIZE: usize = PTR_SIZE;
const PARENT_POINTER_OFFSET: usize = 2;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/// 叶子节点的头格式 (共计 18 个字节)
///
/// 键值对的空间: PAGE_SIZE - LEAF_NODE_HEADER_SIZE = 4096 - 34 = 4062 字节.
/// 其中叶子能够存储 4062 / keys_limit = 20 (10 个键和 10 个值).
const LEAF_NODE_NUM_PAIRS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_NUM_PAIRS_SIZE: usize = PTR_SIZE;
pub(crate) const LEAF_NODE_NEXT_NODE_PTR_OFFSET: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_PAIRS_SIZE;
pub(crate) const LEAF_NODE_PREVIOUS_NODE_PTR_OFFSET: usize = LEAF_NODE_NEXT_NODE_PTR_OFFSET + PTR_SIZE;
pub(crate) const LEAF_NODE_HEADER_SIZE: usize = LEAF_NODE_PREVIOUS_NODE_PTR_OFFSET + PTR_SIZE;
const LEAF_NODE_MAX_KEY_VALUE_PAIRS: usize = 10;

/// 内部节点的头格式 (共计 26 个字节)
///
/// 儿子节点与键的空间: PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE = 4096 - 26 = 4070 字节.
const INTERNAL_NODE_NUM_CHILDREN_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_NUM_CHILDREN_SIZE: usize = PTR_SIZE;
const INTERNAL_NODE_NUM_KEY_OFFSET: usize = INTERNAL_NODE_NUM_CHILDREN_OFFSET + PTR_SIZE;
const INTERNAL_NODE_NUM_KEY_SIZE: usize = PTR_SIZE;
pub(crate) const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_CHILDREN_SIZE + INTERNAL_NODE_NUM_KEY_SIZE;


/// 在一个 64 位机上存储儿子指针数的最大值
/// 是 200 * 8 = 1600 字节
/// +1是因为可能临时超过限制
/// 分裂后将会满足限制
const INTERNAL_NODE_CHILDREN_OFFSET: usize = INTERNAL_NODE_HEADER_SIZE;
pub(crate) const MAX_SPACE_FOR_CHILDREN: usize = (MAX_BRANCHING_FACTOR + 1) * PTR_SIZE;


/// 这留下了 2470 个字节给中间节点的键:
/// 我们用 2388 字节给键并且将剩下的 82 字节视为垃圾.
/// 这意味着每个键被限制为 12 字节. (2470 / keys limit(199) ~= 12)
/// 向下取整到 10 来容纳叶子节点.
const INTERNAL_NODE_KEY_OFFSET: usize = INTERNAL_NODE_CHILDREN_OFFSET + MAX_SPACE_FOR_CHILDREN;
#[allow(dead_code)]
pub(crate) const MAX_SPACE_FOR_KEYS: usize = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE - MAX_SPACE_FOR_CHILDREN;

/// 键和值的大小
pub(crate) const KEY_SIZE: usize = 10;
pub(crate) const VALUE_SIZE: usize = PTR_SIZE;

#[derive(PartialEq)]
pub enum NodeType {
    Internal = 1,
    Leaf = 2,
    Unknown,
}

/// 将一个字节转换成 NodeType.
impl From<u8> for NodeType {
    fn from(orig: u8) -> Self {
        match orig {
            0x01 => NodeType::Internal,
            0x02 => NodeType::Leaf,
            _ => NodeType::Unknown,
        }
    }
}

/// 将一个字节转换成布尔值.
trait FromByte {
    fn from_byte(&self) -> bool;
}

/// 将一个布尔值转换成一个字节
trait ToByte {
    fn to_byte(&self) -> u8;
}

impl FromByte for u8 {
    fn from_byte(&self) -> bool {
        matches!(self, 0x01)
    }
}

impl ToByte for bool {
    fn to_byte(&self) -> u8 {
        match self {
            true => 0x01,
            false => 0x00,
        }
    }
}

/// Node 代表一个页中的B+树的一个节点
pub struct Node {
    pub node_type: NodeType,
    pub parent_offset: usize,
    pub is_root: bool,
    pub offset: usize,
    pub page: Page,
}

impl Node {
    pub fn new(
        node_type: NodeType,
        parent_offset: usize,
        offset: usize,
        is_root: bool,
        mut page: Page,
    ) -> Result<Node, Error> {
        match node_type {
            NodeType::Internal => {
                let num_of_children = page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let num_of_key = page.get_value_from_offset(INTERNAL_NODE_NUM_KEY_OFFSET)?;

                page.write_value_at_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET, num_of_children)?;
                page.write_value_at_offset(INTERNAL_NODE_NUM_KEY_OFFSET, num_of_key)?;
                page.write_bytes_at_offset(&[is_root.to_byte()], IS_ROOT_OFFSET, IS_ROOT_SIZE)?;
                page.write_bytes_at_offset(&[1u8], NODE_TYPE_OFFSET, NODE_TYPE_SIZE)?;
            }
            NodeType::Leaf => {
                let num_of_pairs = page.get_value_from_offset(LEAF_NODE_NUM_PAIRS_OFFSET)?;
                page.write_value_at_offset(LEAF_NODE_NUM_PAIRS_OFFSET, num_of_pairs)?;
                page.write_bytes_at_offset(&[is_root.to_byte()], IS_ROOT_OFFSET, IS_ROOT_SIZE)?;
                page.write_bytes_at_offset(&[2u8], NODE_TYPE_OFFSET, NODE_TYPE_SIZE)?;
            }
            _ => return Err(Error::UnexpectedError)
        }
        Ok(Node {
            node_type,
            parent_offset,
            offset,
            is_root,
            page,
        })
    }

    /// get_key_value_pairs 如果是叶子节点，返回一个KeyValuePair的列表，
    /// 否则返回一个Error
    pub fn get_key_value_pairs(&self) -> Result<Vec<KeyValuePair>, Error> {
        return match self.node_type {
            NodeType::Leaf => {
                let mut res = Vec::<KeyValuePair>::new();
                let mut offset = LEAF_NODE_NUM_PAIRS_OFFSET;
                let num_keys_val_pairs = self.page.get_value_from_offset(offset)?;

                offset = LEAF_NODE_HEADER_SIZE;

                for _i in 0..num_keys_val_pairs {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    offset += KEY_SIZE;

                    let value = self.page.get_value_from_offset(offset)?;
                    offset += VALUE_SIZE;

                    // 去除首位0字符
                    res.push(KeyValuePair::new(
                        key.trim_matches(char::from(0)).to_string(),
                        value,
                    ))
                }
                Ok(res)
            }
            _ => Err(Error::UnexpectedError),
        };
    }

    /// get_children 如果是中间节点，返回一个孩子节点的 offset 列表，
    /// 否则，返回错误
    pub fn get_children(&self) -> Result<Vec<usize>, Error> {
        match self.node_type {
            NodeType::Internal => {
                let num_children = self.page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let mut result = Vec::<usize>::new();
                let mut offset = INTERNAL_NODE_CHILDREN_OFFSET;
                for _i in 1..=num_children {
                    let child_offset = self.page.get_value_from_offset(offset)?;
                    result.push(child_offset);
                    offset += PTR_SIZE;
                }
                Ok(result)
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// get_keys 返回一个包装有 Key 列表的 Result
    /// todo check 能否保证拿出来的键有序？
    pub fn get_keys(&self) -> Result<Vec<String>, Error> {
        return match self.node_type {
            NodeType::Internal => {
                let mut result = Vec::<String>::new();
                let mut offset = INTERNAL_NODE_KEY_OFFSET;
                let num_keys = self.page.get_value_from_offset(INTERNAL_NODE_NUM_KEY_OFFSET)?;
                for _i in 1..=num_keys {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    offset += KEY_SIZE;
                    // 去掉首尾 \0 字符
                    result.push(key.trim_matches(char::from(0)).to_string());
                }
                Ok(result)
            }
            NodeType::Leaf => {
                let mut res = Vec::<String>::new();
                let mut offset = LEAF_NODE_NUM_PAIRS_OFFSET;
                let num_keys_val_pairs = self.page.get_value_from_offset(offset)?;
                offset = LEAF_NODE_HEADER_SIZE;
                for _i in 1..=num_keys_val_pairs {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    // 跳过value
                    offset += KEY_SIZE + VALUE_SIZE;
                    res.push(key.trim_matches(char::from(0)).to_string());
                }
                Ok(res)
            }
            NodeType::Unknown => Err(Error::UnexpectedError),
        };
    }

    /// add_key_value_pair 增加一个键值对到 self ,
    /// 只应当在叶子节点上使用.
    pub fn add_key_value_pair(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        match self.node_type {
            NodeType::Leaf => {
                let num_keys_val_pairs = self.page.get_value_from_offset(LEAF_NODE_NUM_PAIRS_OFFSET)?;
                if num_keys_val_pairs >= LEAF_NODE_MAX_KEY_VALUE_PAIRS {
                    return Err(Error::UnexpectedError);
                }
                let offset = LEAF_NODE_HEADER_SIZE + (KEY_SIZE + VALUE_SIZE) * num_keys_val_pairs;
                // 更新键值对数
                self.page.write_value_at_offset(LEAF_NODE_NUM_PAIRS_OFFSET, num_keys_val_pairs + 1)?;

                // 写入键值对
                let key_raw = kv.key.as_bytes();
                self.page.write_bytes_at_offset(key_raw, offset, KEY_SIZE)?;
                let value_raw = kv.value.to_be_bytes();
                self.page.write_bytes_at_offset(&value_raw, offset + KEY_SIZE, VALUE_SIZE)?;
                Ok(())
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// 增加一个键, 和该键的右子节点
    /// 只应当在中间节点上使用.
    pub fn add_key_and_left_child(&mut self, key: String, left_child_offset: usize) -> Result<(), Error> {
        return match self.node_type {
            NodeType::Internal => {
                // 更新孩子数 (等于键数+1)
                let num_children = self.page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                self.page.write_value_at_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET, num_children + 1)?;

                // 寻找新键的位置.
                let num_keys = self.page.get_value_from_offset(INTERNAL_NODE_NUM_KEY_OFFSET)?;

                let mut offset = INTERNAL_NODE_KEY_OFFSET;
                let end_key_data = offset + num_keys * KEY_SIZE;

                for i in 0..num_keys {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let iter_key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    if *iter_key > *key.as_str() {
                        // 找到位置.
                        self.page.insert_bytes_at_offset(
                            key.as_bytes(),
                            offset,
                            end_key_data,
                            KEY_SIZE,
                        )?;
                        offset = INTERNAL_NODE_CHILDREN_OFFSET;
                        let end_child_data = offset + num_children * PTR_SIZE;
                        offset += i * PTR_SIZE;
                        self.page.insert_bytes_at_offset(
                            &left_child_offset.to_be_bytes(),
                            offset,
                            end_child_data,
                            PTR_SIZE,
                        )?;
                        return Ok(());
                    }
                    offset += KEY_SIZE;
                }
                // 找到位置.
                self.page.insert_bytes_at_offset(
                    key.as_bytes(),
                    offset,
                    end_key_data,
                    KEY_SIZE,
                )?;
                offset = INTERNAL_NODE_CHILDREN_OFFSET;
                let end_child_data = offset + num_children * PTR_SIZE;
                offset += num_children * PTR_SIZE - KEY_SIZE;
                self.page.insert_bytes_at_offset(
                    &left_child_offset.to_be_bytes(),
                    offset,
                    end_child_data,
                    PTR_SIZE,
                )?;
                Ok(())
            }
            _ => Err(Error::UnexpectedError),
        };
    }

    /// get_keys_len 获取当前节点的键数.
    pub fn get_keys_len(&self) -> Result<usize, Error> {
        match self.node_type {
            NodeType::Internal => self.page.get_value_from_offset(INTERNAL_NODE_NUM_KEY_OFFSET),
            NodeType::Leaf => self.page.get_value_from_offset(LEAF_NODE_NUM_PAIRS_OFFSET),
            NodeType::Unknown => Err(Error::UnexpectedError),
        }
    }

    /// get_keys 返回当前节点中包含键的键值对.
    pub fn find_key_value_pair(&self, key: String) -> Result<KeyValuePair, Error> {
        match self.node_type {
            NodeType::Leaf => {
                let kv_pairs = self.get_key_value_pairs()?;
                for kv_pair in kv_pairs {
                    if kv_pair.key == key {
                        return Ok(kv_pair);
                    }
                }
                Err(Error::KeyNotFound)
            }
            _ => Err(Error::KeyNotFound),
        }
    }

    /// 将一个内部节点的key更换成新的key（!!!不保证更改后的key的大小顺序!!!）
    pub fn update_internal_key(&mut self, old_key: &str, new_key: &str) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal => {
                let num_children = self.page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let mut offset = INTERNAL_NODE_HEADER_SIZE + num_children * PTR_SIZE;
                let num_keys = self.page.get_value_from_offset(INTERNAL_NODE_NUM_KEY_OFFSET)?;
                for _i in 1..=num_keys {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    if *key == *old_key {
                        return self.page.write_bytes_at_offset(new_key.trim_matches(char::from(0)).as_bytes(), offset, KEY_SIZE);
                    }
                    offset += KEY_SIZE;
                }
                Err(Error::KeyNotFound)
            }
            _ => Err(Error::UnexpectedError)
        }
    }

    /// 将内部节点的指定offset更新成新的offset
    fn update_internal_value(&mut self, old_node_offset: &usize, new_node_offset: &usize) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal => {
                for (i, offset) in self.get_children()?.iter().enumerate() {
                    if *offset == *old_node_offset {
                        return self.page.write_value_at_offset(INTERNAL_NODE_CHILDREN_OFFSET + i * PTR_SIZE, *new_node_offset);
                    }
                }

                Err(Error::KeyNotFound)
            }
            _ => Err(Error::UnexpectedError)
        }
    }

    /// update_value 更新当前节点中包含键的键值对.
    pub fn update_value(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        match self.node_type {
            NodeType::Leaf => {
                let mut offset = LEAF_NODE_NUM_PAIRS_OFFSET;
                let num_keys_val_pairs = self.page.get_value_from_offset(offset)?;

                offset = LEAF_NODE_HEADER_SIZE;

                for _i in 0..num_keys_val_pairs {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    offset += KEY_SIZE;
                    if key.trim_matches(char::from(0)) == kv.key.trim_matches(char::from(0)) {
                        let value_raw = kv.value.to_be_bytes();
                        self.page.write_bytes_at_offset(&value_raw, offset, VALUE_SIZE)?;
                        return Ok(());
                    }
                    offset += VALUE_SIZE;
                }
                Err(Error::KeyNotFound)
            }
            _ => Err(Error::KeyNotFound),
        }
    }

    /// 向key和children数量一样的节点加一个child
    fn add_child(&mut self, child_offset: usize) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal => {
                let child_num = self.page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let key_num = self.page.get_value_from_offset(INTERNAL_NODE_NUM_KEY_OFFSET)?;
                if key_num < child_num {
                    return Err(Error::UnexpectedError);
                }
                self.page.write_bytes_at_offset(&(child_num + 1).to_be_bytes(), INTERNAL_NODE_CHILDREN_OFFSET, INTERNAL_NODE_NUM_CHILDREN_SIZE)?;
                let offset = INTERNAL_NODE_CHILDREN_OFFSET + PTR_SIZE * child_num;
                self.page.write_bytes_at_offset(&child_offset.to_be_bytes(), offset, PTR_SIZE)?;
                Ok(())
            }
            _ => Err(Error::UnexpectedError)
        }
    }

    /// 分裂内部节点
    /// !!!不做任何检查!!!
    fn split_internal(&mut self, pager: &mut Pager, buffer: &mut Box<dyn Buffer>) -> Result<(Node, String, Node), Error> {
        let mut offset = INTERNAL_NODE_KEY_OFFSET;
        let num_key = self.page.get_value_from_offset(INTERNAL_NODE_NUM_KEY_OFFSET)?;
        let children = self.get_children()?;
        let split_node_num_key = num_key / 2;
        let left_page = pager.get_new_page(buffer)?;
        let right_page = pager.get_new_page(buffer)?;
        let mut left_node = Node::new(NodeType::Internal, self.parent_offset, left_page.page_num, false, left_page)?;
        let mut right_node = Node::new(NodeType::Internal, self.parent_offset, right_page.page_num, false, right_page)?;

        // 前一半的键给新左儿子
        for i in 1..split_node_num_key {
            let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
            let child_offset = children.get(i - 1).unwrap();
            let key = match str::from_utf8(key_raw) {
                Ok(key) => key,
                Err(_) => return Err(Error::UTF8Error),
            };
            left_node.add_key_and_left_child(key.trim_matches(char::from(0)).to_string(), *child_offset)?;
            offset += KEY_SIZE;
        }

        // 跳过中间键（中间键需要上弹）
        offset += KEY_SIZE;

        // 中间键的左儿子给新左儿子
        let median_offset = children.get(split_node_num_key).unwrap();
        left_node.add_child(*median_offset)?;

        // 后一半的键给新右儿子
        for i in split_node_num_key + 1..num_key {
            let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
            let child_offset = children.get(i).unwrap();
            let key = match str::from_utf8(key_raw) {
                Ok(key) => key,
                Err(_) => return Err(Error::UTF8Error),
            };
            right_node.add_key_and_left_child(String::from(key), *child_offset)?;
            offset += KEY_SIZE;
        }

        // 最后一个儿子给右儿子
        let child_offset = children.get(num_key).unwrap();
        right_node.add_child(*child_offset)?;

        // 将中间键作为上弹的键
        offset = INTERNAL_NODE_KEY_OFFSET;
        let median_key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
        let median_key = match str::from_utf8(median_key_raw) {
            Ok(key) => key,
            Err(_) => return Err(Error::UTF8Error),
        };

        Ok((left_node, median_key.trim_matches(char::from(0)).to_string(), right_node))
    }

    pub fn add_next_node(&mut self, offset: usize) -> Result<(), Error> {
        self.page.write_value_at_offset(LEAF_NODE_NEXT_NODE_PTR_OFFSET, offset)
    }

    pub fn get_next_node(&self, pager: &mut Pager, buffer: &mut Box<dyn Buffer>) -> Result<Node, Error> {
        let offset = self.page.get_value_from_offset(LEAF_NODE_NEXT_NODE_PTR_OFFSET)?;
        let page_num = offset / PAGE_SIZE;
        let next_node = Node::try_from(
            NodeSpec {
                page_data: pager.get_page(&page_num, buffer).unwrap().get_data(),
                offset,
            }
        )?;
        Ok(next_node)
    }

    pub fn add_previous_node(&mut self, offset: usize) -> Result<(), Error> {
        self.page.write_value_at_offset(LEAF_NODE_PREVIOUS_NODE_PTR_OFFSET, offset)
    }

    pub fn get_previous_node(&self, pager: &mut Pager, buffer: &mut Box<dyn Buffer>) -> Result<Node, Error> {
        let offset = self.page.get_value_from_offset(LEAF_NODE_PREVIOUS_NODE_PTR_OFFSET)?;
        let page_num = offset / PAGE_SIZE;
        let previous_node = Node::try_from(
            NodeSpec {
                page_data: pager.get_page(&page_num, buffer).unwrap().get_data(),
                offset,
            }
        )?;
        Ok(previous_node)
    }


    /// 分裂叶子节点
    /// !!!不做任何检查!!!
    fn split_leaf(&mut self, pager: &mut Pager, buffer: &mut Box<dyn Buffer>) -> Result<(Node, String, Node), Error> {
        // 初始化新的左右叶子节点
        let mut kv_pairs = self.get_key_value_pairs()?;
        let left_leaf_page = pager.get_new_page(buffer)?;
        let right_leaf_page = pager.get_new_page(buffer)?;
        let mut left_leaf = Node::new(NodeType::Leaf, self.parent_offset, left_leaf_page.page_num, false, left_leaf_page)?;
        let mut right_leaf = Node::new(NodeType::Leaf, self.parent_offset, right_leaf_page.page_num, false, right_leaf_page)?;
        left_leaf.add_next_node(right_leaf.offset)?;
        let previous_node_offset = self.page.get_value_from_offset(LEAF_NODE_PREVIOUS_NODE_PTR_OFFSET)?;
        left_leaf.add_previous_node(previous_node_offset)?;

        right_leaf.add_previous_node(left_leaf.offset)?;
        let next_node_offset = self.page.get_value_from_offset(LEAF_NODE_NEXT_NODE_PTR_OFFSET)?;
        right_leaf.add_next_node(next_node_offset)?;

        if previous_node_offset != 0 {
            let mut previous_node = left_leaf.get_previous_node(pager, buffer)?;
            previous_node.add_next_node(left_leaf.offset)?;
        }

        if next_node_offset != 0 {
            let mut next_node = right_leaf.get_next_node(pager, buffer)?;
            next_node.add_previous_node(right_leaf.offset)?;
        }

        kv_pairs.sort();
        let mid = kv_pairs.len() / 2;
        for (i, kv) in kv_pairs.iter_mut().enumerate() {
            if i < mid {
                left_leaf.add_key_value_pair(kv.clone())?
            } else {
                right_leaf.add_key_value_pair(kv.clone())?
            }
        }

        Ok((left_leaf, kv_pairs.get(mid).unwrap().key.clone(), right_leaf))
    }


    /// 将当前节点分裂成两个节点，并返回中介节点的键和两个节点
    pub(crate) fn split(&mut self, pager: &mut Pager, buffer: &mut Box<dyn Buffer>) -> Result<(bool, usize), Error> {
        if self.is_root {

            // 根节点不满足分裂要求
            if self.get_keys_len()? <= MAX_BRANCHING_FACTOR {
                return Ok((false, 0));
            }

            let (left_node, median_key, right_node) = self.split_internal(pager, buffer)?;

            // 新的根节点只有两个儿子，分别是新左儿子、新右儿子
            self.page.write_value_at_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET, 2)?;

            // 将新左儿子、新右儿子写入到根节点的儿子偏移处
            let offset = INTERNAL_NODE_CHILDREN_OFFSET;
            self.page.write_bytes_at_offset(&left_node.offset.to_be_bytes(), offset, PTR_SIZE)?;
            self.page.write_bytes_at_offset(&right_node.offset.to_be_bytes(), offset, PTR_SIZE)?;

            // 将新的键写入根节点
            self.page.write_bytes_at_offset(median_key.as_bytes(), offset, KEY_SIZE)?;

            // 有分裂，返回true
            return Ok((true, left_node.offset));
        }

        // 不是根节点的情况
        match self.node_type {
            NodeType::Internal => {

                // 是中间节点且不满足分裂条件
                if self.get_keys_len()? < MAX_BRANCHING_FACTOR {
                    return Ok((false, 0));
                }

                // 分裂当前节点
                let (left_node, median_key, right_node) = self.split_internal(pager, buffer)?;

                // 获取父节点
                let parent_offset = self.parent_offset;
                let page_num = parent_offset / PAGE_SIZE;
                let lock =
                    Arc::new(
                        RwLock::new(
                            Node::try_from(
                                NodeSpec {
                                    page_data: pager.get_page(&page_num, buffer).unwrap().get_data(),
                                    offset: parent_offset,
                                }
                            )?
                        )
                    );
                let mut parent_node = match lock.write() {
                    Err(_) => return Err(Error::UnexpectedError),
                    Ok(node) => node,
                };
                // 将新左儿子加到父亲
                parent_node.add_key_and_left_child(median_key, left_node.offset)?;
                parent_node.update_internal_value(&self.offset, &right_node.offset)?;
                // todo 释放当前节点
                Ok((true, left_node.offset))
            }
            NodeType::Leaf => {

                // 是叶子节点，且不满足分裂条件
                if self.get_key_value_pairs()?.len() < LEAF_NODE_MAX_KEY_VALUE_PAIRS {
                    return Ok((false, 0));
                }

                // 分裂当前节点
                let (left_leaf, median_key, right_leaf) = self.split_leaf(pager, buffer)?;

                // 获取父节点
                let parent_offset = self.parent_offset;
                let page_num = parent_offset / PAGE_SIZE;
                let lock_parent_node =
                    Arc::new(
                        RwLock::new(
                            Node::try_from(
                                NodeSpec {
                                    page_data: pager.get_page(&page_num, buffer).unwrap().get_data(),
                                    offset: parent_offset,
                                }
                            )?
                        )
                    );
                let mut parent_node = match lock_parent_node.write() {
                    Err(_) => return Err(Error::UnexpectedError),
                    Ok(node) => node,
                };
                parent_node.add_key_and_left_child(median_key, left_leaf.offset)?;
                parent_node.update_internal_value(&self.offset, &right_leaf.offset)?;
                // todo 释放当前节点
                Ok((true, left_leaf.offset))
            }
            NodeType::Unknown => Err(Error::UnexpectedError),
        }
    }

    /// 将叶子节点的有效位置零
    /// 非叶子节点抛出异常
    /// todo 节点删除
    pub fn delete(&mut self) -> Result<(), Error> {
        match self.node_type {
            NodeType::Leaf => Err(Error::UnexpectedError),
            _ => Err(Error::UnexpectedError)
        }
    }
}

impl TryFrom<Node> for [u8; PAGE_SIZE] {
    type Error = Error;

    fn try_from(node: Node) -> Result<Self, Self::Error> {
        let mut result: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];

        result[IS_ROOT_OFFSET] = node.is_root.to_byte();

        Ok(result)
    }
}

/// NodeSpec 是一个包装。
/// 通过 TryFrom 将一个页的字节数组转换成 Node struct 来实现.
pub struct NodeSpec {
    pub page_data: [u8; PAGE_SIZE],
    pub offset: usize,
}

impl TryFrom<NodeSpec> for Node {
    type Error = Error;
    fn try_from(spec: NodeSpec) -> Result<Self, Self::Error> {
        let page = Page::new_phantom(spec.page_data);
        let is_root = spec.page_data[IS_ROOT_OFFSET].from_byte();
        let node_type = NodeType::from(spec.page_data[NODE_TYPE_OFFSET]);
        if node_type == NodeType::Unknown {
            return Err(Error::UnexpectedError);
        }
        let parent_pointer_offset = page.get_value_from_offset(PARENT_POINTER_OFFSET)?;

        Node::new(
            node_type,
            parent_pointer_offset,
            spec.offset,
            is_root,
            page,
        )
    }
}
