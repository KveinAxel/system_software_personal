use std::convert::TryFrom;
use std::str;
use std::sync::{Arc, RwLock};

use crate::index::btree::MAX_BRANCHING_FACTOR;
use crate::index::key_value_pair::KeyValuePair;
use crate::page::page_item::{Page, PAGE_SIZE, PTR_SIZE};
use crate::page::pager::Pager;
use crate::util::error::Error;

/// 通用的节点头的格式 (共计 10 个字节)
const IS_ROOT_SIZE: usize = 1;
const IS_ROOT_OFFSET: usize = 0;
const NODE_TYPE_SIZE: usize = 1;
const NODE_TYPE_OFFSET: usize = 1;
const VALID_SIZE: usize = 1;
const VALID_OFFSET: usize = 2;
const PARENT_POINTER_SIZE: usize = PTR_SIZE;
const PARENT_POINTER_OFFSET: usize = 3;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE + VALID_SIZE;

/// 叶子节点的头格式 (共计 18 个字节)
///
/// 键值对的空间: PAGE_SIZE - LEAF_NODE_HEADER_SIZE = 4096 - 18 = 4076 字节.
/// 其中叶子能够存储 4076 / keys_limit = 20 (10 个键和 10 个值).
const LEAF_NODE_NUM_PAIRS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_NUM_PAIRS_SIZE: usize = PTR_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_PAIRS_SIZE;

/// 内部节点的头格式 (共计 18 个字节)
///
/// 儿子节点与键的空间: PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE = 4096 - 18 = 4076 字节.
const INTERNAL_NODE_NUM_CHILDREN_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_NUM_CHILDREN_SIZE: usize = PTR_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_CHILDREN_SIZE;

/// 在一个 64 位机上存储儿子指针数的最大值
/// 是 200 * 8 = 1600 字节
/// +1是因为可能临时超过限制
/// 分裂后将会满足限制
const INTERNAL_NODE_CHILDREN_OFFSET: usize = INTERNAL_NODE_HEADER_SIZE;
const MAX_SPACE_FOR_CHILDREN: usize = (MAX_BRANCHING_FACTOR + 1) * PTR_SIZE;


/// 这留下了 2476 个字节给中间节点的键:
/// 我们用 2388 字节给键并且将剩下的88字节视为垃圾.
/// 这意味着每个键被限制为 12 字节. (2476 / keys limit(199) ~= 12)
/// 向下取整到 10 来容纳叶子节点.
const INTERNAL_NODE_KEY_OFFSET: usize = INTERNAL_NODE_CHILDREN_OFFSET + MAX_SPACE_FOR_CHILDREN;
const MAX_SPACE_FOR_KEYS: usize = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE - MAX_SPACE_FOR_CHILDREN;

/// 键和值的大小
const KEY_SIZE: usize = 10;
const VALUE_SIZE: usize = 10;

#[derive(PartialEq)]
pub enum NodeType {
    Internal = 1,
    Leaf = 2,
    Unknown,
}

/// 将一个字节转换成 NodeType.
impl From<u8> for NodeType {
    fn from(orig: u8) -> Self {
        return match orig {
            0x01 => NodeType::Internal,
            0x02 => NodeType::Leaf,
            _ => NodeType::Unknown,
        };
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
        return match self {
            0x01 => true,
            _ => false,
        };
    }
}

impl ToByte for bool {
    fn to_byte(&self) -> u8 {
        return match self {
            true => 0x01,
            false => 0x00,
        };
    }
}

/// Node 代表一个页中的B+树的一个节点
pub struct Node {
    pub node_type: NodeType,
    pub parent_offset: usize,
    pub is_root: bool,
    pub valid: bool,
    pub offset: usize,
    pub page: Page,
}

impl Node {
    /// todo 写入配置信息
    pub fn new(
        node_type: NodeType,
        parent_offset: usize,
        offset: usize,
        is_root: bool,
        page: Page,
        valid: bool,
    ) -> Node {
        Node {
            node_type,
            parent_offset,
            offset,
            is_root,
            page,
            valid,
        }
    }

    /// get_key_value_pairs 如果是叶子节点，返回一个KeyValuePair的列表，
    /// 否则返回一个Error
    pub fn get_key_value_pairs(&self) -> Result<Vec<KeyValuePair>, Error> {
        return match self.node_type {
            NodeType::Leaf => {
                let mut res = Vec::<KeyValuePair>::new();
                let mut offset = VALID_OFFSET;
                let valid = self.page.get_ptr_from_offset(offset, VALID_SIZE)[0].from_byte();
                if valid != true {
                    return Err(Error::UnexpectedError);
                }
                offset = LEAF_NODE_NUM_PAIRS_OFFSET;
                let num_keys_val_pairs = self.page.get_value_from_offset(offset)?;

                offset = LEAF_NODE_HEADER_SIZE;

                for _i in 0..num_keys_val_pairs {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    offset += KEY_SIZE;

                    let value_raw = self.page.get_ptr_from_offset(offset, VALUE_SIZE);
                    let value = match str::from_utf8(value_raw) {
                        Ok(val) => val,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    offset += VALUE_SIZE;

                    // Trim leading or trailing zeros.
                    res.push(KeyValuePair::new(
                        key.trim_matches(char::from(0)).to_string(),
                        value.trim_matches(char::from(0)).to_string(),
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
        return match self.node_type {
            NodeType::Internal => {
                let num_children = self
                    .page
                    .get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let mut result = Vec::<usize>::new();
                let mut offset = INTERNAL_NODE_HEADER_SIZE;
                for _i in 1..=num_children {
                    let child_offset = self.page.get_value_from_offset(offset)?;
                    result.push(child_offset);
                    offset += PTR_SIZE;
                }
                Ok(result)
            }
            _ => Err(Error::UnexpectedError),
        };
    }

    /// get_keys 返回一个包装有 Key 列表的 Result
    pub fn get_keys(&self) -> Result<Vec<String>, Error> {
        return match self.node_type {
            NodeType::Internal => {
                let num_children = self
                    .page
                    .get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let mut result = Vec::<String>::new();
                let mut offset = INTERNAL_NODE_HEADER_SIZE + num_children * PTR_SIZE;
                // 键数总是会比儿子数少一
                let num_keys = num_children - 1;
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
                let num_keys_val_pairs = self
                    .page
                    .get_value_from_offset(LEAF_NODE_NUM_PAIRS_OFFSET)?;
                let offset = LEAF_NODE_HEADER_SIZE + (KEY_SIZE + VALUE_SIZE) * num_keys_val_pairs;
                // Update number of key value pairs.
                self.page
                    .write_value_at_offset(LEAF_NODE_NUM_PAIRS_OFFSET, num_keys_val_pairs + 1)?;
                // Write the key.
                let key_raw = kv.key.as_bytes();
                self.page.write_bytes_at_offset(key_raw, offset, KEY_SIZE)?;
                // Write the value.
                let value_raw = kv.value.as_bytes();
                self.page
                    .write_bytes_at_offset(value_raw, offset + KEY_SIZE, VALUE_SIZE)?;
                Ok(())
            }
            _ => return Err(Error::UnexpectedError),
        }
    }

    /// 增加一个键, 和该键的右子节点
    /// 只应当在中间节点上使用.
    pub fn add_key_and_left_child(&mut self, key: String, left_child_offset: usize) -> Result<(), Error> {
        return match self.node_type {
            NodeType::Internal => {
                let num_children = self
                    .page
                    .get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let mut offset = INTERNAL_NODE_KEY_OFFSET;
                // 更新孩子数 (等于键数+1)
                self.page
                    .write_value_at_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET, num_children + 1)?;
                // 寻找新键的位置.
                let num_keys = num_children - 1;
                let end_key_data = offset + num_keys * KEY_SIZE;
                for i in 0..num_keys {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let iter_key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    if iter_key.to_owned() > key {
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
            NodeType::Internal => {
                let num_children = self
                    .page
                    .get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let num_keys = num_children - 1;
                Ok(num_keys)
            }
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
            _ => return Err(Error::KeyNotFound),
        }
    }

    pub fn update_internal_key(&mut self, old_key: &String, new_key: &String) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal => {
                let num_children = self
                    .page
                    .get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let mut result = Vec::<String>::new();
                let mut offset = INTERNAL_NODE_HEADER_SIZE + num_children * PTR_SIZE;
                // 键数总是会比儿子数少一
                let num_keys = num_children - 1;
                for _i in 1..=num_keys {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    if key == old_key {
                        return self.page.write_bytes_at_offset(new_key.trim_matches(char::from(0)).as_bytes(), offset, KEY_SIZE)
                    }
                    offset += KEY_SIZE;
                    // 去掉首尾 \0 字符
                    result.push(key.trim_matches(char::from(0)).to_string());
                }
                Err(Error::KeyNotFound)
            }
            _ => return Err(Error::UnexpectedError)
        }
    }

    /// update_value 更新当前节点中包含键的键值对.
    pub fn update_value(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        match self.node_type {
            NodeType::Leaf => {
                let mut offset = VALID_OFFSET;
                let valid = self.page.get_ptr_from_offset(offset, VALID_SIZE)[0].from_byte();
                match valid {
                    true => (),
                    _ => return Err(Error::UnexpectedError)
                }
                offset = LEAF_NODE_NUM_PAIRS_OFFSET;
                let num_keys_val_pairs = self.page.get_value_from_offset(offset)?;

                offset = LEAF_NODE_HEADER_SIZE;

                for _i in 0..num_keys_val_pairs {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    offset += KEY_SIZE;
                    if key == kv.key {
                        let value_raw = kv.value.as_bytes();
                        self.page
                            .write_bytes_at_offset(value_raw, offset, VALUE_SIZE)?;
                        return Ok(());
                    }
                    offset += VALUE_SIZE;
                }
                Err(Error::KeyNotFound)
            }
            _ => return Err(Error::KeyNotFound),
        }
    }

    // 向key和children数量一样的节点加一个child
    fn add_child(&mut self, child_offset: usize) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal => {
                let child_num = self.page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let key_num = self.page.get_value_from_offset(INTERNAL_NODE_KEY_OFFSET)?;
                if key_num < child_num {
                    return Err(Error::UnexpectedError);
                }
                self.page.write_bytes_at_offset(&(child_num + 1).to_be_bytes(), INTERNAL_NODE_CHILDREN_OFFSET, INTERNAL_NODE_NUM_CHILDREN_SIZE)?;
                let offset = INTERNAL_NODE_CHILDREN_OFFSET + PTR_SIZE * child_num;
                self.page.write_bytes_at_offset(&child_offset.to_be_bytes(), offset, PTR_SIZE)?;
                Ok(())
            }
            _ => return Err(Error::UnexpectedError)
        }
    }

    /// 将当前节点分裂成两个节点，并返回中介节点的键和两个节点
    pub fn split(&mut self, pager: &mut Pager) -> Result<(), Error> {
        if self.is_root {
            let num_children = self
                .page
                .get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
            let mut offset = INTERNAL_NODE_KEY_OFFSET;
            let num_key = num_children - 1;
            let children = self.get_children()?;
            let split_node_num_key = num_key / 2;
            let left_page = pager.get_new_page()?;
            let mut left_node = Node::new(NodeType::Internal,
                                          self.parent_offset, left_page.page_num, false, left_page, true);
            let right_page = pager.get_new_page()?;
            let mut right_node = Node::new(NodeType::Internal,
                                           self.parent_offset, right_page.page_num, false, right_page, true);

            for i in 1..split_node_num_key {
                let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                let child_offset = children.get(i - 1).unwrap();
                let key = match str::from_utf8(key_raw) {
                    Ok(key) => key,
                    Err(_) => return Err(Error::UTF8Error),
                };
                left_node.add_key_and_left_child(String::from(key), *child_offset)?;
                offset += KEY_SIZE;
            }

            offset += KEY_SIZE;
            let median_offset = children.get(split_node_num_key).unwrap();
            left_node.add_child(*median_offset)?;

            // 清空当前节点孩子，并重新插入
            offset = INTERNAL_NODE_NUM_CHILDREN_OFFSET;
            self.page.write_bytes_at_offset(&[0], offset, INTERNAL_NODE_NUM_CHILDREN_SIZE)?;

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
            let child_offset = children.get(num_key).unwrap();
            right_node.add_child(*child_offset)?;

            self.page.write_bytes_at_offset(&(2 as usize).to_be_bytes(), INTERNAL_NODE_NUM_CHILDREN_OFFSET, INTERNAL_NODE_NUM_CHILDREN_SIZE)?;

            offset = INTERNAL_NODE_CHILDREN_OFFSET;
            self.page.write_bytes_at_offset(&left_node.offset.to_be_bytes(), offset, PTR_SIZE)?;
            self.page.write_bytes_at_offset(&right_node.offset.to_be_bytes(), offset, PTR_SIZE)?;

            offset = INTERNAL_NODE_KEY_OFFSET;

            let median_key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
            let median_key = match str::from_utf8(median_key_raw) {
                Ok(key) => String::from(key),
                Err(_) => return Err(Error::UTF8Error),
            };
            self.page.write_bytes_at_offset(median_key.as_bytes(), offset, KEY_SIZE)?;
            return Ok(())
        }
        match self.node_type {
            NodeType::Internal => {
                let num_children = self.page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let mut offset = INTERNAL_NODE_KEY_OFFSET;
                let num_key = num_children - 1;
                let children = self.get_children()?;
                let split_node_num_key = num_key / 2;
                let left_page = pager.get_new_page()?;
                let mut left_node = Node::new(NodeType::Internal, self.parent_offset, left_page.page_num, false, left_page, true);


                for i in 1..split_node_num_key {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let child_offset = children.get(i - 1).unwrap();
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    left_node.add_key_and_left_child(String::from(key), *child_offset)?;
                    offset += KEY_SIZE;
                }

                let median_key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                let median_key = match str::from_utf8(median_key_raw) {
                    Ok(key) => String::from(key),
                    Err(_) => return Err(Error::UTF8Error),
                };
                offset += KEY_SIZE;
                let median_offset = children.get(split_node_num_key).unwrap();
                left_node.add_child(*median_offset)?;

                // 清空当前节点孩子，并重新插入
                offset = INTERNAL_NODE_NUM_CHILDREN_OFFSET;
                self.page.write_bytes_at_offset(&[0], INTERNAL_NODE_NUM_CHILDREN_OFFSET, INTERNAL_NODE_NUM_CHILDREN_SIZE)?;
                self.page.write_bytes_at_offset(&[0], INTERNAL_NODE_KEY_OFFSET, KEY_SIZE)?;

                for i in split_node_num_key + 1..num_key {
                    let key_raw = self.page.get_ptr_from_offset(offset, KEY_SIZE);
                    let child_offset = children.get(i).unwrap();
                    let key = match str::from_utf8(key_raw) {
                        Ok(key) => String::from(key),
                        Err(_) => return Err(Error::UTF8Error),
                    };
                    self.add_key_and_left_child(String::from(key), *child_offset)?;
                    offset += KEY_SIZE;
                }
                let child_offset = children.get(num_key).unwrap();
                self.add_child(*child_offset)?;

                let parent_offset = self.parent_offset;
                let page_num = parent_offset / PAGE_SIZE;
                let lock = Arc::new(RwLock::new(
                    Node::try_from(NodeSpec {
                        page_data: pager.get_page(self.page.file_name.as_str(), &page_num).unwrap().get_data(),
                        offset: parent_offset,
                    })?));
                let mut parent_node = match lock.write() {
                    Err(_) => return Err(Error::UnexpectedError),
                    Ok(node) => node,
                };
                parent_node.add_key_and_left_child(String::from(median_key), left_node.offset)?;
                Ok(())
            }
            NodeType::Leaf => {
                let mut kv_pairs = self.get_key_value_pairs()?;
                let left_leaf_page = pager.get_new_page()?;
                let mut left_leaf = Node::new(NodeType::Leaf, self.parent_offset, left_leaf_page.page_num, false, left_leaf_page, true);

                let mut right_kv_pairs = Vec::<KeyValuePair>::new();

                kv_pairs.sort();
                let mid = kv_pairs.len() / 2;
                for (i, kv) in kv_pairs.iter_mut().enumerate() {
                    if i < mid {
                        left_leaf.add_key_value_pair(kv.clone())?
                    } else {
                        right_kv_pairs.push(kv.clone());
                    }
                }

                let parent_offset = self.parent_offset;
                let page_num = parent_offset / PAGE_SIZE;
                let lock_parent_node = Arc::new(RwLock::new(
                    Node::try_from(NodeSpec {
                        page_data: pager.get_page(self.page.file_name.as_str(), &page_num).unwrap().get_data(),
                        offset: parent_offset,
                    })?));
                let mut parent_node = match lock_parent_node.write() {
                    Err(_) => return Err(Error::UnexpectedError),
                    Ok(node) => node,
                };
                parent_node.add_key_and_left_child(kv_pairs.get(mid).unwrap().key.clone(), left_leaf.offset)?;

                self.page.write_bytes_at_offset(&right_kv_pairs.len().to_be_bytes(), LEAF_NODE_NUM_PAIRS_OFFSET, LEAF_NODE_NUM_PAIRS_SIZE)?;
                for i in right_kv_pairs {
                    self.add_key_value_pair(i.clone())?;
                }
                Ok(())
            }
            NodeType::Unknown => Err(Error::UnexpectedError),
        }
    }

    /// 将叶子节点的有效位置零
    /// 非叶子节点抛出异常
    pub fn delete(&mut self) -> Result<(), Error> {
        return match self.node_type {
            NodeType::Leaf => {
                let offset = VALID_OFFSET;
                self.page.write_bytes_at_offset(&[true.to_byte()], offset, VALID_SIZE)
            }
            _ => Err(Error::UnexpectedError)
        };
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

/// NodeSpec 是一个包装。通过 TryFrom 将一个页的字节数组转换成 Node struct 来实现.
pub struct NodeSpec {
    pub page_data: [u8; PAGE_SIZE],
    pub offset: usize,
}

impl TryFrom<NodeSpec> for Node {
    type Error = Error;
    fn try_from(spec: NodeSpec) -> Result<Self, Self::Error> {
        let page = Page::new_phantom(spec.page_data);
        let is_root = spec.page_data[IS_ROOT_OFFSET].from_byte();
        let valid = spec.page_data[VALID_OFFSET].from_byte();
        let node_type = NodeType::from(spec.page_data[NODE_TYPE_OFFSET]);
        if node_type == NodeType::Unknown {
            return Err(Error::UnexpectedError);
        }
        let parent_pointer_offset = page.get_value_from_offset(PARENT_POINTER_OFFSET)?;

        return Ok(Node::new(
            node_type,
            parent_pointer_offset,
            spec.offset,
            is_root,
            page,
            valid,
        ));
    }
}

// #[cfg(test)]
// mod tests {
//     use std::convert::TryFrom;
//
//     use crate::index::node::{INTERNAL_NODE_HEADER_SIZE, KEY_SIZE, LEAF_NODE_HEADER_SIZE, Node, NodeSpec, VALUE_SIZE};
//     use crate::page::page_item::{PAGE_SIZE, PTR_SIZE};
//     use crate::util::error::Error;
//
//     #[test]
//     fn page_to_node_works() -> Result<(), Error> {
//         const DATA_LEN: usize = LEAF_NODE_HEADER_SIZE + KEY_SIZE + VALUE_SIZE;
//         let page_data: [u8; DATA_LEN] = [
//             0x01, // Is-Root byte.
//             0x02, // Node type byte.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Parent offset.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Number of Key-Value pairs.
//             0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
//             0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
//         ];
//         let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
//         let mut page = [0x00; PAGE_SIZE];
//         for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
//             *to = *from
//         }
//
//         let offset = PAGE_SIZE * 2;
//         let node = Node::try_from(NodeSpec {
//             offset,
//             page_data: page,
//         })?;
//
//         assert_eq!(node.is_root, true);
//         assert_eq!(node.parent_offset, 0);
//
//         Ok(())
//     }
//
//     #[test]
//     fn get_key_value_pairs_works() -> Result<(), Error> {
//         const DATA_LEN: usize = LEAF_NODE_HEADER_SIZE + KEY_SIZE + VALUE_SIZE;
//         let page_data: [u8; DATA_LEN] = [
//             0x01, // Is-Root byte.
//             0x02, // Node type byte.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Parent offset.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Number of Key-Value pairs.
//             0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
//             0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
//         ];
//         let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
//         let mut page = [0x00; PAGE_SIZE];
//         for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
//             *to = *from
//         }
//
//         let offset = PAGE_SIZE * 2;
//         let node = Node::try_from(NodeSpec {
//             offset,
//             page_data: page,
//         })?;
//         let kv = node.get_key_value_pairs()?;
//
//         assert_eq!(kv.len(), 1);
//         let first_kv = match kv.get(0) {
//             Some(kv) => kv,
//             None => return Err(Error::UnexpectedError),
//         };
//
//         assert_eq!(first_kv.key, "hello");
//         assert_eq!(first_kv.value, "world");
//
//         Ok(())
//     }
//
//     #[test]
//     fn get_children_works() -> Result<(), Error> {
//         const DATA_LEN: usize = INTERNAL_NODE_HEADER_SIZE + 3 * PTR_SIZE + 2 * KEY_SIZE;
//         let page_data: [u8; DATA_LEN] = [
//             0x01, // Is-Root byte.
//             0x01, // Node type byte.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Parent offset.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // Number of children.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096  (2nd Page)
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, // 8192  (3rd Page)
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, // 12288 (4th Page)
//             0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
//             0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
//         ];
//         let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
//
//         // Concatenate the two arrays; page_data and junk.
//         let mut page = [0x00; PAGE_SIZE];
//         for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
//             *to = *from
//         }
//
//         let offset = 0;
//         let node = Node::try_from(NodeSpec {
//             offset,
//             page_data: page,
//         })?;
//         let children = node.get_children()?;
//
//         assert_eq!(children.len(), 3);
//         for (i, child) in children.iter().enumerate() {
//             assert_eq!(*child, PAGE_SIZE * (i + 1));
//         }
//
//         Ok(())
//     }
//
//     #[test]
//     fn get_keys_work_for_internal_node() -> Result<(), Error> {
//         const DATA_LEN: usize = INTERNAL_NODE_HEADER_SIZE + 3 * PTR_SIZE + 2 * KEY_SIZE;
//         let page_data: [u8; DATA_LEN] = [
//             0x01, // Is-Root byte.
//             0x01, // Node type byte.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Parent offset.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // Number of children.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096  (2nd Page)
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, // 8192  (3rd Page)
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, // 12288 (4th Page)
//             0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
//             0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
//         ];
//         let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
//
//         // Concatenate the two arrays; page_data and junk.
//         let mut page = [0x00; PAGE_SIZE];
//         for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
//             *to = *from
//         }
//
//         let offset = 0;
//         let node = Node::try_from(NodeSpec {
//             offset,
//             page_data: page,
//         })?;
//         let keys = node.get_keys()?;
//         assert_eq!(keys.len(), 2);
//
//         let first_key = match keys.get(0) {
//             Some(key) => key,
//             None => return Err(Error::UnexpectedError),
//         };
//         assert_eq!(first_key, "hello");
//
//         let second_key = match keys.get(1) {
//             Some(key) => key,
//             None => return Err(Error::UnexpectedError),
//         };
//         assert_eq!(second_key, "world");
//
//         Ok(())
//     }
//
//     #[test]
//     fn get_keys_work_for_leaf_node() -> Result<(), Error> {
//         const DATA_LEN: usize = INTERNAL_NODE_HEADER_SIZE + 2 * KEY_SIZE + 2 * VALUE_SIZE;
//         let page_data: [u8; DATA_LEN] = [
//             0x01, // Is-Root byte.
//             0x02, // Node type byte.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Parent offset.
//             0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // Number of Key-Value pairs.
//             0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
//             0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
//             0x66, 0x6f, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // "foo"
//             0x62, 0x61, 0x72, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // "bar"
//         ];
//
//         let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
//
//         // Concatenate the two arrays; page_data and junk.
//         let mut page = [0x00; PAGE_SIZE];
//         for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
//             *to = *from
//         }
//
//         let offset = 0;
//         let node = Node::try_from(NodeSpec {
//             offset,
//             page_data: page,
//         })?;
//
//         let keys = node.get_keys()?;
//         assert_eq!(keys.len(), 2);
//
//         let first_key = match keys.get(0) {
//             Some(key) => key,
//             None => return Err(Error::UnexpectedError),
//         };
//         assert_eq!(first_key, "hello");
//
//         let second_key = match keys.get(1) {
//             Some(key) => key,
//             None => return Err(Error::UnexpectedError),
//         };
//         assert_eq!(second_key, "foo");
//
//         Ok(())
//     }
// }