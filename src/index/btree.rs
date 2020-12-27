use std::convert::TryFrom;
use std::sync::{Arc, RwLock};

use crate::index::key_value_pair::KeyValuePair;
use crate::index::node::{Node, NodeSpec, NodeType, LEAF_NODE_NEXT_NODE_PTR_OFFSET, LEAF_NODE_PREVIOUS_NODE_PTR_OFFSET};
use crate::page::page::{Page, PAGE_SIZE};
use crate::page::pager::Pager;
use crate::util::error::Error;
use crate::data_item::buffer::Buffer;

/// B+树 配置
pub const MAX_BRANCHING_FACTOR: usize = 200;
pub const MIN_BRANCHING_FACTOR: usize = 100;
pub const NODE_KEYS_LIMIT: usize = MAX_BRANCHING_FACTOR - 1;

/// B+树的定义
pub struct BTree {
    file_name: String,
    root: Arc<RwLock<Node>>,
    pub(crate) pager: Box<Pager>,
    first_offset: usize,
}

impl Clone for BTree {
    fn clone(&self) -> Self {
        Self {
            file_name: self.file_name.clone(),
            root: Arc::clone(&self.root),
            pager: self.pager.clone(),
            first_offset: self.first_offset,
        }
    }
}

impl BTree {
    pub(crate) fn new(mut pager: Box<Pager>, file_name: String, buffer: &mut Box<dyn Buffer>) -> Result<BTree, Error> {
        let page = pager.get_new_page(buffer)?;
        let page_num = page.page_num;
        let root =
            Arc::new(
                RwLock::new(
                    Node::new(
                        NodeType::Leaf,
                        0,
                        page_num,
                        true,
                        page,
                    )?
                )
            );

        Ok(BTree {
            file_name,
            pager,
            root,
            first_offset: page_num,
        })
    }

    /// 在树上查询一个键
    pub fn search(&self, key: String, buffer: &mut Box<dyn Buffer>) -> Result<KeyValuePair, Error> {
        let (_, kv) = self.search_node(Arc::clone(&self.root), &key, buffer)?;
        match kv {
            Some(kv) => Ok(kv),
            None => Err(Error::KeyNotFound),
        }
    }

    /// 在树上查询一个两个键之间的所有节点
    pub fn search_range(&self, raw_left_key: Option<String>, raw_right_key: Option<String>, buffer: &mut Box<dyn Buffer>) -> Result<Vec<KeyValuePair>, Error> {
        match raw_left_key {
            Some(left_key) => {
                let (node, raw_kv) = self.search_node(Arc::clone(&self.root), &left_key, buffer)?;
                let mut res = Vec::<KeyValuePair>::new();
                match raw_kv {
                    Some(kv) => kv,
                    None => return Err(Error::KeyNotFound),
                };
                let read_node = match node.read() {
                    Ok(rn) => rn,
                    _ => return Err(Error::UnexpectedError)
                };
                let mut next_node_offset = read_node.offset;
                let mut right_key = "".to_string();
                let has_right_key = match raw_right_key {
                    Some(right_key_data) => {
                        right_key = right_key_data;
                        true
                    }
                    None => false
                };
                while next_node_offset != 0 {
                    let page_num = next_node_offset;
                    let new_node =
                        Arc::new(
                            RwLock::new(
                                Node::try_from(
                                    NodeSpec {
                                        page_data: self.pager.get_page(&page_num, buffer).unwrap().get_data(),
                                        offset: next_node_offset,
                                    }
                                )?
                            )
                        );
                    let read_node = match new_node.read() {
                        Ok(rn ) => rn,
                        _ => return Err(Error::UnexpectedError)
                    };
                    next_node_offset = read_node.page.get_value_from_offset(LEAF_NODE_NEXT_NODE_PTR_OFFSET)?;
                    let mut ok = false;
                    if has_right_key {
                        for i in read_node.get_keys()? {
                            if i.trim() == right_key.trim() {
                                ok = true;
                                break;
                            }
                        }
                    }
                    if ok {
                        let mut kv_pairs = read_node.get_key_value_pairs()?;
                        kv_pairs.sort();

                        for i in kv_pairs {
                            if i.key.trim() <= right_key.trim() {
                                res.push(i);
                            } else {
                                break;
                            }
                        }
                        break;
                    } else {
                        for i in read_node.get_key_value_pairs()? {
                            res.push(i);
                        }
                    }
                }
                Ok(res)
            }
            None => {
                match raw_right_key {
                    Some(right_key) => {
                        let (node, raw_kv) = self.search_node(Arc::clone(&self.root), &right_key, buffer)?;
                        match raw_kv {
                            Some(kv) => kv,
                            None => return Err(Error::KeyNotFound),
                        };
                        let read_node = match node.read() {
                            Ok(rn) => rn,
                            _ => return Err(Error::UnexpectedError)
                        };
                        let mut res = Vec::<KeyValuePair>::new();
                        let mut next_node_offset = read_node.offset;
                        while next_node_offset != 0 {
                            let page_num = next_node_offset;
                            let new_node =
                                Arc::new(
                                    RwLock::new(
                                        Node::try_from(
                                            NodeSpec {
                                                page_data: self.pager.get_page(&page_num, buffer).unwrap().get_data(),
                                                offset: next_node_offset,
                                            }
                                        )?
                                    )
                                );
                            let read_node = match new_node.read() {
                                Ok(rn) => rn,
                                _ => return Err(Error::UnexpectedError)
                            };
                            next_node_offset = read_node.page.get_value_from_offset(LEAF_NODE_PREVIOUS_NODE_PTR_OFFSET)?;
                            for i in read_node.get_key_value_pairs()? {
                                res.push(i);
                            }
                        }
                        Ok(res)
                    }
                    None => {
                        let mut res = Vec::<KeyValuePair>::new();
                        if self.first_offset == 0 {
                            return Ok(res);
                        }
                        let mut next_node_offset = self.first_offset;
                        while next_node_offset != 0 {
                            let page_num = next_node_offset;
                            let new_node =
                                Arc::new(
                                    RwLock::new(
                                        Node::try_from(
                                            NodeSpec {
                                                page_data: self.pager.get_page(&page_num, buffer).unwrap().get_data(),
                                                offset: next_node_offset,
                                            }
                                        )?
                                    )
                                );
                            let read_node = match new_node.read() {
                                Ok(rn ) => rn,
                                _ => return Err(Error::UnexpectedError)
                            };
                            next_node_offset = read_node.page.get_value_from_offset(LEAF_NODE_NEXT_NODE_PTR_OFFSET)?;
                            for i in read_node.get_key_value_pairs()? {
                                res.push(i);
                            }
                        }
                        Ok(res)
                    }
                }
            }
        }
    }


    /// 插入一个键值对，可能沿途分裂节点
    pub fn insert(&mut self, kv: KeyValuePair, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        let (node, kv_pair_exists) = self.search_node_inserted(Arc::clone(&self.root), &kv.key, buffer)?;
        if kv_pair_exists.is_some() {
            return Err(Error::KeyAlreadyExists)
        };
        // 在这里加键可能会沿途分裂节点
        let mut guarded_node = match node.write() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };
        let keys_len = guarded_node.get_keys_len()?;
        if keys_len < NODE_KEYS_LIMIT {
            // 向叶子节点插入键值对.
            guarded_node.add_key_value_pair(kv)?;
            // 将对应页写入磁盘.
            return self
                .pager.as_mut()
                .write_page(Page::new(guarded_node.page.get_data(), &guarded_node.page.file_name, guarded_node.page.page_num), buffer);
        }
        self.split_node(Arc::clone(&node), buffer)
    }


    /// 将key所对应的值更新为value
    pub fn update(&mut self, kv: KeyValuePair, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        let (node, kv_pair_exists) = self.search_node(Arc::clone(&self.root), &kv.key, buffer)?;
        match kv_pair_exists {
            None => return Err(Error::KeyNotFound),
            Some(_) => ()
        }
        let mut guarded_node = match node.write() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node
        };
        guarded_node.update_value(kv)
    }

    /// 查找并删除满足key的叶子节点
    pub fn delete(&mut self, key: String, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        let (node, kv_pair_exists) = self.search_node(Arc::clone(&self.root), &key, buffer)?;
        match kv_pair_exists {
            None => return Err(Error::KeyNotFound),
            Some(_) => ()
        }
        let mut guarded_node = match node.write() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node
        };
        guarded_node.delete()
    }

    /// search_node 以当前节点为根的子树递归查询一个键
    /// 使用 pager 来获取页来遍历子树
    /// 如果遍历了所有的叶子节点，还没有找到对应的键
    /// 返回叶子节点和空来表示没找到
    /// 否则，继续递归或者返回合适的错误
    /// inserted字段控制在找不到合适节点时是否插入新节点并返回
    fn search_node(
        &self,
        node: Arc<RwLock<Node>>,
        search_key: &str,
        buffer: &mut Box<dyn Buffer>,
    ) -> Result<(Arc<RwLock<Node>>, Option<KeyValuePair>), Error> {

        // 获取待查询子树的读权限
        let guarded_node = match node.read() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };

        // 分派节点类型
        match guarded_node.node_type {

            // 对于叶子节点
            // 获取叶子的所有的键
            // 然后匹配这些键
            NodeType::Leaf => {
                let keys = guarded_node.get_keys()?;
                for (i, key) in keys.iter().enumerate() {
                    if *key == *search_key {
                        let kv_pairs = guarded_node.get_key_value_pairs()?;
                        return match kv_pairs.get(i) {
                            None => Ok((Arc::clone(&node), None)),
                            Some(kv) => Ok((Arc::clone(&node), Some(kv.clone()))),
                        };
                    }
                }
                Ok((Arc::clone(&node), None))
            }

            // 对于中间节点
            // 获取节点所有的键
            // 找到第一个比待查询键大的键
            // 若找到，获取键左边的儿子，并递归查询
            // 若找不到，且需要插入，则扩大最后一个键，并递归插入
            NodeType::Internal => {
                let keys = guarded_node.get_keys()?;
                let mut index: Option<usize> = None;
                for (i, key) in keys.iter().enumerate() {
                    if *search_key <= *key.as_str() {
                        index = Some(i);
                        break;
                    }
                };

                match index {
                    Some(i) => {
                        let children_ptrs = guarded_node.get_children()?;
                        let child_offset = match children_ptrs.get(i) {
                            None => return Err(Error::UnexpectedError),
                            Some(child_offset) => child_offset,
                        };
                        let page_num = child_offset / PAGE_SIZE;
                        let child_node = Node::try_from(NodeSpec {
                            offset: *child_offset,
                            page_data: self.pager.get_page(&page_num, buffer)?.get_data(),
                        })?;
                        self.search_node(Arc::new(RwLock::new(child_node)), search_key, buffer)
                    }
                    None => Err(Error::KeyNotFound)
                }
            }
            NodeType::Unknown => {
                Err(Error::UnexpectedError)
            }
        }
    }

    /// search_node 以当前节点为根的子树递归查询一个键
    /// 使用 pager 来获取页来遍历子树
    /// 如果遍历了所有的叶子节点，还没有找到对应的键
    /// 返回叶子节点和空来表示没找到
    /// 否则，继续递归或者返回合适的错误
    /// inserted字段控制在找不到合适节点时是否插入新节点并返回
    fn search_node_inserted(
        &mut self,
        node: Arc<RwLock<Node>>,
        search_key: &str,
        buffer: &mut Box<dyn Buffer>,
    ) -> Result<(Arc<RwLock<Node>>, Option<KeyValuePair>), Error> {

        // 获取待查询子树的读权限
        let guarded_node = match node.read() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };

        // 分派节点类型
        match guarded_node.node_type {

            // 对于叶子节点
            // 获取叶子的所有的键
            // 然后匹配这些键
            NodeType::Leaf => {
                let keys = guarded_node.get_keys()?;
                for (i, key) in keys.iter().enumerate() {
                    if *key == *search_key {
                        let kv_pairs = guarded_node.get_key_value_pairs()?;
                        return match kv_pairs.get(i) {
                            None => Ok((Arc::clone(&node), None)),
                            Some(kv) => Ok((Arc::clone(&node), Some(kv.clone()))),
                        };
                    }
                }
                Ok((Arc::clone(&node), None))
            }

            // 对于中间节点
            // 获取节点所有的键
            // 找到第一个比待查询键大的键
            // 若找到，获取键左边的儿子，并递归查询
            // 若找不到，且需要插入，则扩大最后一个键，并递归插入
            NodeType::Internal => {
                let keys = guarded_node.get_keys()?;
                let mut index: Option<usize> = None;
                for (i, key) in keys.iter().enumerate() {
                    if *search_key <= *key.as_str() {
                        index = Some(i);
                        break;
                    }
                };

                return match index {
                    Some(i) => {
                        let children_ptrs = guarded_node.get_children()?;
                        let child_offset = match children_ptrs.get(i) {
                            None => return Err(Error::UnexpectedError),
                            Some(child_offset) => child_offset,
                        };
                        let page_num = child_offset / PAGE_SIZE;
                        let child_node = Node::try_from(NodeSpec {
                            offset: *child_offset,
                            page_data: self.pager.as_mut().get_page(&page_num, buffer)?.get_data(),
                        })?;
                        self.search_node_inserted(Arc::new(RwLock::new(child_node)), search_key, buffer)
                    }
                    None => {
                        // 获取最后一个键用于插入
                        let last_key = keys.last();

                        match last_key {
                            Some(last_key) => {
                                //获取写权限
                                let mut write_node = match node.write() {
                                    Err(_) => return Err(Error::UnexpectedError),
                                    Ok(node) => node
                                };

                                // 更新最后一个键
                                write_node.update_internal_key(last_key, search_key)?;

                                // 获取最后一个儿子
                                let children_ptrs = write_node.get_children()?;
                                let child_offset = match children_ptrs.last() {
                                    None => return Err(Error::UnexpectedError),
                                    Some(child_offset) => child_offset,
                                };
                                let pager = self.pager.as_mut();
                                let page_num = child_offset / PAGE_SIZE;
                                let child_node = Node::try_from(NodeSpec {
                                    offset: *child_offset,
                                    page_data: pager.get_page(&page_num, buffer)?.get_data(),
                                })?;

                                // 查询最后一个儿子， 实际上这里会导致递归插入
                                self.search_node_inserted(Arc::new(RwLock::new(child_node)), search_key, buffer)
                            }
                            None => Err(Error::UnexpectedError)
                        }
                    }
                };
            }
            NodeType::Unknown => {
                Err(Error::UnexpectedError)
            }
        }
    }

    /// 沿当前节点向上检查所有的节点是否超过最大节点数
    /// 若超过，则分裂
    fn split_node(&mut self, node: Arc<RwLock<Node>>, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {

        // 获取写权限
        let mut guarded_node = match node.write() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };

        if guarded_node.is_root {
            // 如果是根节点，直接分裂
            let (is_split, offset) = guarded_node.split(&mut self.pager, buffer)?;
            if guarded_node.offset == self.first_offset && is_split {
                self.first_offset = offset;
            }
            Ok(())
        } else {
            // 如果当前节点分裂，则父节点也可能需要分裂
            let (is_split, offset) = guarded_node.split(&mut self.pager, buffer)?;
            if is_split {
                if guarded_node.offset == self.first_offset {
                    self.first_offset = offset;
                }
                let page_num = guarded_node.parent_offset / PAGE_SIZE;
                let parent_node =
                    Arc::new(
                        RwLock::new(
                            Node::try_from(
                                NodeSpec {
                                    page_data: self.pager.get_page(&page_num, buffer).unwrap().get_data(),
                                    offset: guarded_node.parent_offset,
                                }
                            )?
                        )
                    );
                // 递归分裂父节点
                self.split_node(parent_node, buffer)?;

            }
            Ok(())
        }
    }
}
