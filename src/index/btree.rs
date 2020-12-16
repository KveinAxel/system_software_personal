use std::convert::TryFrom;
use std::sync::{Arc, RwLock};

use crate::index::key_value_pair::KeyValuePair;
use crate::index::node::{Node, NodeSpec, NodeType};
use crate::page::page_item::{Page, PAGE_SIZE};
use crate::page::pager::Pager;
use crate::util::error::Error;
use std::borrow::BorrowMut;

/// B+树 配置
pub const MAX_BRANCHING_FACTOR: usize = 200;
pub const MIN_BRANCHING_FACTOR: usize = 100;
pub const NODE_KEYS_LIMIT: usize = MAX_BRANCHING_FACTOR - 1;

/// B+树的定义
pub struct BTree {
    file_name: String,
    root: Arc<RwLock<Node>>,
    pager: Pager,
}

impl BTree {
    fn new(pager: Pager, root: Node, file_name: String) -> BTree {
        BTree {
            file_name,
            pager,
            root: Arc::new(RwLock::new(root)),
        }
    }

    /// 在树上查询一个键
    pub fn search(&mut self, key: String) -> Result<KeyValuePair, Error> {
        let (_, kv) = self.search_node(Arc::clone(&self.root), &key, false)?;
        return match kv {
            Some(kv) => Ok(kv),
            None => Err(Error::KeyNotFound),
        };
    }

    /// 插入一个键值对，可能沿途分裂节点
    pub fn insert(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        let (node, kv_pair_exists) = self.search_node(Arc::clone(&self.root), &kv.key, true)?;
        match kv_pair_exists {
            // 树中已经有键了
            Some(_) => return Err(Error::KeyAlreadyExists),
            None => (),
        };
        // 在这里加键可能会沿途分裂节点
        let mut guarded_node = match node.write() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };
        let keys_len = guarded_node.get_keys_len()?;
        if keys_len < NODE_KEYS_LIMIT {
            // 在内存中的struct中添加键值对.
            guarded_node.add_key_value_pair(kv)?;
            // 将对应页写入磁盘.
            return self
                .pager.borrow_mut()
                .write_page(Page::new(guarded_node.page.get_data(), &guarded_node.page.file_name, guarded_node.page.page_num));
        }
        self.split_node(Arc::clone(&node))
    }


    /// 将key所对应的值更新为value
    pub fn update(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        let (node, kv_pair_exists) = self.search_node( Arc::clone(&self.root), &kv.key, false)?;
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
    pub fn delete(&mut self, key: String) -> Result<(), Error> {
        let (node, kv_pair_exists) = self.search_node(Arc::clone(&self.root), &key, false)?;
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
    fn search_node(
        &mut self,
        node: Arc<RwLock<Node>>,
        search_key: &String,
        inserted: bool,
    ) -> Result<(Arc<RwLock<Node>>, Option<KeyValuePair>), Error> {
        let guarded_node = match node.read() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };

        match guarded_node.node_type {
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
            NodeType::Internal => {
                let keys = guarded_node.get_keys()?;
                let mut prev_key: Option<&String> = None;
                let mut index: Option<usize> = None;
                for (i, key) in keys.iter().enumerate() {
                    match prev_key {
                        Some(p_key) => {
                            if *p_key < *search_key && *search_key <= *key {
                                index = Some(i);
                                break;
                            }
                        }
                        None => {
                            if *search_key <= *key {
                                index = Some(i);
                                break;
                            }
                        }
                    }
                    prev_key = Some(key);
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
                            page_data: self.pager.borrow_mut().get_page(self.file_name.as_str(), &page_num)?.get_data(),
                        })?;
                        return self.search_node(Arc::new(RwLock::new(child_node)), search_key, inserted);
                    }
                    None => {
                        if inserted {
                            return match prev_key {
                                Some(last_key) => {
                                    let mut write_node = match node.write() {
                                        Err(_) => return Err(Error::UnexpectedError),
                                        Ok(node) => node
                                    };
                                    write_node.update_internal_key(last_key, search_key)?;
                                    let children_ptrs = write_node.get_children()?;
                                    let child_offset = match children_ptrs.get(children_ptrs.len() - 1) {
                                        None => return Err(Error::UnexpectedError),
                                        Some(child_offset) => child_offset,
                                    };

                                    let pager = self.pager.borrow_mut();
                                    let page_num = child_offset / PAGE_SIZE;
                                    let child_node = Node::try_from(NodeSpec {
                                        offset: *child_offset,
                                        page_data: pager.get_page(self.file_name.as_str(), &page_num)?.get_data(),
                                    })?;
                                    self.search_node(Arc::new(RwLock::new(child_node)), search_key, inserted)
                                }
                                None => Err(Error::UnexpectedError)
                            }
                        } else {
                            Err(Error::UnexpectedError)
                        }
                    }
                }

            }
            NodeType::Unknown => {
                Err(Error::UnexpectedError)
            }
        }
    }

    /// 沿当前节点向上检查所有的节点是否超过最大节点数
    /// 若超过，则分裂
    fn split_node(&mut self, node: Arc<RwLock<Node>>) -> Result<(), Error> {

        let mut guarded_node = match node.write() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };

        return if guarded_node.is_root {
            // 处理root节点
            guarded_node.split(&mut self.pager)?;
            Ok(())
        } else {
            guarded_node.split(&mut self.pager)?;
            let page_num = guarded_node.parent_offset / PAGE_SIZE;
            let parent_node = Arc::new(RwLock::new(Node::try_from(NodeSpec {
                page_data: self.pager.get_page(self.file_name.as_str(), &page_num).unwrap().get_data(),
                offset: guarded_node.parent_offset,
            })?));
            self.split_node(parent_node)
        }
    }
}