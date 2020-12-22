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
    fn new(mut pager: Pager, file_name: String) -> Result<BTree, Error> {
        let mut page = pager.get_new_page()?;
        let mut root =
            Arc::new(
                RwLock::new(
                    Node::new(
                        NodeType::Internal,
                        0,
                        page.page_num,
                        true,
                        page,
                    )?
                )
            );

        Ok(BTree {
            file_name,
            pager,
            root,
        })
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
            // 向叶子节点插入键值对.
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
        let (node, kv_pair_exists) = self.search_node(Arc::clone(&self.root), &kv.key, false)?;
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
    /// inserted字段控制在找不到合适节点时是否插入新节点并返回
    fn search_node(
        &mut self,
        node: Arc<RwLock<Node>>,
        search_key: &String,
        inserted: bool,
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
                    if *search_key <= *key {
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
                            page_data: self.pager.borrow_mut().get_page(&page_num)?.get_data(),
                        })?;
                        return self.search_node(Arc::new(RwLock::new(child_node)), search_key, inserted);
                    }
                    None => {
                        if inserted {
                            // 获取最后一个键用于插入
                            let last_key = keys.last();

                            return match last_key {
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
                                    let pager = self.pager.borrow_mut();
                                    let page_num = child_offset / PAGE_SIZE;
                                    let child_node = Node::try_from(NodeSpec {
                                        offset: *child_offset,
                                        page_data: pager.get_page(&page_num)?.get_data(),
                                    })?;

                                    // 查询最后一个儿子， 实际上这里会导致递归插入
                                    self.search_node(Arc::new(RwLock::new(child_node)), search_key, inserted)
                                }
                                None => Err(Error::UnexpectedError)
                            };
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

        // 获取写权限
        let mut guarded_node = match node.write() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };

        return if guarded_node.is_root {
            // 如果是根节点，直接分裂
            guarded_node.split(&mut self.pager)?;
            Ok(())
        } else {
            // 如果当前节点分裂，则父节点也可能需要分裂
            if guarded_node.split(&mut self.pager)? {
                let page_num = guarded_node.parent_offset / PAGE_SIZE;
                let parent_node =
                    Arc::new(
                        RwLock::new(
                            Node::try_from(
                                NodeSpec {
                                    page_data: self.pager.get_page(&page_num).unwrap().get_data(),
                                    offset: guarded_node.parent_offset,
                                }
                            )?
                        )
                    );
                // 递归分裂父节点
                self.split_node(parent_node)?;
            }
            Ok(())
        };
    }
}