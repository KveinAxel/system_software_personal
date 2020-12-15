use crate::error::Error;
use crate::index::key_value_pair::KeyValuePair;
use crate::node::{Node, NodeSpec, NodeType};
use crate::page::pager::Pager;
use std::convert::TryFrom;
use std::sync::{Arc, RwLock};
use crate::page::Page;
use crate::index::node::{Node, NodeSpec, NodeType};
use crate::util::error::Error;

/// B+树 配置
pub const MAX_BRANCHING_FACTOR: usize = 200;
pub const MIN_BRANCHING_FACTOR: usize = 100;
pub const NODE_KEYS_LIMIT: usize = MAX_BRANCHING_FACTOR - 1;

/// B+树的定义
pub struct BTree {
    root: Arc<RwLock<Node>>,
    pager: Box<Pager>,
}

impl BTree {
    #[allow(dead_code)]
    fn new(pager: Box<Pager>, root: Node) -> BTree {
        BTree {
            pager,
            root: Arc::new(RwLock::new(root)),
        }
    }

    /// 在树上查询一个键
    pub fn search(&mut self, key: String) -> Result<KeyValuePair, Error> {
        let (_, kv) = self.search_node(Arc::clone(&self.root), &key)?;
        return match kv {
            Some(kv) => Ok(kv),
            None => Err(Error::KeyNotFound),
        };
    }

    /// 插入一个键值对，可能沿途分裂节点
    pub fn insert(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        let (node, kv_pair_exists) = self.search_node(Arc::clone(&self.root), &kv.key)?;
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
                .pager
                .write_page(Page::new(guarded_node.page.get_data()), &guarded_node.offset);
        }
        self.split_node(Arc::clone(&node))?;
        Ok(())
    }


    /// 将key所对应的值更新为value
    pub fn update(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        let (node, kv_pair_exists) = self.search_node(Arc::clone(&self.root), &kv.key)?;
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

    /// search_node 以当前节点为根的子树递归查询一个键
    /// 使用 pager 来获取页来遍历子树
    /// 如果遍历了所有的叶子节点，还没有找到对应的键
    /// 返回叶子节点和空来表示没找到
    /// 否则，继续递归或者返回合适的错误
    fn search_node(
        &mut self,
        node: Arc<RwLock<Node>>,
        search_key: &String,
    ) -> Result<(Arc<RwLock<Node>>, Option<KeyValuePair>), Error> {
        let guarded_node = match node.read() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };
        let keys = guarded_node.get_keys()?;
        for (i, key) in keys.iter().enumerate() {
            if *key == *search_key {
                let kv_pairs = guarded_node.get_key_value_pairs()?;
                return match kv_pairs.get(i) {
                    None => Err(Error::UnexpectedError),
                    Some(kv) => Ok((Arc::clone(&node), Some(kv.clone()))),
                };
            }
            if *key > *search_key {
                return self.traverse_or_return(Arc::clone(&node), i, search_key);
            }
        }
        self.traverse_or_return(Arc::clone(&node), keys.len(), search_key)
    }

    fn traverse_or_return(
        &mut self,
        node: Arc<RwLock<Node>>,
        index: usize,
        search_key: &String,
    ) -> Result<(Arc<RwLock<Node>>, Option<KeyValuePair>), Error> {
        let guarded_node = match node.read() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };
        return match guarded_node.node_type {
            NodeType::Leaf => Ok((Arc::clone(&node), None)),
            NodeType::Internal => {
                let children_ptrs = guarded_node.get_children()?;
                let child_offset = match children_ptrs.get(index) {
                    None => return Err(Error::UnexpectedError),
                    Some(child_offset) => child_offset,
                };
                let child_node = Node::try_from(NodeSpec {
                    offset: *child_offset,
                    page_data: self.pager.get_page(child_offset)?,
                })?;
                self.search_node(Arc::new(RwLock::new(child_node)), search_key)
            }
            NodeType::Unknown => Err(Error::UnexpectedError),
        };
    }

    fn split_node(&mut self, node: Arc<RwLock<Node>>) -> Result<(), Error> {
        let guarded_node = match node.write() {
            Err(_) => return Err(Error::UnexpectedError),
            Ok(node) => node,
        };
        let keys = guarded_node.get_keys()?;
        let mut parent_node = Node::try_from(NodeSpec {
            offset: guarded_node.parent_offset,
            page_data: self.pager.get_page(&guarded_node.parent_offset)?,
        })?;
        let median_key = &keys[keys.len() / 2];
        parent_node.add_key(median_key.to_string())
        // todo 分裂两个节点，并建立索引
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::util::error::Error;

    #[test]
    fn search_works() -> Result<(), Error> {
        // TODO: write this.
        Ok(())
    }

    #[test]
    fn insert_works() -> Result<(), Error> {
        // TODO: write this.
        Ok(())
    }
}
