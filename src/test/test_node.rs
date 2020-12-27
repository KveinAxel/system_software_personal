
#[cfg(test)]
mod test_node {
    use std::convert::TryFrom;

    use crate::index::node::{INTERNAL_NODE_HEADER_SIZE, KEY_SIZE, LEAF_NODE_HEADER_SIZE, Node, NodeSpec, VALUE_SIZE, MAX_SPACE_FOR_KEYS, MAX_SPACE_FOR_CHILDREN};
    use crate::page::page::{PAGE_SIZE, PTR_SIZE};
    use crate::util::error::Error;

    #[test]
    fn page_to_node_works() -> Result<(), Error> {
        // 测试单个根节点
        const DATA_LEN: usize = LEAF_NODE_HEADER_SIZE + KEY_SIZE + VALUE_SIZE;
        let page_data: [u8; DATA_LEN] = [
            0x01, // 是否是根 true
            0x02, // 节点类型 LEAF
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 父节点指针 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // 键值对个数 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 下个叶子节点的指针 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 上个叶子节点的指针 0
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello" 键
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096
        ];
        let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
            *to = *from
        }

        let offset = PAGE_SIZE * 2;
        let node = Node::try_from(NodeSpec {
            offset,
            page_data: page,
        })?;

        assert_eq!(node.is_root, true);
        assert_eq!(node.parent_offset, 0);

        Ok(())
    }

    #[test]
    fn get_key_value_pairs_works() -> Result<(), Error> {
        const DATA_LEN: usize = LEAF_NODE_HEADER_SIZE + KEY_SIZE + VALUE_SIZE;
        let page_data: [u8; DATA_LEN] = [
            0x01, // 是否是根节点 true
            0x02, // 节点类型 LEAF
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 父节点指针 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // 键值对数量 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 下个叶子节点的指针 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 上个叶子节点的指针 0
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello" 键
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096
        ];
        let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
            *to = *from
        }

        let offset = PAGE_SIZE * 2;
        let node = Node::try_from(NodeSpec {
            offset,
            page_data: page,
        })?;
        let kv = node.get_key_value_pairs()?;

        assert_eq!(kv.len(), 1);
        let first_kv = match kv.get(0) {
            Some(kv) => kv,
            None => return Err(Error::UnexpectedError),
        };

        assert_eq!(first_kv.key, "hello");
        assert_eq!(first_kv.value, 4096usize);

        Ok(())
    }

    #[test]
    fn get_children_works() -> Result<(), Error> {
        let internal_header: [u8; INTERNAL_NODE_HEADER_SIZE] = [
            0x01, // 是否为根 true
            0x01, // 节点类型 INTERNAL
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 父节点指针 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // 儿子的个数 3
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // 键个数 2
        ];

        let children_data: [u8; PTR_SIZE * 3] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, // 8192
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, // 12288
        ];
        const JUNK_CHILDREN_DATA_SIZE: usize = MAX_SPACE_FOR_CHILDREN - 3 * PTR_SIZE;
        let junk_children_data: [u8; JUNK_CHILDREN_DATA_SIZE] = [0u8; JUNK_CHILDREN_DATA_SIZE];

        let key_data: [u8; 2 * KEY_SIZE] = [
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
            0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
        ];

        const JUNK_SIZE: usize = MAX_SPACE_FOR_KEYS - 2 * KEY_SIZE;
        let junk: [u8; JUNK_SIZE] = [0x00; JUNK_SIZE];

        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut()
            .zip(internal_header.iter()
                .chain(children_data.iter())
                .chain(junk_children_data.iter())
                .chain(key_data.iter())
                .chain(junk.iter())
            ) {
            *to = *from
        }

        let offset = 0;
        let node = Node::try_from(NodeSpec {
            offset,
            page_data: page,
        })?;
        let children = node.get_children()?;

        assert_eq!(children.len(), 3);
        for (i, child) in children.iter().enumerate() {
            assert_eq!(*child, PAGE_SIZE * (i + 1));
        }

        Ok(())
    }

    #[test]
    fn get_keys_work_for_internal_node() -> Result<(), Error> {
        let internal_header: [u8; INTERNAL_NODE_HEADER_SIZE] = [
            0x01, // 是否为根 true
            0x01, // 节点类型 INTERNAL
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 父节点指针 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // 值的个数 3
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // 键个数 2
        ];

        let children_data: [u8; PTR_SIZE * 3] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, // 8192
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, // 12288
        ];
        const JUNK_CHILDREN_DATA_SIZE: usize = MAX_SPACE_FOR_CHILDREN - 3 * PTR_SIZE;
        let junk_children_data: [u8; JUNK_CHILDREN_DATA_SIZE] = [0u8; JUNK_CHILDREN_DATA_SIZE];

        let key_data: [u8; 2 * KEY_SIZE] = [
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
            0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
        ];

        const JUNK_SIZE: usize = MAX_SPACE_FOR_KEYS - 2 * KEY_SIZE;
        let junk: [u8; JUNK_SIZE] = [0x00; JUNK_SIZE];

        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut()
            .zip(internal_header.iter()
                .chain(children_data.iter())
                .chain(junk_children_data.iter())
                .chain(key_data.iter())
                .chain(junk.iter())
            ) {
            *to = *from
        }

        let offset = 0;
        let node = Node::try_from(NodeSpec {
            offset,
            page_data: page,
        })?;
        let keys = node.get_keys()?;
        assert_eq!(keys.len(), 2);

        let first_key = match keys.get(0) {
            Some(key) => key,
            None => return Err(Error::UnexpectedError),
        };
        assert_eq!(first_key, "hello");

        let second_key = match keys.get(1) {
            Some(key) => key,
            None => return Err(Error::UnexpectedError),
        };
        assert_eq!(second_key, "world");

        Ok(())
    }

    #[test]
    fn get_keys_work_for_leaf_node() -> Result<(), Error> {
        const DATA_LEN: usize = LEAF_NODE_HEADER_SIZE + 2 * KEY_SIZE + 2 * VALUE_SIZE;
        let page_data: [u8; DATA_LEN] = [
            0x01, // 是否为根节点 true
            0x02, // 节点类型 LEAF
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 父节点指针
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // 键值对个数 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 下个叶子节点的指针
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 上个叶子节点的指针
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello" 键0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096
            0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world" 值0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, // 4096 * 2
        ];

        let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];

        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
            *to = *from
        }

        let offset = 0;
        let node = Node::try_from(NodeSpec {
            offset,
            page_data: page,
        })?;

        let keys = node.get_keys()?;
        assert_eq!(keys.len(), 2);

        let first_key = match keys.get(0) {
            Some(key) => key,
            None => return Err(Error::UnexpectedError),
        };
        assert_eq!(first_key, "hello");

        let second_key = match keys.get(1) {
            Some(key) => key,
            None => return Err(Error::UnexpectedError),
        };
        assert_eq!(second_key, "world");

        Ok(())
    }
}