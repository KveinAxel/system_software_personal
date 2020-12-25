
#[cfg(test)]
mod test_btree {
    use crate::util::error::Error;
    use crate::util::test_lib::{rm_test_file, gen_tree, gen_kv, gen_2_kv, gen_buffer};
    use crate::index::key_value_pair::KeyValuePair;

    #[test]
    fn test_search_empty_tree() -> Result<(), Error> {
        rm_test_file();

        let mut buffer = gen_buffer()?;
        let tree = gen_tree(&mut buffer)?;

        let kv = gen_kv()?;
        match tree.search(kv.key, &mut buffer) {
            Err(Error::KeyNotFound) => (),
            _ => {
                assert!(false);
            }
        }

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_insert_search_tree() -> Result<(), Error> {
        rm_test_file();

        let mut buffer = gen_buffer()?;
        let mut tree = gen_tree(&mut buffer)?;

        let (kv1, kv2) = gen_2_kv()?;

        tree.insert(kv1, &mut buffer)?;
        tree.insert(kv2, &mut buffer)?;

        let res1 = tree.search("Hello".to_string(), &mut buffer)?;
        assert_eq!(res1.value, 4096usize);
        let res2 = tree.search("Test".to_string(), &mut buffer)?;
        assert_eq!(res2.value, 4096 * 2usize);
        match tree.search("not_exist".to_string(), &mut buffer) {
            Err(Error::KeyNotFound) => (),
            _ => {
                assert!(false);
            }
        }

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_update() -> Result<(), Error> {
        rm_test_file();

        let mut buffer = gen_buffer()?;
        let mut tree = gen_tree(&mut buffer)?;

        let (kv1, kv2) = gen_2_kv()?;

        tree.insert(kv1.clone(), &mut buffer)?;
        assert_eq!(tree.search(kv1.key.clone(), &mut buffer)?.value, kv1.value);

        let kv3 = KeyValuePair::new(kv1.key.clone(), kv2.value.clone());
        tree.update(kv3, &mut buffer)?;

        assert_ne!(tree.search(kv1.key.clone(), &mut buffer)?.value, kv1.value);
        assert_eq!(tree.search(kv1.key.clone(), &mut buffer)?.value, kv2.value);

        rm_test_file();
        Ok(())
    }
}