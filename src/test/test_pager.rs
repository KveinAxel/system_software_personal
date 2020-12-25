
#[cfg(test)]
mod test_pager {
    use crate::util::error::Error;
    use crate::page::pager::Pager;
    use crate::util::test_lib::{rm_test_file, gen_buffer};

    #[test]
    fn test_get_new_pager() -> Result<(), Error> {
        rm_test_file();

        let mut buffer = gen_buffer()?;
        let mut pager = Pager::new("test.db".to_string(), 50, &mut buffer)?;
        assert_eq!(pager.cnt, 0);
        pager.get_new_page(&mut buffer)?;
        assert_eq!(pager.cnt, 1);
        pager.get_new_page(&mut buffer)?;
        assert_eq!(pager.cnt, 2);

        rm_test_file();
        Ok(())
    }

}
