#[cfg(test)]
mod test {
    use crate::util::test_lib::rm_test_file;
    use crate::util::error::Error;

    #[test]
    fn test_create_table() -> Result<(), Error>{
        rm_test_file();

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_read_full_table() -> Result<(), Error>{
        rm_test_file();

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_insert_table() -> Result<(), Error>{
        rm_test_file();

        rm_test_file();
        Ok(())
    }

}