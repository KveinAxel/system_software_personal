
#[cfg(test)]
mod test {
    use crate::util::error::Error;
    use crate::util::test_lib::rm_test_file;

    #[test]
    fn test_create_field() -> Result<(), Error> {
        rm_test_file();



        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_parse_field() -> Result<(), Error> {
        rm_test_file();

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_insert() -> Result<(), Error> {
        rm_test_file();

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_search() -> Result<(), Error> {
        rm_test_file();

        rm_test_file();
        Ok(())
    }
}