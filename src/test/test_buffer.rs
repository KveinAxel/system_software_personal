#[cfg(test)]
mod test_buffer {
    use crate::data_item::buffer::{Buffer, LRUBuffer, ClockBuffer};
    use std::path::Path;
    use std::fs;
    use crate::page::page_item::{PAGE_SIZE, Page};
    use crate::util::error::Error;
    use crate::util::test_lib::rm_test_file;

    #[test]
    fn test_add_file() -> Result<(), Error> {
        rm_test_file();

        let mut buffer = LRUBuffer::new(10, "metadata.db".to_string())?;
        buffer.add_file(Path::new("test.db"))?;

        rm_test_file();

        let mut buffer2 = ClockBuffer::new(10, "metadata.db".to_string())?;
        buffer2.add_file(Path::new("test.db"))?;

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_fill_up_to() -> Result<(), Error>{
        match fs::remove_file("metadata2.db") {
            Ok(_) => (),
            Err(_) => (),
        };
        match fs::remove_file("test2.db") {
            Ok(_) => (),
            Err(_) => (),
        };

        let mut buffer = LRUBuffer::new(10, "metadata2.db".to_string())?;
        buffer.add_file(Path::new("test2.db"))?;
        buffer.fill_up_to("test2.db", 10)?;
        buffer.flush_file("test2.db")?;

        let meta = fs::metadata(Path::new("test2.db"))?;
        assert_eq!(14 * PAGE_SIZE as u64, meta.len());

        match fs::remove_file("metadata2.db") {
            Ok(_) => (),
            Err(_) => (),
        };
        match fs::remove_file("test2.db") {
            Ok(_) => (),
            Err(_) => (),
        };

        let mut buffer = ClockBuffer::new(10, "metadata2.db".to_string())?;
        buffer.add_file(Path::new("test2.db"))?;

        buffer.fill_up_to("test2.db", 10)?;
        buffer.flush_file("test2.db")?;

        let meta = fs::metadata(Path::new("test2.db"))?;
        assert_eq!(14 * PAGE_SIZE as u64, meta.len());

        match fs::remove_file("metadata2.db") {
            Ok(_) => (),
            Err(_) => (),
        };
        match fs::remove_file("test2.db") {
            Ok(_) => (),
            Err(_) => (),
        };        Ok(())
    }

    #[test]
    fn test_page_get_write() -> Result<(), Error> {
        rm_test_file();

        // test lru
        let mut slice: [u8; 4096] = [0; 4096];
        for i in 0..4096 {
            slice[i] = (i % 8) as u8;
        }
        let mut page = Page::new_phantom(slice);
        page.page_num = 1;
        page.file_name = String::from("test.db");
        let mut buffer = LRUBuffer::new(10, "metadata.db".to_string())?;
        buffer.add_file(Path::new("test.db"))?;
        buffer.fill_up_to("test.db", 10)?;
        buffer.write_page(page)?;
        buffer.flush_file("test.db")?;

        let page2 = buffer.get_page("test.db", 1)?.get_data();

        for i in 0..4096usize {
            assert_eq!((i % 8) as u8, page2[i]);
        }

        rm_test_file();

        // test clock
        let mut slice: [u8; 4096] = [0; 4096];
        for i in 0..4096 {
            slice[i] = (i % 8) as u8;
        }
        let mut page = Page::new_phantom(slice);
        page.page_num = 1;
        page.file_name = String::from("test.db");
        let mut buffer = ClockBuffer::new(10, "metadata.db".to_string())?;
        buffer.add_file(Path::new("test.db"))?;
        buffer.fill_up_to("test.db", 10)?;
        buffer.write_page(page)?;
        buffer.flush_file("test.db")?;

        let page2 = buffer.get_page("test.db", 1)?.get_data();

        for i in 0..4096usize {
            assert_eq!((i % 8) as u8, page2[i]);
        }

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_lru_algo() -> Result<(), Error> {
        rm_test_file();

        let mut buffer = LRUBuffer::new(4, "metadata.db".to_string())?;
        buffer.add_file(Path::new("test.db"))?;
        buffer.fill_up_to("test.db", 10)?;

        buffer.get_page("test.db", 2)?;
        buffer.get_page("test.db", 4)?;
        buffer.get_page("test.db", 3)?;
        buffer.get_page("test.db", 1)?;

        let vec = vec![2, 4, 3, 1];

        let list = &buffer.list;
        for (i, item) in list.iter().enumerate() {
            assert_eq!(item.page.page_num, vec[i]);
        }

        buffer.get_page("test.db", 5)?;
        buffer.get_page("test.db", 7)?;
        buffer.get_page("test.db", 3)?;
        buffer.get_page("test.db", 6)?;

        let vec2 = vec![5, 7, 3, 6];
        let list = &buffer.list;
        for (i, item) in list.iter().enumerate() {
            assert_eq!(item.page.page_num, vec2[i]);
        }

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_clock_algo() -> Result<(), Error> {
        rm_test_file();

        let mut buffer = ClockBuffer::new(4, "metadata.db".to_string())?;
        buffer.add_file(Path::new("test.db"))?;
        buffer.fill_up_to("test.db", 10)?;

        buffer.get_page("test.db", 2)?;
        buffer.get_page("test.db", 4)?;
        buffer.get_page("test.db", 3)?;
        buffer.get_page("test.db", 1)?;

        let vec = vec![2, 4, 3, 1];

        let list = &buffer.list;
        for (i, item) in list.iter().enumerate() {
            assert_eq!(item.page.page_num, vec[i]);
        }

        buffer.get_page("test.db", 5)?;
        buffer.get_page("test.db", 7)?;
        buffer.get_page("test.db", 3)?;
        buffer.get_page("test.db", 6)?;

        let vec2 = vec![5, 7, 3, 6];
        let list = &buffer.list;
        for (i, item) in list.iter().enumerate() {
            assert_eq!(item.page.page_num, vec2[i]);
        }

        rm_test_file();
        Ok(())
    }
}