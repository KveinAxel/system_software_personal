#[cfg(test)]
mod test {
    use crate::util::test_lib::{rm_test_file, gen_buffer};
    use crate::util::error::Error;
    use crate::table::table_manager::TableManager;
    use crate::table::field::{Field, FieldType, FieldValue};
    use crate::table::entry::{Entry};
    use crate::data_item::buffer::LRUBuffer;
    use std::fs;

    #[test]
    fn test_create_table() -> Result<(), Error>{
        rm_test_file();

        let buffer = gen_buffer()?;
        let mut table = TableManager::new(buffer);
        let mut fields = Vec::<Field>::new();
        let f = Field::create_field("test_field".to_string(), FieldType::INT32)?;
        fields.push(f);
        table.create_table("test_table".to_string(), fields)?;

        assert_eq!(table.table_cache.get("test_table").unwrap().fields.len(), 1);
        assert_eq!(table.table_cache.get("test_table").unwrap().fields.get(0).unwrap().field_name, "test_field".to_string());
        match table.table_cache.get("test_table").unwrap().fields.get(0).unwrap().field_type {
            FieldType::INT32 => (),
            _ => {
                assert!(false);
                ()
            }
        };

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_insert_and_read_full_table() -> Result<(), Error>{
        match fs::remove_file("id.idx") {
            Ok(_) => (),
            Err(_) => (),
        };
        match fs::remove_file("test_table") {
            Ok(_) => (),
            Err(_) => (),
        };

        let buffer = Box::new(LRUBuffer::new(4, "metadata.db".to_string())?);
        let mut table = TableManager::new(buffer);
        let mut fields = Vec::<Field>::new();
        let f1 = Field::create_field("id".to_string(), FieldType::INT32)?;
        let f2 = Field::create_field("test_field".to_string(), FieldType::INT32)?;
        fields.push(f1);
        fields.push(f2);
        table.create_table("test_table".to_string(), fields)?;
        table.create_index("test_table".to_string(), 0)?;

        let mut entry = Entry {
            data: Vec::<FieldValue>::new()
        };
        entry.data.push(FieldValue::INT32(1));
        entry.data.push(FieldValue::INT32(2));
        table.insert("test_table".to_string(), entry)?;

        let res = table.read_full_table("test_table".to_string())?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].data.len(), 2);
        match res[0].data[0] {
            FieldValue::INT32(i) => {
                assert_eq!(i, 1);
            }
            _ => assert!(false)
        };
        match res[0].data[1] {
            FieldValue::INT32(i) => {
                assert_eq!(i, 2);
            }
            _ => assert!(false)
        };

        match fs::remove_file("id.idx") {
            Ok(_) => (),
            Err(_) => (),
        };
        match fs::remove_file("test_table") {
            Ok(_) => (),
            Err(_) => (),
        };
        Ok(())
    }

}