use uuid::Uuid;
use crate::table::table_item::Table;
use std::rc::{Weak};
use crate::index::btree::BTree;
use crate::util::error::Error;

pub trait Value {}

pub enum FieldType {
    INT32,
    FLOAT32,
    VARCHAR20,
}

impl Clone for FieldType {
    fn clone(&self) -> Self {
        match self {
            FieldType::FLOAT32 => FieldType::FLOAT32,
            FieldType::INT32 => FieldType::INT32,
            FieldType::VARCHAR20 => FieldType::VARCHAR20,
        }
    }
}

pub struct Field {
    self_uuid: Uuid,
    table: Weak<Table>,
    field_name: String,
    field_type: FieldType,
    index: Uuid,
    btree: BTree,
}

// impl Clone for Field {
//     fn clone(&self) -> Self {
//         Self {
//             self_uuid: self.self_uuid.clone(),
//             table: self.table.clone(),
//             field_name: self.field_name.clone(),
//             field_type: self.field_type.clone(),
//             index: self.index.clone(),
//             btree: self.btree.clone(),
//         }
//     }
// }

impl Field {
    fn load_field(table: &Table, uuid: Uuid) -> Result<Field, Error> {
        Err(Error::UnexpectedError)
    }

    fn parse_self(&self) -> Result<Vec<u8>, Error> {
        Err(Error::UnexpectedError)
    }

    fn create_field(table: &Table, field_name: String, field_type: FieldType, indexed: bool) -> Result<Field, Error> {
        Err(Error::UnexpectedError)
    }

    fn persist_self(&self) -> Result<(), Error> {
        Err(Error::UnexpectedError)
    }

    fn print(&self) -> Result<(), Error> {
        Err(Error::UnexpectedError)
    }

    fn is_indexed(&self) -> Result<bool, Error> {
        Err(Error::UnexpectedError)
    }

    fn insert(&mut self, key: Box<dyn Value>, uuid: Uuid) -> Result<(), Error> {
        Err(Error::UnexpectedError)
    }

    fn search(&self, left: Box<dyn Value>, right: Box<dyn Value>) -> Result<Vec<Uuid>, Error> {
        Err(Error::UnexpectedError)
    }

    fn str_to_value(&self, str: String) -> Result<Box<dyn Value>, Error> {
        Err(Error::UnexpectedError)
    }

    fn value_to_raw(&self, value: Box<dyn Value>) -> Result<Vec<u8>, Error> {
        Err(Error::UnexpectedError)
    }

    fn parse_value(&self, bytes: &[u8]) -> Result<Box<dyn Value>, Error> {
        Err(Error::UnexpectedError)
    }

    fn value_to_uuid(&self, value: Box<dyn Value>) -> Result<Uuid, Error> {
        Err(Error::UnexpectedError)
    }

    fn value_print(&self, value: Box<dyn Value>) -> Result<(), Error> {
        Err(Error::UnexpectedError)
    }

    fn calc_exp() -> Result<(), Error> {
        Err(Error::UnexpectedError)
    }
}

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
    fn test_persist() -> Result<(), Error> {
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