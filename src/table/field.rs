use uuid::Uuid;
use crate::table::table_item::Table;
use std::rc::{Weak, Rc};
use crate::index::btree::BTree;
use crate::util::error::Error;
use crate::page::pager::Pager;
use crate::data_item::buffer::Buffer;
use crate::index::key_value_pair::KeyValuePair;

pub enum FieldType {
    INT32,
    FLOAT32,
    VARCHAR40,
}

impl Clone for FieldType {
    fn clone(&self) -> Self {
        match self {
            FieldType::FLOAT32 => FieldType::FLOAT32,
            FieldType::INT32 => FieldType::INT32,
            FieldType::VARCHAR40 => FieldType::VARCHAR40,
        }
    }
}

pub enum FieldValue {
    INT32(i32),
    FLOAT32(f32),
    VARCHAR40(String),
}

impl Clone for FieldValue {
    fn clone(&self) -> Self {
        match self {
            FieldValue::INT32(data) => FieldValue::INT32(*data),
            FieldValue::FLOAT32(data) => FieldValue::FLOAT32(*data),
            FieldValue::VARCHAR40(data) => FieldValue::VARCHAR40(data.clone())
        }
    }
}

impl From<i32> for FieldValue {
    fn from(data: i32) -> Self {
        FieldValue::INT32(data)
    }
}

impl From<f32> for FieldValue {
    fn from(data: f32) -> Self {
        FieldValue::FLOAT32(data)
    }
}

impl From<String> for FieldValue {
    fn from(data: String) -> Self {
        FieldValue::VARCHAR40(data)
    }
}

impl From<FieldValue> for i32 {
    fn from(fv: FieldValue) -> Self {
        match fv {
            FieldValue::INT32(data) => data,
            _ => 0,
        }
    }
}
pub struct Field {
    table: Weak<Table>,
    field_name: String,
    field_type: FieldType,
    btree: Option<BTree>,
}

impl Clone for Field {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            field_name: self.field_name.clone(),
            field_type: self.field_type.clone(),
            btree: self.btree.clone(),
        }
    }
}

impl Field {
    // fn load_field(table: &Table, uuid: Uuid) -> Result<FieldValue, Error> {
    //     Err(Error::UnexpectedError)
    // }

    fn parse_self(&self, bytes: &[u8], offset: usize) -> Result<FieldValue, Error> {
        match self.field_type {
            FieldType::INT32 => {
                let mut i32_data: [u8; 4] = [0; 4];
                i32_data.clone_from_slice(&bytes[offset..offset + 4]);
                let res = i32::from_be_bytes(i32_data);
                Ok(FieldValue::INT32(res))
            }
            FieldType::FLOAT32 => {
                let mut f32_data = [0u8; 4];
                f32_data.clone_from_slice(&bytes[offset..offset + 4]);
                let res = f32::from_be_bytes(f32_data);
                Ok(FieldValue::FLOAT32(res))
            }
            FieldType::VARCHAR40 => {
                let mut char_data: [u8; 40] = [0; 40];
                char_data.clone_from_slice(&bytes[offset..offset + 40]);
                let res = match std::str::from_utf8(&char_data) {
                    Ok(data) => data,
                    Err(_) => return Err(Error::UnexpectedError)
                };
                Ok(FieldValue::VARCHAR40(res.to_owned()))
            }
        }
    }

    pub fn create_field(table: Rc<Table>, field_name: String, field_type: FieldType) -> Result<Field, Error> {
        Ok(Field {
            table: Rc::downgrade(&table),
            field_name,
            field_type,
            btree: None,
        })
    }

    pub fn create_btree(&mut self, file_name: String, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        let pager = Pager::new(
            file_name.clone(),
            40,
            buffer,
        )?;
        self.btree = Some(
            BTree::new(
                pager,
                file_name.clone(),
                buffer,
            )?
        );
        Ok(())
    }

    fn persist(fv: FieldValue) -> Result<Vec<u8>, Error> {
        match &fv {
            FieldValue::INT32(data) => {
                Ok(data.to_be_bytes().to_vec())
            }

            FieldValue::FLOAT32(data) => {
                Ok(data.to_be_bytes().to_vec())
            }

            FieldValue::VARCHAR40(data) => {
                let mut res: [u8; 40] = [0u8; 40];
                let data_res = data.as_bytes();
                if data_res.len() > 40 {
                    Err(Error::FieldValueTooLong)
                } else {
                    for (i, item) in data_res.iter().enumerate() {
                        res[i] = *item;
                    }
                    Ok(res.to_vec())
                }
            }

            _ => Err(Error::FieldValueNotCompatible)
        }
    }

    fn insert(&mut self, fv: FieldValue, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        match &mut self.btree {
            Some(btree) => {
                // todo
                let bytes = Field::persist(fv.clone())?;
                let offset = btree.pager.insert_value(bytes.as_slice(), buffer)?;
                // let key = match fv/
                // let kv = KeyValuePair::new();
                Ok(())
            }
            None => {
                Err(Error::IndexWithoutBTree)
            }
        }
    }

    fn search(&self, left: FieldValue, right: FieldValue) -> Result<Vec<Uuid>, Error> {
        match &self.btree {
            Some(btree) => {
                // todo
                Err(Error::UnexpectedError)
            }
            None => {
                Err(Error::IndexWithoutBTree)
            }
        }
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