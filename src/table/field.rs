use crate::index::btree::BTree;
use crate::util::error::Error;
use crate::page::pager::Pager;
use crate::data_item::buffer::Buffer;
use crate::index::key_value_pair::KeyValuePair;
use crate::table::entry::Entry;

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

impl FieldValue {
    fn to_size(&self) -> usize {
        match self {
            FieldValue::INT32(_data) => 32,
            FieldValue::FLOAT32(_data) => 32,
            FieldValue::VARCHAR40(_data) => 40,
        }
    }
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

impl From<FieldValue> for String {
    fn from(fv: FieldValue) -> Self {
        match fv {
            FieldValue::INT32(data) => data.to_string().clone(),
            FieldValue::FLOAT32(data) => data.to_string().clone(),
            FieldValue::VARCHAR40(data) => data.clone()
        }
    }
}

impl From<&FieldValue> for String {
    fn from(fv: &FieldValue) -> Self {
        match fv {
            FieldValue::INT32(data) => data.to_string().clone(),
            FieldValue::FLOAT32(data) => data.to_string().clone(),
            FieldValue::VARCHAR40(data) => data.clone()
        }
    }
}


impl From<FieldValue> for Vec<u8> {
    fn from(fv: FieldValue) -> Self {
        match fv {
            FieldValue::INT32(data) => data.to_be_bytes().to_vec(),
            FieldValue::FLOAT32(data) => data.to_be_bytes().to_vec(),
            FieldValue::VARCHAR40(data) => data.into_bytes()
        }
    }
}

pub struct Field {
    field_name: String,
    pub(crate) field_type: FieldType,
    btree: Option<BTree>,
}

impl Clone for Field {
    fn clone(&self) -> Self {
        Self {
            field_name: self.field_name.clone(),
            field_type: self.field_type.clone(),
            btree: self.btree.clone(),
        }
    }
}

impl Field {

    pub fn parse_self(&self, bytes: &[u8], offset: usize) -> Result<(FieldValue, usize), Error> {
        match self.field_type {
            FieldType::INT32 => {
                let mut i32_data: [u8; 4] = [0; 4];
                i32_data.clone_from_slice(&bytes[offset..offset + 4]);
                let res = i32::from_be_bytes(i32_data);
                Ok((FieldValue::INT32(res), 32))
            }
            FieldType::FLOAT32 => {
                let mut f32_data = [0u8; 4];
                f32_data.clone_from_slice(&bytes[offset..offset + 4]);
                let res = f32::from_be_bytes(f32_data);
                Ok((FieldValue::FLOAT32(res), 32))
            }
            FieldType::VARCHAR40 => {
                let mut char_data: [u8; 40] = [0; 40];
                char_data.clone_from_slice(&bytes[offset..offset + 40]);
                let res = match std::str::from_utf8(&char_data) {
                    Ok(data) => data,
                    Err(_) => return Err(Error::UnexpectedError)
                };
                Ok((FieldValue::VARCHAR40(res.to_owned()), 40))
            }
        }
    }

    pub fn create_field(field_name: String, field_type: FieldType) -> Result<Field, Error> {
        Ok(Field {
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

    pub fn insert(&mut self, key_index: usize, entry: Entry, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        match &mut self.btree {
            Some(btree) => {
                if key_index > entry.data.len() {
                    return Err(Error::UnexpectedError)
                }
                match (&self.field_type, &entry.data.get(key_index).unwrap()) {
                    (FieldType::INT32, FieldValue::INT32(_data)) => (),
                    (FieldType::FLOAT32, FieldValue::FLOAT32(_data)) => (),
                    (FieldType::VARCHAR40, FieldValue::VARCHAR40(_data)) => (),
                    _ => return Err(Error::UnexpectedError)
                }
                let key: String = entry.data.get(key_index).unwrap().into();
                let bytes = entry.to_bytes();
                let offset = btree.pager.insert_value(bytes.as_slice(), buffer)?;
                let kv = KeyValuePair::new(key, offset);
                btree.insert(kv, buffer)
            }
            None => {
                Err(Error::IndexWithoutBTree)
            }
        }
    }

    pub fn search(&self, fv: FieldValue, buffer: &mut Box<dyn Buffer>) -> Result<Vec<u8>, Error> {
        match &self.btree {
            Some(btree) => {
                let key = (&fv).into();
                match btree.search(key, buffer) {
                    Ok(data) => {
                        let offset = data.value;
                        let siz = fv.to_size();
                        btree.pager.get_value(offset, siz, buffer)
                    }
                    Err(err) => return Err(err)
                }
            }
            None => {
                Err(Error::IndexWithoutBTree)
            }
        }
    }

    pub fn is_indexed(&self) -> bool {
        match &self.btree {
            Some(_) => true,
            None => false
        }
    }
}
