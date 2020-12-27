use crate::table::field::{Field, FieldValue, FieldType};
use crate::util::error::Error;
use crate::table::entry::Entry;
use crate::data_item::buffer::Buffer;
use crate::page::pager::Pager;
use std::path::Path;

pub struct Table {
    pub(crate) table_name: String,
    pub(crate) fields: Vec<Field>,
    pager: Box<Pager>
}

impl Table {
    pub fn new(table_name: String, buffer: &mut Box<dyn Buffer>) -> Result<Table, Error> {
        buffer.add_file(Path::new(table_name.as_str()))?;
        Ok(Table {
            table_name: table_name.clone(),
            fields: Vec::<Field>::new(),
            pager: Pager::new(table_name, 40, buffer)?,
        })
    }

    pub fn insert(&mut self, entry: Entry, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        if self.fields.len() != entry.data.len() {
            return Err(Error::UnexpectedError)
        }

        for (i, item) in self.fields.iter().enumerate() {
            Table::check_field(item, entry.data.get(i).unwrap())?;
        }

        let primary_key = self.fields.get_mut(0).unwrap();
        primary_key.insert(0, entry, &mut self.pager, buffer)
    }

    pub fn add_fields(&mut self, fields: Vec<Field>) {
        self.fields = [self.fields.clone(), fields].concat();
    }

    pub fn search(&self, key_index: usize, fv: FieldValue, buffer: &mut Box<dyn Buffer>) -> Result<Entry, Error> {
        if key_index > self.fields.len() {
            return Err(Error::UnexpectedError)
        }

        Table::check_field(self.fields.get(key_index).unwrap(), &fv)?;

        let field = if self.fields.get(key_index).unwrap().is_indexed() {
            self.fields.get(key_index).unwrap()
        } else {
            return Err(Error::IndexWithoutBTree)
        };
        let res = field.search(fv, buffer)?;
        let res_slice = res.as_slice();
        let mut offset = 0;
        let mut entry = Entry {
            data: Vec::<FieldValue>::new()
        };

        for item in &self.fields {
            let (fv, siz) = item.parse_self(res_slice, offset)?;
            offset += siz;
            entry.data.push(fv);
        }

        Ok(entry)

    }

    pub fn search_range(&mut self, key_index: usize, raw_left_value: Option<FieldValue>, raw_right_value: Option<FieldValue>, buffer: &mut Box<dyn Buffer>) -> Result<Vec<Entry>, Error> {
        if key_index > self.fields.len() {
            return Err(Error::UnexpectedError)
        }

        match &raw_left_value {
            Some(left_value) => {
                Table::check_field(self.fields.get(key_index).unwrap(), left_value)?;
            }
            None => ()
        };
        match &raw_right_value {
            Some(right_value) => {
                Table::check_field(self.fields.get(key_index).unwrap(), right_value)?;
            }
            None => ()
        };

        let field = if self.fields.get(key_index).unwrap().is_indexed() {
            self.fields.get(key_index).unwrap()
        } else {
            return Err(Error::IndexWithoutBTree)
        };

        let mut siz = 0;
        for f in &self.fields {
            siz += match f.field_type {
                FieldType::INT32 => 4,
                FieldType::FLOAT32 => 4,
                FieldType::VARCHAR40 => 40,
            };
        }
        let res = field.search_range(raw_left_value, raw_right_value, buffer, siz, &mut self.pager)?;
        let mut res_vec = Vec::<Entry>::new();
        for row in res {
            let res_slice = row.as_slice();
            let mut offset = 0;
            let mut entry = Entry {
                data: Vec::<FieldValue>::new()
            };

            for item in &self.fields {
                let (fv, siz) = item.parse_self(res_slice, offset)?;
                offset += siz;
                entry.data.push(fv);
            }
            res_vec.push(entry);
        }

        Ok(res_vec)
    }

    fn check_field(field: &Field, fv: &FieldValue) -> Result<(), Error> {
        match (&field.field_type, fv) {
            (FieldType::INT32, FieldValue::INT32(_)) => Ok(()),
            (FieldType::FLOAT32, FieldValue::FLOAT32(_)) => Ok(()),
            (FieldType::VARCHAR40, FieldValue::VARCHAR40(data)) => {
                if data.as_bytes().len() > 40 {
                    return Err(Error::VarcharTooLong)
                }
                Ok(())
            },
            _ => {
                Err(Error::FieldValueNotCompatible)
            }
        }
    }

    pub fn create_index(&mut self, key_index: usize, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        if self.fields.len() <= key_index {
            return Err(Error::UnexpectedError)
        }

        let k = self.fields.get_mut(key_index).unwrap();
        let file_name = k.field_name.clone() + ".idx";
        k.create_btree(file_name, buffer)
    }
}

impl Clone for Table {
    fn clone(&self) -> Self {
        let mut fields = Vec::<Field>::new();
        for i in &self.fields {
            fields.push(i.clone());
        }
        Table {
            table_name: self.table_name.clone(),
            fields,
            pager: self.pager.clone()
        }
    }
}
