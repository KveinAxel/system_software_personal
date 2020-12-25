use std::rc::{Weak};

use crate::table::field::{Field, FieldValue, FieldType};
use crate::table::table_manager::TableManager;
use crate::util::error::Error;
use crate::table::entry::Entry;
use crate::data_item::buffer::Buffer;

pub enum Status {
    NORMAL = 1,
}

impl Clone for Status {
    fn clone(&self) -> Self {
        match self {
            Status::NORMAL => Status::NORMAL
        }
    }
}

pub struct Table {
    table_manager: Weak<TableManager>,
    pub(crate) table_name: String,
    status: Status,
    fields: Vec<Field>,
}

impl Table {
    pub fn new(table_manager: Weak<TableManager>, table_name: String) -> Table {
        Table {
            table_manager,
            table_name,
            status: Status::NORMAL,
            fields: Vec::<Field>::new(),
        }
    }

    pub fn insert(&mut self, entry: Entry, buffer: &mut Box<dyn Buffer>) -> Result<(), Error> {
        if self.fields.len() > entry.data.len() {
            return Err(Error::UnexpectedError)
        }

        for (i, item) in self.fields.iter().enumerate() {
            Table::check_field(item, entry.data.get(i).unwrap())?;
        }

        let primary_key = self.fields.get_mut(0).unwrap();
        primary_key.insert(0, entry, buffer)
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
            self.fields.get(0).unwrap()
        };
        let res = field.search(fv, buffer)?;
        let res_slice = res.as_slice();
        let mut offset = 0;
        let mut entry = Entry {
            data: Vec::<FieldValue>::new()
        };

        for (i, item) in self.fields.iter().enumerate() {
            let (fv, siz) = self.fields.get(i).unwrap().parse_self(res_slice, offset)?;
            offset += siz;
            entry.data.push(fv);
        }

        Ok(entry)

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
}

impl Clone for Table {
    fn clone(&self) -> Self {
        let mut fields = Vec::<Field>::new();
        for i in &self.fields {
            fields.push(i.clone());
        }
        Table {
            table_manager: self.table_manager.clone(),
            table_name: self.table_name.clone(),
            status: self.status.clone(),
            fields: fields.clone(),
        }
    }
}
