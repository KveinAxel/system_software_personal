use std::rc::{Weak};

use crate::table::field::{Field, FieldValue};
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
        let primary_key = self.fields.get_mut(0).unwrap();

        // todo check field

        primary_key.insert(0, entry, buffer)
    }

    pub fn add_fields(&mut self, fields: Vec<Field>) {
        self.fields = [self.fields.clone(), fields].concat();
    }

    pub fn search(&self, key_index: usize, fv: FieldValue, buffer: &mut Box<dyn Buffer>) -> Result<Entry, Error> {
        // todo check field
        if key_index > self.fields.len() {
            return Err(Error::UnexpectedError)
        }

        let field = if self.fields.get(key_index).unwrap().is_indexed() {
            self.fields.get(key_index).unwrap()
        } else {
            self.fields.get(0).unwrap()
        };
        let res = field.search(fv, buffer)?;
        // todo 解析

        Err(Error::UnexpectedError)

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
