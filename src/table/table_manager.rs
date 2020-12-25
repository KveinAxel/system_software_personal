use std::collections::HashMap;
use crate::table::table_item::Table;
use crate::util::error::Error;
use crate::data_item::buffer::Buffer;
use crate::table::entry::Entry;
use crate::table::field::{Field, FieldValue};

pub struct TableManager {
    table_cache: HashMap<String, Table>,
    buffer: Box<dyn Buffer>
}

impl TableManager {
    pub fn new(buffer: Box<dyn Buffer>) -> TableManager {
        TableManager {
            table_cache: HashMap::<String, Table>::new(),
            buffer
        }
    }

    pub fn read_full_table(&mut self, table_name: String) -> Result<Vec<Entry>, Error> {
        let raw_table = self.table_cache.get_mut(table_name.as_str());
        match raw_table {
            Some(table) => Ok(table.search_range(0, None, None, &mut self.buffer)?),
            None => Err(Error::TableNotFound)
        }
    }

    pub fn insert(&mut self, table_name: String, entry: Entry) -> Result<(), Error> {
        let raw_table = self.table_cache.get_mut(table_name.as_str());
        match raw_table {
            Some(table) => {
                table.insert(entry, &mut self.buffer)
            }
            None => Err(Error::TableNotFound)
        }
    }

    pub fn create_table(&mut self, table_name: String, fields: Vec<Field>) -> Result<(), Error> {
        let raw_table = self.table_cache.get(table_name.as_str());
        match raw_table {
            Some(_table) => return Err(Error::TableAlreadyExists),
            None => ()
        };

        let mut table = Table::new(table_name);
        table.add_fields(fields);
        self.table_cache.insert(table.table_name.clone(), table);
        Ok(())
    }
}
