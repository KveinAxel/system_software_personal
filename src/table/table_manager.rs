use std::collections::HashMap;
use crate::page::pager::Pager;
use crate::table::table_item::Table;
use crate::util::error::Error;
use crate::data_item::buffer::Buffer;
use crate::table::entry::Entry;

pub struct TableManager {
    pager: Pager,
    table_cache: HashMap<String, Table>,
    buffer: Box<dyn Buffer>
}

impl TableManager {
    pub fn new(pager: Pager, buffer: Box<dyn Buffer>) -> TableManager {
        TableManager {
            pager,
            table_cache: HashMap::<String, Table>::new(),
            buffer
        }
    }

    pub fn read_full_table(&self, table_name: String) -> Result<Table, Error> {
        let raw_table = self.table_cache.get(table_name.as_str());
        match raw_table {
            Some(table) => {
                Ok(table.clone())
            }
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

    pub fn create_table(&mut self, table_to_create: Table) -> Result<(), Error> {
        // let raw_table = self.table_cache.get(table_to_create.table_name.as_str());
        // match raw_table {
        //     Some(_table) => return Err(Error::TableAlreadyExists),
        //     None => ()
        // };
        //
        // let table = Table::new(self, uuid, table_to_create)?;
        // self.update_first_uuid(table.self_uuid)?;
        // self.table_cache.insert(table.table_name.clone(), table.clone());
        Ok(())
    }
}
