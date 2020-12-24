use std::collections::HashMap;

use uuid::Uuid;

use crate::page::pager::Pager;
use crate::table::field::Field;
use crate::table::table_item::Table;
use crate::util::error::Error;
use crate::index::btree::BTree;
use crate::data_item::buffer::Buffer;

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

    pub fn load_tables(&mut self) -> Result<(), Error> {
        let mut uuid = self.first_uuid()?;
        while uuid != Uuid::nil() {
            let table = Table::load_table(self, uuid)?;
            uuid = table.next_table;
            self.table_cache.insert(table.table_name.clone(), *table);
        }
        Ok(())
    }

    pub fn first_uuid(&mut self) -> Result<Uuid, Error> {
        let uuid = self.pager.get_first_uuid(&self.buffer)?;
        Ok(uuid)
    }

    pub fn update_first_uuid(&mut self, uuid: Uuid) -> Result<(), Error> {
        self.pager.update_first_uuid(uuid, &self.buffer)
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

    pub fn insert(&mut self, table_name: String, field: Field) -> Result<(), Error> {
        let raw_table = self.table_cache.get_mut(table_name.as_str());
        match raw_table {
            Some(table) => {
                table.insert(field)
            }
            None => Err(Error::TableNotFound)
        }
    }

    pub fn create_table(&mut self, table_to_create: Table) -> Result<(), Error> {
        let raw_table = self.table_cache.get(table_to_create.table_name.as_str());
        match raw_table {
            Some(_table) => return Err(Error::TableAlreadyExists),
            None => ()
        };

        let uuid = self.first_uuid()?.clone();
        let table = Table::create_table(self, uuid, table_to_create)?;
        self.update_first_uuid(table.self_uuid)?;
        self.table_cache.insert(table.table_name.clone(), table.clone());
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::util::test_lib::rm_test_file;
    use crate::util::error::Error;

    #[test]
    fn test_create_table() -> Result<(), Error>{
        rm_test_file();

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_read_full_table() -> Result<(), Error>{
        rm_test_file();

        rm_test_file();
        Ok(())
    }

    #[test]
    fn test_insert_table() -> Result<(), Error>{
        rm_test_file();

        rm_test_file();
        Ok(())
    }

}