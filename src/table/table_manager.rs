use crate::table::Table;
use crate::table::field::Field;
use crate::error::Error;
use crate::page::pager::Pager;
use crate::table::booter::Booter;

use uuid::Uuid;
use std::collections::HashMap;
use std::sync::Mutex;
use std::path::Path;
use std::borrow::Borrow;
use std::intrinsics::type_name;
use std::ops::Deref;
use crate::table::table_item::Table;

pub struct TableManager {
    pager: Pager,
    booter: Booter,
    table_cache: HashMap<String, Table>,
}

impl TableManager {
    pub fn new(pager: Pager, booter: Booter) -> TableManager {
        TableManager {
            pager,
            booter,
            table_cache: HashMap::<String, Table>::new(),
        }
    }

    pub fn create(path: Path, pager: Pager) -> Result<TableManager, Error> {
        let booter = Booter::create(path)?;
        Ok(TableManager::new(pager, booter))
    }

    pub fn open(path: Path, pager: Pager) -> Result<TableManager, Error> {
        let booter = Booter::open(path)?;
        Ok(TableManager::new(pager, booter))
    }

    pub fn load_tables(&mut self) -> Result<(), Error> {
        let mut uuid = self.first_uuid()?;
        while uuid != Uuid::nil() {
            let mut table = Table::load_table(self, uuid)?;
            uuid = table.next_table;
            self.table_cache.insert(table.table_name.clone(), *table);
        }
        Ok(())
    }

    pub fn first_uuid(&self) -> Result<Uuid, Error> {
        let uuid = self.booter.load()?;
        Ok(uuid)
    }

    pub fn update_first_uuid(&mut self, uuid: Uuid) -> Result<(), Error> {
        self.booter.update(uuid)
    }

    pub fn read(&self, table_name: String) -> Result<Table, Error> {
        let raw_table = self.table_cache.get(table_name.as_str());
        match raw_table {
            Some(table) => {
                Ok(*table.clone())
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
            Some(table) => return Err(Error::TableAlreadyExists),
            None => ()
        };

        let table = Table::create_table(self, self.first_uuid().clone()?, table_to_create)?;
        self.update_first_uuid(table.self_uuid);
        self.table_cache.insert(table.table_name.clone(), table.clone());
        Ok(())
    }

}