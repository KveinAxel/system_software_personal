use std::rc::Rc;

use uuid::Uuid;

use crate::table::field::Field;
use crate::table::table_manager::TableManager;
use crate::util::error::Error;

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
    table_manager: Rc<TableManager>,
    pub(crate) table_name: String,
    pub(crate) next_table: Uuid,
    pub(crate) self_uuid: Uuid,
    status: Status,
    fields: Vec<Rc<Field>>,
}

impl Table {
    pub fn new_orphan_table(table_manager: Rc<TableManager>, table_name: String) -> Table {
        Table {
            table_manager,
            table_name,
            next_table: Uuid::nil(),
            self_uuid: Uuid::nil(),
            status: Status::NORMAL,
            fields: Vec::<Rc<Field>>::new(),
        }
    }

    pub fn load_table(tbm: &TableManager, uuid: Uuid) -> Result<Box<Table>, Error> {
        Err(Error::UnexpectedError)
    }

    pub fn insert(&mut self, field: Field) -> Result<(), Error> {
        Err(Error::UnexpectedError)
    }

    pub fn create_table(tbm: &TableManager, uuid: Uuid, table: Table) -> Result<Table, Error> {
        Err(Error::UnexpectedError)
    }
}

impl Clone for Table {
    fn clone(&self) -> Self {
        let mut fields = Vec::<Rc<Field>>::new();
        for i in &self.fields {
            fields.push(i.clone());
        }
        Table {
            table_manager: self.table_manager.clone(),
            table_name: self.table_name.clone(),
            next_table: self.next_table.clone(),
            self_uuid: self.self_uuid.clone(),
            status: self.status.clone(),
            fields,
        }
    }
}
