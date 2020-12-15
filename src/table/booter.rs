use std::path::Path;
use crate::error::Error;
use uuid::Uuid;
use crate::util::error::Error;

pub struct Booter {

}

impl Booter {
    pub fn create(path: Path) ->Result<Booter, Error> {
        Ok(Booter {

        })
    }

    pub fn open(path: Path) ->Result<Booter, Error> {
        Ok(Booter {

        })
    }

    pub fn load(&self) -> Result<Uuid, Error> {

    }

    pub fn update(&mut self, uuid: Uuid) -> Result<(), Error> {

    }
}