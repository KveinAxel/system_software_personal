#[derive(Debug)]
pub enum Error {
    KeyNotFound,
    KeyAlreadyExists,
    TableAlreadyExists,
    UnexpectedError,
    TryFromSliceError(&'static str),
    UTF8Error,
    NotInBufferError,
    TableNotFound,
}

impl std::convert::From<std::io::Error> for Error {
    fn from(_e: std::io::Error) -> Error {
        Error::UnexpectedError
    }
}
