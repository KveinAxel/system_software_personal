use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub mod index;
pub mod util;
pub mod data_item;
pub mod page;
pub mod table;
pub mod test;


fn main() {
    let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

    let dialect = GenericDialect {};

    let _ast = &Parser::parse_sql(&dialect, sql).unwrap()[0];



    // println!("{:#?}", ast);

}