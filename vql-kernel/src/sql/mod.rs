mod ast;
mod ddl_parser;
mod splitter;

pub(crate) use ast::{CreateModel, CreateStream, CreateTable, ShowKind, VqlStatement};
pub(crate) use ddl_parser::parse_statement;
pub use splitter::{ends_with_statement_terminator, split_statements};
