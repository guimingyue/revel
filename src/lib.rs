use std::fmt::{Display, Formatter};
use std::io;
use std::sync::PoisonError;
use crate::error::Error;

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

pub mod db;
pub mod error;

mod memtable;
mod log;
mod fs;
mod filename;