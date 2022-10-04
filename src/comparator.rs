use std::cmp::Ordering;
use crate::slice::Slice;

pub trait Comparator {

    fn compare(&self, a: &Slice, b: &Slice) -> Ordering;

    fn name(&self) -> &str;
}