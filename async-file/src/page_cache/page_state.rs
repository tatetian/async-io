#[cfg(sgx)]
use std::prelude::v1::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageState {
    Uninit,
    UpToDate,
    Dirty,
    Fetching,
    Flushing,
}
