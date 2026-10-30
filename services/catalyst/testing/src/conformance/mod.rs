//! The API conformance fixture semantics (ported from
//! `catalyrst/crates/catalyrst-conformance`): the fixture format and its
//! volatility scrub, the JSON diff and the per-section rules. ONE
//! implementation for the in-process stage-1 replay (`content::tests`) and
//! the live replay engine in `tests/support/conformance/` (`replay.rs`,
//! `routes.rs`), which re-exports these modules.

pub mod compare;
pub mod diff;
pub mod fixture;
pub mod volatility;
