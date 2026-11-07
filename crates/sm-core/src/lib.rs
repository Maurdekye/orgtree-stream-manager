//! Stream manager core. Pure logic, no I/O. Times are seconds as `f64`.
//!
//! Section numbers (§N) refer to the approved plan, "Stream manager: design plan" (rev 6).

pub mod afk;
pub mod sleep;
pub mod credit;
pub mod curve;
pub mod donation;
pub mod effects;
pub mod gate;
pub mod metrics;
pub mod outbound;
pub mod pipeline;
pub mod protocol;
pub mod text;
