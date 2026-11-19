//! The WeChat channel implementation inside the single mesh plugin.
//!
//! Everything here is a module of the one plugin: the lease owner loop, the
//! bridge, the media tools and the phone commands share the process-wide
//! identity (`crate::mesh::Channel`) instead of holding a second node_id/epoch.
//! There is no second dylib, so there are no cross-dylib coordination traps.

pub mod bridge;
pub mod commands;
pub mod error;
pub mod channel;
pub mod ilink;
pub mod media;
pub mod outbound;
pub mod owner;
pub mod phone;
pub mod qr;
pub mod state;
