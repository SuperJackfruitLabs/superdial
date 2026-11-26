//! superdial-agent: the agent's side of a superdial call.
//!
//! The AgentPod hub asks it, over the C6 call API ([`api`]), to join a bridge agent's call
//! with that agent's call device. It hears each thing the caller says ([`turns`]), has it
//! transcribed ([`speech`]), sends the transcript to the caller's call device (C2), and speaks
//! the reply the hub hands back ([`textprep`], [`speech`]). It never reads room timelines and
//! discards room keys ([`session`]).

pub mod api;
pub mod audio;
pub mod config;
pub mod conversation;
pub mod hub;
pub mod speech;
pub mod textprep;
pub mod turns;
