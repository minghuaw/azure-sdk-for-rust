//! Receiver for Service Bus queues and subscriptions.
use serde_amqp::{primitives::OrderedMap, Value};

use crate::{amqp::amqp_receiver::AmqpReceiver, sealed::Sealed};

cfg_either_rustls_or_native_tls! {
    pub mod service_bus_receiver;
    pub mod service_bus_session_receiver;
}

pub mod service_bus_receive_mode;

/// The dead letter options.
///
/// Default values are `None` for all fields
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeadLetterOptions {
    /// The reason for dead-lettering the message
    pub dead_letter_reason: Option<String>,

    /// The error description for dead-lettering the message
    pub dead_letter_error_description: Option<String>,

    /// The properties to modify on the message
    pub properties_to_modify: Option<OrderedMap<String, Value>>,
}

/// A trait for transaction settling of messages.
pub trait MaybeSessionReceiver: Sealed {

    /// Get a mutable reference to the inner AMQP receiver and the session id.
    fn get_inner_mut_and_session_id(&mut self) -> (&mut AmqpReceiver, Option<&str>);
}
