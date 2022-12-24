//! Implements processor for Service Bus.

use crate::ServiceBusReceivedMessage;

#[derive(Debug)]
pub struct ProcessMessageEventArgs<'a> {
    pub message: ServiceBusReceivedMessage,
    pub entity_path: &'a str,
}
