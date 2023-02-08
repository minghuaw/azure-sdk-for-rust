//! Transaction traits for Service Bus

use std::future::Future;

use async_trait::async_trait;
use fe2o3_amqp_types::primitives::OrderedMap;
use serde_amqp::Value;

use crate::{ServiceBusMessage, ServiceBusReceivedMessage};

#[async_trait]
pub trait TransactionClient {
    type Scope<'t>;
    type TransactionError: std::error::Error;

    async fn create_and_run_transaction_scope<F, Fut, O>(
        &mut self,
        op: F
    ) -> Result<O, Self::TransactionError>
    where
        F: FnOnce(Self::Scope<'_>) -> Fut + Send,
        Fut: Future<Output = Result<O, Self::TransactionError>> + Send;
}

#[async_trait]
pub trait TransactionProcessing {
    type Sender;
    type SendError;
    type MessageBatch;

    type Receiver;
    type DispositionError;

    async fn send(
        &self,
        sender: &mut Self::Sender,
        messages: impl Iterator<Item = ServiceBusMessage> + ExactSizeIterator + Send,
    ) -> Result<(), Self::SendError>;

    async fn send_batch(
        &self,
        sender: &mut Self::Sender,
        message_batch: Self::MessageBatch,
    ) -> Result<(), Self::SendError>;


    async fn complete(
        &self,
        receiver: &mut Self::Receiver,
        message: &ServiceBusReceivedMessage,
        session_id: Option<&str>
    ) -> Result<(), Self::DispositionError>;

    async fn abandon(
        &self,
        receiver: &mut Self::Receiver,
        message: &ServiceBusReceivedMessage,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>
    ) -> Result<(), Self::DispositionError>;

    async fn dead_letter(
        &self,
        receiver: &mut Self::Receiver,
        message: &ServiceBusReceivedMessage,
        dead_letter_reason: Option<String>,
        dead_letter_error_description: Option<String>,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>
    ) -> Result<(), Self::DispositionError>;

    async fn defer(
        &self,
        receiver: &mut Self::Receiver,
        message: &ServiceBusReceivedMessage,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        session_id: Option<&str>
    ) -> Result<(), Self::DispositionError>;
}
