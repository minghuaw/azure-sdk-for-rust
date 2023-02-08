//! Implements transactional operations
//!
//! # Important
//!
//! Azure Service Bus doesn't retry an operation in case of an exception when the operation is in a
//! transaction scope.

use crate::{
    core::{
        TransactionProcessing
    },
    primitives::service_bus_received_message::ReceivedMessageLockToken,
    ServiceBusMessage, ServiceBusReceivedMessage,
};

use async_trait::async_trait;
use fe2o3_amqp::{
    link::{delivery::DeliveryInfo, IllegalLinkStateError},
    transaction::{Transaction, TransactionalRetirement},
};
use fe2o3_amqp_types::{
    messaging::{Modified, Outcome},
    primitives::OrderedMap,
};
use serde_amqp::Value;

use super::{
    amqp_message_batch::AmqpMessageBatch,
    amqp_message_converter::{
        batch_service_bus_messages_as_amqp_message, build_amqp_batch_from_messages, BatchEnvelope,
        SendableEnvelope,
    },
    amqp_receiver::{dead_letter_error, AmqpReceiver},
    amqp_sender::AmqpSender,
    error::{AmqpTransactionDispositionError, AmqpTransactionSendError, NotAcceptedError},
};

#[derive(Debug)]
pub(crate) struct AmqpTransaction<'t>(pub(crate) Transaction<'t>);

impl<'t> AmqpTransaction<'t> {
    async fn send_batch_envelope(
        &self,
        sender: &mut AmqpSender,
        batch: BatchEnvelope,
    ) -> Result<(), AmqpTransactionSendError> {
        // There will not be any retry, so we don't need to keep the batch around
        let outcome = match batch.sendable {
            SendableEnvelope::Single(sendable) => self.0.post(&mut sender.sender, sendable).await?,
            SendableEnvelope::Batch(sendable) => {
                let fut = self.0.post_batchable(&mut sender.sender, sendable).await?;
                fut.await?
            }
        };

        match outcome {
            Outcome::Accepted(_) => Ok(()),
            Outcome::Rejected(rejected) => Err(AmqpTransactionSendError::from(
                NotAcceptedError::Rejected(rejected),
            )),
            Outcome::Released(released) => Err(AmqpTransactionSendError::from(
                NotAcceptedError::Released(released),
            )),
            Outcome::Modified(modified) => Err(AmqpTransactionSendError::from(
                NotAcceptedError::Modified(modified),
            )),
            #[cfg(feature = "transaction")]
            Outcome::Declared(_) => {
                unreachable!("Declared is not expected outside txn-control links")
            }
        }
    }

    async fn complete_message(
        &self,
        receiver: &mut AmqpReceiver,
        delivery_info: DeliveryInfo,
    ) -> Result<(), IllegalLinkStateError> {
        self.0.accept(&mut receiver.receiver, delivery_info).await?;
        Ok(())
    }

    async fn abandon_message(
        &self,
        receiver: &mut AmqpReceiver,
        delivery_info: DeliveryInfo,
        properties_to_modify: Option<OrderedMap<String, Value>>,
    ) -> Result<(), IllegalLinkStateError> {
        let message_annotations =
            properties_to_modify.map(|map| map.into_iter().map(|(k, v)| (k.into(), v)).collect());
        let modified = Modified {
            delivery_failed: None,
            undeliverable_here: None,
            message_annotations,
        };

        self.0
            .modify(&mut receiver.receiver, delivery_info, modified)
            .await?;
        Ok(())
    }

    async fn dead_letter_message(
        &self,
        receiver: &mut AmqpReceiver,
        delivery_info: DeliveryInfo,
        dead_letter_reason: Option<String>,
        dead_letter_error_description: Option<String>,
        properties_to_modify: Option<OrderedMap<String, Value>>,
    ) -> Result<(), IllegalLinkStateError> {
        let error = dead_letter_error(
            dead_letter_reason,
            dead_letter_error_description,
            properties_to_modify,
        );

        self.0
            .reject(&mut receiver.receiver, delivery_info, error)
            .await?;
        Ok(())
    }

    async fn defer_message(
        &self,
        receiver: &mut AmqpReceiver,
        delivery_info: DeliveryInfo,
        properties_to_modify: Option<OrderedMap<String, Value>>,
    ) -> Result<(), IllegalLinkStateError> {
        let message_annotations =
            properties_to_modify.map(|map| map.into_iter().map(|(k, v)| (k.into(), v)).collect());
        let modified = Modified {
            delivery_failed: None,
            undeliverable_here: Some(true),
            message_annotations,
        };

        self.0
            .modify(&mut receiver.receiver, delivery_info, modified)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<'t> TransactionProcessing for AmqpTransaction<'t> {
    type Sender = AmqpSender;
    type SendError = AmqpTransactionSendError;
    type MessageBatch = AmqpMessageBatch;
    type Receiver = AmqpReceiver;
    type DispositionError = AmqpTransactionDispositionError;

    async fn send(
        &self,
        sender: &mut Self::Sender,
        messages: impl Iterator<Item = ServiceBusMessage> + ExactSizeIterator + Send,
    ) -> Result<(), Self::SendError> {
        match batch_service_bus_messages_as_amqp_message(messages, false) {
            Some(envelope) => {
                self.send_batch_envelope(sender, envelope).await?;
                Ok(())
            }
            None => Ok(()),
        }
    }

    async fn send_batch(
        &self,
        sender: &mut Self::Sender,
        message_batch: Self::MessageBatch,
    ) -> Result<(), Self::SendError> {
        match build_amqp_batch_from_messages(message_batch.messages.into_iter(), false) {
            Some(envelope) => {
                self.send_batch_envelope(sender, envelope).await?;
                Ok(())
            }
            None => Ok(()),
        }
    }


    async fn complete(
        &self,
        receiver: &mut Self::Receiver,
        message: &ServiceBusReceivedMessage,
        _session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        match &message.lock_token {
            ReceivedMessageLockToken::LockToken(_lock_token) => {
                Err(AmqpTransactionDispositionError::TransactionalRequestResponseNotImplemented)
            }
            ReceivedMessageLockToken::Delivery { delivery_info, .. } => {
                self.complete_message(receiver, delivery_info.clone())
                    .await?;
                Ok(())
            }
        }
    }

    async fn abandon(
        &self,
        receiver: &mut Self::Receiver,
        message: &ServiceBusReceivedMessage,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        _session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        match &message.lock_token {
            ReceivedMessageLockToken::LockToken(_lock_token) => {
                Err(AmqpTransactionDispositionError::TransactionalRequestResponseNotImplemented)
            }
            ReceivedMessageLockToken::Delivery { delivery_info, .. } => {
                self.abandon_message(receiver, delivery_info.clone(), properties_to_modify)
                    .await?;
                Ok(())
            }
        }
    }

    async fn dead_letter(
        &self,
        receiver: &mut Self::Receiver,
        message: &ServiceBusReceivedMessage,
        dead_letter_reason: Option<String>,
        dead_letter_error_description: Option<String>,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        _session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        match &message.lock_token {
            ReceivedMessageLockToken::LockToken(_lock_token) => {
                Err(AmqpTransactionDispositionError::TransactionalRequestResponseNotImplemented)
            }
            ReceivedMessageLockToken::Delivery { delivery_info, .. } => {
                self.dead_letter_message(
                    receiver,
                    delivery_info.clone(),
                    dead_letter_reason,
                    dead_letter_error_description,
                    properties_to_modify,
                )
                .await?;
                Ok(())
            }
        }
    }

    async fn defer(
        &self,
        receiver: &mut Self::Receiver,
        message: &ServiceBusReceivedMessage,
        properties_to_modify: Option<OrderedMap<String, Value>>,
        _session_id: Option<&str>,
    ) -> Result<(), Self::DispositionError> {
        match &message.lock_token {
            ReceivedMessageLockToken::LockToken(_lock_token) => {
                Err(AmqpTransactionDispositionError::TransactionalRequestResponseNotImplemented)
            }
            ReceivedMessageLockToken::Delivery { delivery_info, .. } => {
                self.defer_message(receiver, delivery_info.clone(), properties_to_modify)
                    .await?;
                Ok(())
            }
        }
    }
}
