//! Transaction for Service Bus
//!
//! The operations that can be performed within a transaction scope are as follows:
//!
//! - [x] Send
//! - [x] Complete
//! - [x] Abandon
//! - [x] Deadletter
//! - [x] Defer
//! - [ ] Renew lock

use fe2o3_amqp::transaction::{TransactionDischarge, ControllerSendError};
use fe2o3_amqp_types::primitives::OrderedMap;
use serde_amqp::Value;

use crate::{
    amqp::{
        amqp_transaction::AmqpTransaction,
        error::{AmqpTransactionDispositionError, AmqpTransactionSendError},
    },
    core::TransactionProcessing,
    ServiceBusMessage, ServiceBusMessageBatch, ServiceBusReceivedMessage, ServiceBusSender, receiver::{MaybeSessionReceiver, DeadLetterOptions},
};

/// A Service Bus transaction scope
#[derive(Debug)]
pub struct TransactionScope<'t> {
    txn: AmqpTransaction<'t>,
}

impl<'t> TransactionScope<'t> {
    pub(crate) fn new(txn: AmqpTransaction<'t>) -> Self {
        Self { txn }
    }

    /// Commit the transaction
    pub async fn commit(self) -> Result<(), ControllerSendError> {
        self.txn.0.commit().await
    }

    /// Rollback the transaction
    pub async fn rollback(self) -> Result<(), ControllerSendError> {
        self.txn.0.rollback().await
    }

    /// Send a message within the transaction scope
    pub async fn send_message(
        &self,
        sender: &mut ServiceBusSender,
        message: impl Into<ServiceBusMessage>,
    ) -> Result<(), AmqpTransactionSendError> {
        let messages = std::iter::once(message.into());
        self.txn.send(sender.as_mut(), messages).await
    }

    /// Send a set of messages within the transaction scope
    pub async fn send_messages<M, I>(
        &self,
        sender: &mut ServiceBusSender,
        messages: M,
    ) -> Result<(), AmqpTransactionSendError>
    where
        M: IntoIterator<Item = I>,
        M::IntoIter: ExactSizeIterator + Send,
        I: Into<ServiceBusMessage>,
    {
        let messages = messages.into_iter().map(|m| m.into());
        self.txn.send(sender.as_mut(), messages).await
    }

    /// Send a [`ServiceBusMessageBatch`] within the transaction scope
    pub async fn send_message_batch(
        &self,
        sender: &mut ServiceBusSender,
        batch: ServiceBusMessageBatch,
    ) -> Result<(), AmqpTransactionSendError> {
        self.txn.send_batch(sender.as_mut(), batch.inner).await
    }

    /// Complete a message within the transaction scope
    pub async fn complete_message(
        &self,
        receiver: &mut impl MaybeSessionReceiver,
        message: impl AsRef<ServiceBusReceivedMessage>,
    ) -> Result<(), AmqpTransactionDispositionError> {
        let (amqp_receiver, session_id) = receiver.get_inner_mut_and_session_id();
        self.txn.complete(amqp_receiver, message.as_ref(), session_id).await?;
        Ok(())
    }

    /// Abandon a message within the transaction scope
    pub async fn abandon_message(
        &self,
        receiver: &mut impl MaybeSessionReceiver,
        message: impl AsRef<ServiceBusReceivedMessage>,
        properties_to_modify: Option<OrderedMap<String, Value>>,
    ) -> Result<(), AmqpTransactionDispositionError> {
        let (amqp_receiver, session_id) = receiver.get_inner_mut_and_session_id();
        self.txn
            .abandon(amqp_receiver, message.as_ref(), properties_to_modify, session_id)
            .await?;
        Ok(())
    }

    /// Deadletter a message within the transaction scope
    pub async fn dead_letter_message(
        &self,
        receiver: &mut impl MaybeSessionReceiver,
        message: impl AsRef<ServiceBusReceivedMessage>,
        options: DeadLetterOptions,
    ) -> Result<(), AmqpTransactionDispositionError> {
        let (amqp_receiver, session_id) = receiver.get_inner_mut_and_session_id();
        self.txn
            .dead_letter(
                amqp_receiver,
                message.as_ref(),
                options.dead_letter_reason,
                options.dead_letter_error_description,
                options.properties_to_modify,
                session_id,
            )
            .await?;
        Ok(())
    }

    /// Defer a message within the transaction scope
    pub async fn defer_message(
        &self,
        receiver: &mut impl MaybeSessionReceiver,
        message: impl AsRef<ServiceBusReceivedMessage>,
        properties_to_modify: Option<OrderedMap<String, Value>>,
    ) -> Result<(), AmqpTransactionDispositionError> {
        let (amqp_receiver, session_id) = receiver.get_inner_mut_and_session_id();
        self.txn
            .defer(amqp_receiver, message.as_ref(), properties_to_modify, session_id)
            .await?;
        Ok(())
    }
}
