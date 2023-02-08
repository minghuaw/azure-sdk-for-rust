//! Errors with transactions.

use crate::amqp::error::{AmqpTransactionError};

/// Type alias for the error type returned by the transaction API
pub type TransactionError = AmqpTransactionError;
