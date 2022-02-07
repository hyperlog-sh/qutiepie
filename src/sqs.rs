// third party
use aws_sdk_sqs::error::{ChangeMessageVisibilityError, DeleteMessageError, ReceiveMessageError};
use aws_sdk_sqs::model::Message as SqsMessage;
use aws_sdk_sqs::SdkError;
use snafu::prelude::*;

// internal
use crate::parser::*;

pub(crate) type ReceiveMessageResult = Result<Option<Vec<SqsMessage>>, SqsClientError>;

#[derive(Debug, Clone)]
pub enum SqsClient {
    Aws(aws_sdk_sqs::Client),
    #[cfg(any(test, feature = "mock"))]
    Mock(),
}

#[derive(Debug, Snafu)]
pub enum SqsClientError {
    #[snafu(display("failed to receive message(s) => {source}"))]
    ReceiveMessageError {
        #[snafu(source(from(SdkError<ReceiveMessageError>, Box::new)))]
        source: Box<SdkError<ReceiveMessageError>>,
    },

    #[snafu(display("failed to delete message with receipt_handle={receipt_handle} => {source}"))]
    DeleteMessageError {
        #[snafu(source(from(SdkError<DeleteMessageError>, Box::new)))]
        source: Box<SdkError<DeleteMessageError>>,
        receipt_handle: String,
    },

    #[snafu(display(
        "failed to change visibility for message with receipt_handle={receipt_handle} => {source}"
    ))]
    ChangeMessageVisibilityError {
        #[snafu(source(from(SdkError<ChangeMessageVisibilityError>, Box::new)))]
        source: Box<SdkError<ChangeMessageVisibilityError>>,
        receipt_handle: String,
    },
}

impl SqsClient {
    pub(crate) async fn receive_message(
        &self,
        queue_url: &str,
        visibility_timeout: VisibilityTimeout,
        wait_time_seconds: ReceiveWaitTime,
        max_number_of_messages: ReceiveBatchSize,
    ) -> ReceiveMessageResult {
        match self {
            SqsClient::Aws(client) => {
                let res = client
                    .receive_message()
                    .queue_url(queue_url)
                    .visibility_timeout(visibility_timeout.as_i32_seconds())
                    .wait_time_seconds(wait_time_seconds.as_i32_seconds())
                    .max_number_of_messages(max_number_of_messages.as_i32())
                    .send()
                    .await
                    .context(ReceiveMessageSnafu)?;
                Ok(res.messages)
            }
            #[cfg(any(test, feature = "mock"))]
            SqsClient::Mock() => unimplemented!(),
        }
    }

    pub(crate) async fn delete_message(
        &self,
        queue_url: &str,
        receipt_handle: &str,
    ) -> Result<(), SqsClientError> {
        match self {
            SqsClient::Aws(client) => {
                let _ = client
                    .delete_message()
                    .queue_url(queue_url)
                    .receipt_handle(receipt_handle)
                    .send()
                    .await
                    .context(DeleteMessageSnafu {
                        receipt_handle: receipt_handle.to_string(),
                    })?;
                Ok(())
            }
            #[cfg(any(test, feature = "mock"))]
            SqsClient::Mock() => Ok(()),
        }
    }

    pub(crate) async fn change_message_visibility(
        &self,
        queue_url: &str,
        receipt_handle: &str,
        visibility_timeout: VisibilityTimeout,
    ) -> Result<(), SqsClientError> {
        match self {
            SqsClient::Aws(client) => {
                let _ = client
                    .change_message_visibility()
                    .queue_url(queue_url)
                    .receipt_handle(receipt_handle)
                    .visibility_timeout(visibility_timeout.as_i32_seconds())
                    .send()
                    .await
                    .context(ChangeMessageVisibilitySnafu {
                        receipt_handle: receipt_handle.to_string(),
                    })?;
                Ok(())
            }
            #[cfg(any(test, feature = "mock"))]
            SqsClient::Mock() => Ok(()),
        }
    }
}
