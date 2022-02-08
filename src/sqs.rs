// third party
use aws_sdk_sqs::error::{ChangeMessageVisibilityError, DeleteMessageError, ReceiveMessageError};
use aws_sdk_sqs::model::Message as SqsMessage;
use aws_sdk_sqs::SdkError;
use snafu::prelude::*;
use tracing::instrument;

// internal
use crate::parser::*;

pub(crate) type ReceiveMessageResult = Result<Option<Vec<SqsMessage>>, SqsClientError>;

#[derive(Debug, Clone)]
pub enum SqsClient {
    Aws(aws_sdk_sqs::Client),
    #[cfg(any(test, feature = "mock"))]
    Mock(MockClient),
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
    #[instrument(skip(self))]
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
            SqsClient::Mock(client) => {
                let res = client
                    .receive_message(
                        visibility_timeout,
                        wait_time_seconds,
                        max_number_of_messages,
                    )
                    .await;

                Ok(res)
            }
        }
    }

    #[instrument(skip(self))]
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
            SqsClient::Mock(_) => {
                MockClient::simulate_network_latency().await;
                Ok(())
            }
        }
    }

    #[instrument(skip(self))]
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
            SqsClient::Mock(_) => {
                MockClient::simulate_network_latency().await;
                Ok(())
            }
        }
    }
}

#[cfg(any(test, feature = "mock"))]
use rand::distributions::{Distribution, Uniform};
#[cfg(any(test, feature = "mock"))]
use rand::prelude::*;
#[cfg(any(test, feature = "mock"))]
use std::sync::{Arc, Mutex};

#[cfg(any(test, feature = "mock"))]
#[derive(Debug)]
pub struct MockClient {
    /// For now just use the same body for all messages.
    /// We could improve this to use either a pool or maybe random generation.
    pub message_body: String,

    messages_sent: Arc<Mutex<usize>>,
}

#[allow(clippy::mutex_atomic)]
#[cfg(any(test, feature = "mock"))]
impl MockClient {
    pub fn new(message_body: String) -> Self {
        Self {
            message_body,
            messages_sent: Arc::new(Mutex::new(0)),
        }
    }

    pub fn get_sent_count(&self) -> usize {
        *self.messages_sent.lock().unwrap()
    }

    async fn receive_message(
        &self,
        _visibility_timeout: VisibilityTimeout,
        wait_time_seconds: ReceiveWaitTime,
        max_number_of_messages: ReceiveBatchSize,
    ) -> Option<Vec<SqsMessage>> {
        let send_max_messages: bool = rand::thread_rng().gen();

        let messages = if send_max_messages {
            self.get_max_messages(max_number_of_messages)
        } else {
            self.get_rand_msgs_after_wait_time(max_number_of_messages, wait_time_seconds)
                .await
        };

        Self::simulate_network_latency().await;

        if messages.is_empty() {
            None
        } else {
            let mut counter_guard = self.messages_sent.lock().unwrap();
            *counter_guard += messages.len();
            Some(messages)
        }
    }

    fn get_max_messages(&self, max: ReceiveBatchSize) -> Vec<SqsMessage> {
        let mut messages = Vec::with_capacity(max.as_usize());

        // Store sent message ids unique to each message and verify in the test
        for _ in 0..max.as_usize() {
            let m = SqsMessage::builder()
                .set_message_id(Some("mock-message-id".to_string()))
                .set_receipt_handle(Some("mock-receipt-handle".to_string()))
                .set_body(Some(self.message_body.clone()))
                .build();
            messages.push(m);
        }

        messages
    }

    async fn get_rand_msgs_after_wait_time(
        &self,
        max: ReceiveBatchSize,
        wait_time: ReceiveWaitTime,
    ) -> Vec<SqsMessage> {
        let num_messages = Uniform::new(0, max.as_usize()).sample(&mut rand::thread_rng());

        let mut messages = Vec::with_capacity(num_messages);

        for _ in 0..num_messages {
            let m = SqsMessage::builder()
                .set_message_id(Some("mock-message-id".to_string()))
                .set_receipt_handle(Some("mock-receipt-handle".to_string()))
                .set_body(Some(self.message_body.clone()))
                .build();
            messages.push(m);
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(wait_time.as_u64_seconds())).await;
        messages
    }

    async fn simulate_network_latency() {
        let delay = rand::thread_rng().gen_range(10..=100);

        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
    }
}

#[cfg(any(test, feature = "mock"))]
impl Clone for MockClient {
    fn clone(&self) -> Self {
        Self {
            message_body: self.message_body.clone(),
            messages_sent: Arc::clone(&self.messages_sent),
        }
    }
}
