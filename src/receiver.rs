// third party
use snafu::prelude::*;
use tokio::sync::mpsc::UnboundedSender;

// internal
use crate::parser::*;
use crate::sqs::{ReceiveMessageResult, SqsClient, SqsClientError};

#[derive(Debug)]
pub(crate) struct SqsReceive {
    pub(crate) client: SqsClient,
    pub(crate) queue_url: String,
    pub(crate) visibility_timeout: VisibilityTimeout,
    pub(crate) receive_batch_size: ReceiveBatchSize,
    pub(crate) receive_message_wait_time: ReceiveWaitTime,
    pub(crate) tx: UnboundedSender<ReceiveMessageResult>,
}

impl SqsReceive {
    pub(crate) fn run(self) {
        tokio::spawn(async move {
            let res = self
                .client
                .receive_message(
                    &self.queue_url,
                    self.visibility_timeout,
                    self.receive_message_wait_time,
                    self.receive_batch_size,
                )
                .await;

            match self.tx.send(res) {
                Ok(_) => (),
                Err(e) => tracing::error!("failed to send message to channel: {}", e),
            }
        });
    }
}
