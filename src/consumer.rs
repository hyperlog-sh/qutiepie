// std lib
use std::future::Future;

// third party
use aws_sdk_sqs::model::Message as SqsMessage;
use snafu::prelude::*;

// internal
use crate::batch_poller::{BatchPoller, BatchPollerError};
use crate::builder::SqsConsumerBuilder;
use crate::parser::*;
use crate::sqs::SqsClient;

#[derive(Debug)]
pub enum MessagePostProcessing {
    Delete(MessageMetadata),
    Reprocess(MessageMetadata),
}

/// These are Option types only because the generated AWS SDK types are options.
/// When the SDK hits v1 these should be corrected to be non-option types.
#[derive(Debug)]
pub struct MessageMetadata {
    pub message_id: Option<String>,
    pub receipt_handle: Option<String>,
}

#[derive(Debug, Snafu)]
pub enum ConsumerError {
    PollError { source: BatchPollerError },
}

// TODO: Make the heartbeat interval optional.
#[derive(Debug)]
pub struct SqsConsumer<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Send + 'static,
{
    pub client: SqsClient,
    pub queue_url: String,
    pub visibility_timeout: VisibilityTimeout,
    pub heartbeat_interval: HeartbeatInterval,
    pub receive_batch_size: ReceiveBatchSize,
    pub receive_message_wait_time: ReceiveWaitTime,
    pub processing_concurrency_limit: ProcessingConcurrencyLimit,
    pub message_processor: M,
}

impl<M, F> SqsConsumer<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Send + 'static,
{
    pub async fn poll(self) -> Result<(), ConsumerError> {
        let poller = BatchPoller::new(self);

        poller.start().await.context(PollSnafu)
    }

    pub fn builder() -> SqsConsumerBuilder<M, F> {
        SqsConsumerBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqs::{MockClient, SqsClient};
    use rand::prelude::*;
    use std::sync::{Arc, Mutex};

    /*
    This test isn't really testing anything. It's what I was using to understand the API.
    It's also not in the external `tests` folder because rust-analyzer wasn't able to
    figure out the conditional compilation stuff.
    */
    /*
    This test can fail sometimes because we don't have any graceful shutdown logic.
    */
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn runs_without_panic() {
        #[allow(clippy::mutex_atomic)]
        let processed_count = Arc::new(Mutex::new(0));
        let processor = {
            let processed_count = processed_count.clone();
            |m: SqsMessage| async move {
                let processing_time = rand::thread_rng().gen_range(50..=1000);
                tokio::time::sleep(tokio::time::Duration::from_millis(processing_time)).await;
                let mut processed_count = processed_count.lock().unwrap();
                *processed_count += 1;

                MessagePostProcessing::Delete(MessageMetadata {
                    message_id: m.message_id,
                    receipt_handle: m.receipt_handle,
                })
            }
        };

        let client = MockClient::new("mock-message-body".to_string());

        let consumer = SqsConsumer::builder()
            .client(SqsClient::Mock(client.clone()))
            .queue_url("mock-queue-url".to_string())
            .message_processor(processor)
            .visibility_timeout_seconds(10)
            .expect("invalid visibility timeout")
            .receive_batch_size(10)
            .expect("invalid batch size")
            .receive_message_wait_time(2)
            .expect("invalid receive message wait time")
            .heartbeat_interval_seconds(5)
            .message_processing_concurrency_limit(100)
            .build()
            .expect("invalid consumer");

        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(60), consumer.poll()).await;

        let processed_count = processed_count.lock().unwrap();
        println!("processed_count: {}", processed_count);
        println!("sent_count: {}", client.get_sent_count());
        assert_eq!(*processed_count, client.get_sent_count());
    }
}
