// std lib
use std::future::Future;

// third party
use aws_sdk_sqs::model::Message as SqsMessage;
use snafu::prelude::*;
use tokio::sync::oneshot;
use tokio::time;

// internal
use crate::batch_poller::BatchPoller;
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

// TODO: Make the heartbeat interval optional.
#[derive(Debug)]
pub struct SqsConsumer<M, F>
where
    M: Fn(SqsMessage) -> F + Clone + Sync + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Sync + Send + 'static,
{
    pub client: SqsClient,
    pub queue_url: String,
    pub visibility_timeout: VisibilityTimeout,
    pub heartbeat_interval: HeartbeatInterval,
    pub receive_batch_size: ReceiveBatchSize,
    pub receive_message_wait_time: ReceiveWaitTime,
    pub processing_concurrency_limit: ProcessingConcurrencyLimit,
    pub message_processor: M,

    /// This is a quick and dirty shutdown enabling mechanism.
    /// TODO: Wait until in progress messages are processed before shutting down.
    /// In any case, a SQS consumer HAS to handle duplicate message processing at the application level.
    pub shutdown_duration: time::Duration,
}

type ShutdownSignal = oneshot::Sender<()>;

impl<M, F> SqsConsumer<M, F>
where
    M: Fn(SqsMessage) -> F + Clone + Sync + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Sync + Send + 'static,
{
    pub async fn poll(self) -> ShutdownSignal {
        let (tx, rx) = oneshot::channel();

        let poller = BatchPoller::new(self);

        tokio::spawn(poller.start(rx));

        tx
    }
}
