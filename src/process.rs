// std lib
use std::future::Future;

// third party
use aws_sdk_sqs::model::Message as SqsMessage;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time;
use tracing::instrument;

// internal
use crate::consumer::MessagePostProcessing;
use crate::parser::*;
use crate::sqs::SqsClient;

// Make this an enum type when/if supporting processing without heartbeats
#[derive(Debug)]
pub(crate) struct ProcessWithHeartbeat<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Send + 'static,
{
    pub(crate) client: SqsClient,
    pub(crate) queue_url: String,
    pub(crate) visibility_timeout: VisibilityTimeout,
    pub(crate) heartbeat_interval: HeartbeatInterval,
    pub(crate) message_processor: M,
    pub(crate) message: SqsMessage,
    pub(crate) post_processing_tx: UnboundedSender<MessagePostProcessing>,
}

impl<M, F> ProcessWithHeartbeat<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Send + 'static,
{
    pub(crate) fn process(self) {
        tokio::spawn(async move {
            let (tx, mut rx) = mpsc::unbounded_channel();

            let mut heartbeat = time::interval(time::Duration::from_secs(
                self.heartbeat_interval.as_u64_seconds(),
            ));
            heartbeat.tick().await; // First tick is immediate

            Self::spawn_message_processor(self.message_processor, self.message.clone(), tx);
            loop {
                tokio::select! {
                    biased;
                    processing_result = rx.recv() => match processing_result {
                       Some(pr) => {
                           match self.post_processing_tx.send(pr) {
                                 Ok(_) => (),
                                 Err(e) => tracing::warn!("Failed to send message post-processing result: {}", e),
                           };
                           break;
                       },
                       None => tracing::warn!("message processor sender dropped")
                    },
                    _ = heartbeat.tick() => {
                        match self.message.receipt_handle {
                            Some(ref receipt_handle) => {
                                let res = self.client.change_message_visibility(
                                    &self.queue_url,
                                    receipt_handle,
                                    self.visibility_timeout,
                                ).await;
                                match res {
                                    Ok(_) => (),
                                    Err(e) => tracing::warn!("Failed to change message visibility: {}", e),
                                }
                            }
                            None => {
                                tracing::warn!("message has no receipt handle");
                            }
                        }
                    },
                }
            }
        });
    }

    #[instrument(skip(message_processor, message, tx))]
    fn spawn_message_processor(
        message_processor: M,
        message: SqsMessage,
        tx: UnboundedSender<MessagePostProcessing>,
    ) {
        tokio::spawn(async move {
            let res = message_processor(message).await;

            match tx.send(res) {
                Ok(_) => (),
                Err(e) => tracing::warn!("Failed to send message post processing result: {:?}", e),
            }
        });
    }
}
