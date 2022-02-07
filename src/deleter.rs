// third party
use snafu::prelude::*;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

// internal
use crate::consumer::{MessagePostProcessing, SqsConsumer};
use crate::counter::{CounterError, MessagesBeingProcessedCounter};
use crate::sqs::SqsClient;

#[derive(Debug, Snafu)]
pub enum DeleterError {
    CounterError { source: CounterError },
}

impl DeleterError {
    pub fn is_fatal(&self) -> bool {
        match self {
            DeleterError::CounterError { .. } => true,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MessageDeleter {
    client: SqsClient,
    queue_url: String,
    in_flight_counter: MessagesBeingProcessedCounter,
    rx: UnboundedReceiver<MessagePostProcessing>,
    errors_tx: UnboundedSender<DeleterError>,
}

impl MessageDeleter {
    pub(crate) fn new(
        client: SqsClient,
        queue_url: String,
        in_flight_counter: MessagesBeingProcessedCounter,
        rx: UnboundedReceiver<MessagePostProcessing>,
        errors_tx: UnboundedSender<DeleterError>,
    ) -> Self {
        Self {
            client,
            queue_url,
            in_flight_counter,
            rx,
            errors_tx,
        }
    }

    pub(crate) async fn listen(mut self) {
        while let Some(post_processing) = self.rx.recv().await {
            // Decrement counter before deleting messages.
            // Should be fine as SQS supports "a nearly unlimited number of API calls per second"
            // Env specific constraints could still be a problem. TODO: If so, run this decrement logic after successful deletion.
            match self.in_flight_counter.decrement() {
                Ok(_) => match post_processing {
                    MessagePostProcessing::Delete(metadata) => match metadata.receipt_handle {
                        None => {
                            tracing::warn!(
                                ?metadata,
                                warning = "No receipt handle to delete message"
                            )
                        }
                        Some(receipt_handle) => self.spawn_message_deleter(receipt_handle),
                    },
                    MessagePostProcessing::Reprocess(metadata) => {
                        tracing::debug!(?metadata, message = "Taking no action. Allowing message visibility to run out and be reprocessed.")
                    }
                },
                Err(e) => {
                    tracing::error!(
                        ?e,
                        message = "Error decrementing counter after message processing"
                    );
                    let _ = self
                        .errors_tx
                        .send(DeleterError::CounterError { source: e });
                }
            }
        }
    }

    fn spawn_message_deleter(&self, message_receipt_handle: String) {
        let client = self.client.clone();
        let queue_url = self.queue_url.clone();

        tokio::spawn(async move {
            // If delete message fails the message will be reprocessed again.
            // Because of the way SQS works this'll have to be handled by the application.
            // Ensuring that message processing is idempotent/nullipotent is a a requirement when working with SQS.
            // The sdk itself implements retries for specific cases so I don't think we should complicate logic here.
            // TODO: For now just emit a tracing event!

            let _ = client
                .delete_message(&queue_url, &message_receipt_handle)
                .await;
        });
    }
}
