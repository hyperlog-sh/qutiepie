// std lib
use std::future::Future;
use std::ops::Div;

// third party
use aws_sdk_sqs::model::Message as SqsMessage;
use futures::future;
use snafu::prelude::*;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::instrument;

// internal
use crate::consumer::{MessagePostProcessing, SqsConsumer};
use crate::counter::{CounterError, MessagesBeingProcessedCounter};
use crate::deleter::{DeleterError, MessageDeleter};
use crate::parser::*;
use crate::process::ProcessWithHeartbeat;
use crate::receiver::SqsReceive;

#[derive(Debug, Snafu)]
pub enum BatchPollerError {
    CounterError { source: CounterError },
    DeleterError { source: DeleterError },
    InvalidLeftoverBatchSize { source: ReceiveBatchSizeError },
    Unexpected { message: String },
}

impl BatchPollerError {
    fn is_fatal(&self) -> bool {
        match self {
            BatchPollerError::CounterError { .. } => true,
            BatchPollerError::DeleterError { source } => {
                matches!(source, DeleterError::CounterError { .. })
            }
            BatchPollerError::InvalidLeftoverBatchSize { .. } => true,
            BatchPollerError::Unexpected { .. } => false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BatchPoller<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Send + 'static,
{
    consumer: SqsConsumer<M, F>,
    in_flight_counter: MessagesBeingProcessedCounter,
    message_buffer: Vec<SqsMessage>,
}

impl<M, F> BatchPoller<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Send + 'static,
{
    pub(crate) fn new(consumer: SqsConsumer<M, F>) -> Self {
        let in_flight_counter =
            MessagesBeingProcessedCounter::new(consumer.processing_concurrency_limit.as_usize());
        let message_buffer = vec![];

        Self {
            consumer,
            in_flight_counter,
            message_buffer,
        }
    }

    #[instrument(skip(self))]
    pub(crate) async fn start(mut self) -> Result<(), BatchPollerError> {
        let (post_processing_tx, post_processing_rx) = mpsc::unbounded_channel();
        let (deleter_err_tx, mut deleter_err_rx) = mpsc::unbounded_channel();
        let (poll_err_tx, mut poll_err_rx) = mpsc::unbounded_channel();

        self.spawn_finish_line(post_processing_rx, deleter_err_tx);

        loop {
            self.batch_poll(poll_err_tx.clone(), post_processing_tx.clone())
                .await;

            tokio::select! {
                biased;
                de = deleter_err_rx.recv() => match de {
                    Some(err) => {
                        if err.is_fatal() {
                            tracing::error!(?err, "Fatal error in deleter; skipping graceful shutdown");
                            return Err(BatchPollerError::DeleterError { source: err });
                        } else {
                            tracing::warn!(?err, "Non-fatal error in deleter");
                            return Ok(())
                        }
                    }
                    None => {
                        tracing::warn!("Deleter error channel closed");
                        return Err(BatchPollerError::Unexpected { message: "Deleter error channel closed".to_string() })
                    }
                },
                pe = poll_err_rx.recv() => match pe {
                    Some(err) => {
                        if err.is_fatal() {
                            tracing::error!(?err, "Fatal error in poller; skipping graceful shutdown");
                            return Err(err)
                        } else {
                            tracing::warn!(?err, "Non-fatal error in poller");
                            return Ok(())
                        }
                    }
                    None => {
                        tracing::warn!("Poller error channel closed");
                        return Err(BatchPollerError::Unexpected { message: "Poller error channel closed".to_string() })
                    }
                },

                // The idea is that with a biased select loop and cancellation safe futures this ensures that
                // the select does not await on receiving errors but will match when they do resolve.
                _ = future::ready(1) => {
                    tracing::debug!("no errors so continue polling");
                    continue
                },
            }
        }
    }

    #[instrument(skip(self, rx, errors_tx))]
    fn spawn_finish_line(
        &self,
        rx: UnboundedReceiver<MessagePostProcessing>,
        errors_tx: UnboundedSender<DeleterError>,
    ) {
        let client = self.consumer.client.clone();
        let queue_url = self.consumer.queue_url.clone();
        let in_flight_counter = self.in_flight_counter.clone();

        let deleter = MessageDeleter::new(client, queue_url, in_flight_counter, rx, errors_tx);
        tokio::spawn(deleter.listen());
    }

    #[instrument(skip(self, post_processing_tx, error_tx))]
    async fn batch_poll(
        &mut self,
        error_tx: UnboundedSender<BatchPollerError>,
        post_processing_tx: UnboundedSender<MessagePostProcessing>,
    ) {
        if let Err(e) = self.process_from_buffer(post_processing_tx.clone()) {
            if let Err(SendError(err)) = error_tx.send(e) {
                tracing::error!("Error sending error to error_tx: {:?}", err);
            }
        }

        match self.batch_sizes() {
            Ok(batch_sizes) => {
                if batch_sizes.is_empty() {
                    return;
                }

                let (tx, mut rx) = mpsc::unbounded_channel();

                for batch_size in batch_sizes {
                    let receiver = SqsReceive {
                        client: self.consumer.client.clone(),
                        queue_url: self.consumer.queue_url.clone(),
                        visibility_timeout: self.consumer.visibility_timeout,
                        receive_batch_size: batch_size,
                        receive_message_wait_time: self.consumer.receive_message_wait_time,
                        tx: tx.clone(),
                    };

                    receiver.run();
                }

                drop(tx); // Drop the original tx so that the receive loop will stop when all receivers have responded
                while let Some(r) = rx.recv().await {
                    match r {
                        Ok(Some(messages)) => {
                            match self.process_messages(messages, post_processing_tx.clone()) {
                                Err(e) => {
                                    if let Err(SendError(err)) = error_tx.send(e) {
                                        tracing::error!(
                                            "Error sending error to error_tx: {:?}",
                                            err
                                        );
                                    }
                                }
                                _ => continue,
                            }
                        }
                        Ok(None) => {
                            tracing::warn!("Received None from SqsReceive");
                        }
                        Err(e) => {
                            tracing::error!("Error receiving messages from SqsReceive: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                if let Err(SendError(err)) = error_tx.send(e) {
                    tracing::error!("Error sending error to error_tx: {:?}", err);
                }
            }
        }
    }

    #[instrument(skip(self, post_processing_tx))]
    fn process_messages(
        &mut self,
        messages: Vec<SqsMessage>,
        post_processing_tx: UnboundedSender<MessagePostProcessing>,
    ) -> Result<(), BatchPollerError> {
        match self.messages_to_process(messages) {
            Ok(res) => match res {
                Some(currently_possible) => {
                    for message in currently_possible {
                        let processor = ProcessWithHeartbeat {
                            message,
                            client: self.consumer.client.clone(),
                            queue_url: self.consumer.queue_url.clone(),
                            visibility_timeout: self.consumer.visibility_timeout,
                            heartbeat_interval: self.consumer.heartbeat_interval,
                            message_processor: self.consumer.message_processor.clone(),
                            post_processing_tx: post_processing_tx.clone(),
                        };

                        processor.process();
                    }
                    Ok(())
                }
                None => {
                    tracing::debug!(
                        "received messages not being processed immediately. Added to the buffer"
                    );
                    Ok(())
                }
            },

            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self, post_processing_tx))]
    fn process_from_buffer(
        &mut self,
        post_processing_tx: UnboundedSender<MessagePostProcessing>,
    ) -> Result<(), BatchPollerError> {
        if self.message_buffer.is_empty() {
            return Ok(());
        }

        match self.buffered_messages_to_process() {
            Ok(res) => match res {
                Some(messages) => {
                    for m in messages.iter() {
                        let processor = ProcessWithHeartbeat {
                            client: self.consumer.client.clone(),
                            queue_url: self.consumer.queue_url.clone(),
                            visibility_timeout: self.consumer.visibility_timeout,
                            heartbeat_interval: self.consumer.heartbeat_interval,
                            message_processor: self.consumer.message_processor.clone(),
                            message: m.clone(),
                            post_processing_tx: post_processing_tx.clone(),
                        };

                        processor.process();
                    }
                    Ok(())
                }
                None => {
                    tracing::debug!(
                        buffer.length = self.message_buffer.len(),
                        "at concurrency limit, cannot remove messages from buffer",
                    );
                    Ok(())
                }
            },
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self))]
    fn messages_to_process(
        &mut self,
        messages: Vec<SqsMessage>,
    ) -> Result<Option<Vec<SqsMessage>>, BatchPollerError> {
        let processing_concurrency_limit = self.consumer.processing_concurrency_limit.as_usize();
        let message_buffer = &mut self.message_buffer;

        self.in_flight_counter
            .risky_run_with_lock(|guard| {
                Self::currently_processable_messages_with_rest_buffered(
                    messages,
                    message_buffer,
                    processing_concurrency_limit,
                    guard,
                )
            })
            .context(CounterSnafu)
    }

    #[instrument(skip(counter_guard))]
    fn currently_processable_messages_with_rest_buffered(
        mut messages: Vec<SqsMessage>,
        message_buffer: &mut Vec<SqsMessage>,
        processing_concurrency_limit: usize,
        mut counter_guard: std::sync::MutexGuard<'_, usize>,
    ) -> Option<Vec<SqsMessage>> {
        if *counter_guard >= processing_concurrency_limit {
            message_buffer.append(&mut messages);
            return None;
        };

        let limit = processing_concurrency_limit - *counter_guard;
        let to_process = if limit >= messages.len() {
            messages
        } else {
            let currently_possible = messages.split_off(limit);

            message_buffer.append(&mut messages);
            currently_possible
        };

        *counter_guard += to_process.len();
        Some(to_process)
    }

    #[instrument(skip(self))]
    fn buffered_messages_to_process(
        &mut self,
    ) -> Result<Option<Vec<SqsMessage>>, BatchPollerError> {
        let processing_concurrency_limit = self.consumer.processing_concurrency_limit.as_usize();
        let message_buffer = &mut self.message_buffer;

        self.in_flight_counter
            .risky_run_with_lock(|guard| {
                Self::drain_buffer_till_limit(message_buffer, processing_concurrency_limit, guard)
            })
            .context(CounterSnafu)
    }

    #[instrument(skip(message_buffer, processing_concurrency_limit))]
    fn drain_buffer_till_limit(
        message_buffer: &mut Vec<SqsMessage>,
        processing_concurrency_limit: usize,
        mut counter_guard: std::sync::MutexGuard<'_, usize>,
    ) -> Option<Vec<SqsMessage>> {
        if *counter_guard >= processing_concurrency_limit {
            return None;
        }

        let limit = processing_concurrency_limit - *counter_guard;

        let to_process: Vec<SqsMessage> = if limit >= message_buffer.len() {
            message_buffer.drain(..).collect()
        } else {
            message_buffer.drain(..limit).collect()
        };

        *counter_guard += to_process.len();
        Some(to_process)
    }

    #[instrument(skip(self))]
    fn batch_sizes(&self) -> Result<Vec<ReceiveBatchSize>, BatchPollerError> {
        let mut batch_sizes = vec![];

        let messages_to_receive = self.in_flight_counter.available_space();
        if messages_to_receive == 0 {
            return Ok(batch_sizes);
        }

        let num_full_batches = messages_to_receive.div(self.consumer.receive_batch_size.as_usize());
        let leftover_batch_size =
            messages_to_receive.rem_euclid(self.consumer.receive_batch_size.as_usize());

        for _ in 0..num_full_batches {
            batch_sizes.push(self.consumer.receive_batch_size);
        }
        if leftover_batch_size > 0 {
            let leftover_batch = ReceiveBatchSize::parse(leftover_batch_size)
                .context(InvalidLeftoverBatchSizeSnafu)?;
            batch_sizes.push(leftover_batch);
        }

        Ok(batch_sizes)
    }
}
