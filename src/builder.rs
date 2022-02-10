// std lib
use std::future::Future;

// third party
use aws_sdk_sqs::model::Message as SqsMessage;
use snafu::prelude::*;

// internal
use crate::consumer::{MessagePostProcessing, SqsConsumer};
use crate::parser::*;
use crate::sqs::SqsClient;

#[derive(Debug)]
pub struct SqsConsumerBuilder<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Send + 'static,
{
    client: Option<SqsClient>,
    queue_url: Option<String>,
    visibility_timeout: Option<VisibilityTimeout>,
    heartbeat_interval_seconds: Option<usize>,
    receive_batch_size: Option<ReceiveBatchSize>,
    receive_message_wait_time: Option<ReceiveWaitTime>,
    message_processor: Option<M>,
    message_processing_concurrency_limit: Option<usize>,
}

#[derive(Debug, Snafu)]
pub enum BuilderError {
    InvalidVisibilityTimeout {
        input: usize,
        source: VisibilityTimeoutError,
    },

    InvalidBatchSize {
        input: usize,
        source: ReceiveBatchSizeError,
    },

    InvalidHeartbeatInterval {
        input: usize,
        source: HeartbeatIntervalError,
    },

    InvalidWaitTime {
        input: usize,
        source: ReceiveWaitTimeError,
    },

    InvalidConcurrencyLimit {
        input: usize,
        source: ProcessingConcurrencyLimitError,
    },

    #[snafu(display("the SQS Consumer builder configuration is invalid => {message}"))]
    RequiredArgumentNotProvided { message: String },
}

impl<M, F> SqsConsumerBuilder<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Send + 'static,
{
    pub fn new() -> Self {
        SqsConsumerBuilder {
            client: None,
            queue_url: None,
            visibility_timeout: None,
            heartbeat_interval_seconds: None,
            receive_batch_size: None,
            receive_message_wait_time: None,
            message_processor: None,
            message_processing_concurrency_limit: None,
        }
    }

    pub fn client(mut self, client: SqsClient) -> Self {
        self.client = Some(client);
        self
    }

    pub fn queue_url(mut self, queue_url: impl Into<std::string::String>) -> Self {
        self.queue_url = Some(queue_url.into());
        self
    }

    pub fn visibility_timeout_seconds(mut self, input: usize) -> Result<Self, BuilderError> {
        let visibility_timeout =
            VisibilityTimeout::parse(input).context(InvalidVisibilityTimeoutSnafu { input })?;
        self.visibility_timeout = Some(visibility_timeout);
        Ok(self)
    }

    pub fn heartbeat_interval_seconds(mut self, input: usize) -> Self {
        self.heartbeat_interval_seconds = Some(input);
        self
    }

    pub fn receive_batch_size(mut self, input: usize) -> Result<Self, BuilderError> {
        let receive_batch_size =
            ReceiveBatchSize::parse(input).context(InvalidBatchSizeSnafu { input })?;
        self.receive_batch_size = Some(receive_batch_size);
        Ok(self)
    }

    pub fn receive_message_wait_time(mut self, input: usize) -> Result<Self, BuilderError> {
        let receive_message_wait_time =
            ReceiveWaitTime::parse(input).context(InvalidWaitTimeSnafu { input })?;
        self.receive_message_wait_time = Some(receive_message_wait_time);
        Ok(self)
    }

    pub fn message_processor(mut self, input: M) -> Self {
        self.message_processor = Some(input);
        self
    }

    pub fn message_processing_concurrency_limit(mut self, input: usize) -> Self {
        self.message_processing_concurrency_limit = Some(input);
        self
    }

    pub fn build(self) -> Result<SqsConsumer<M, F>, BuilderError> {
        use BuilderError::*;

        let client = self.client.ok_or(RequiredArgumentNotProvided {
            message: "sqs client must be provided".to_string(),
        })?;

        let queue_url = self.queue_url.ok_or(RequiredArgumentNotProvided {
            message: "queue_url must be provided".to_string(),
        })?;

        let visibility_timeout = self.visibility_timeout.ok_or(RequiredArgumentNotProvided {
            message: "visibility timeout must be provided".to_string(),
        })?;

        let heartbeat_interval_seconds =
            self.heartbeat_interval_seconds
                .ok_or(RequiredArgumentNotProvided {
                    message: "heartbeat interval seconds must be provided".to_string(),
                })?;

        let heartbeat_interval =
            HeartbeatInterval::parse(heartbeat_interval_seconds, &visibility_timeout).context(
                InvalidHeartbeatIntervalSnafu {
                    input: heartbeat_interval_seconds,
                },
            )?;

        let receive_batch_size = self.receive_batch_size.ok_or(RequiredArgumentNotProvided {
            message: "receive batch size must be provided".to_string(),
        })?;

        let receive_message_wait_time =
            self.receive_message_wait_time
                .ok_or(RequiredArgumentNotProvided {
                    message: "receive message wait time must be provided".to_string(),
                })?;

        let message_processor = self.message_processor.ok_or(RequiredArgumentNotProvided {
            message: "message processor must be provided".to_string(),
        })?;

        let message_processing_concurrency_limit = self
            .message_processing_concurrency_limit
            .ok_or(RequiredArgumentNotProvided {
                message: "message processing concurrency limit must be provided".to_string(),
            })?;

        let processing_concurrency_limit = ProcessingConcurrencyLimit::parse(
            message_processing_concurrency_limit,
            &receive_batch_size,
        )
        .context(InvalidConcurrencyLimitSnafu {
            input: message_processing_concurrency_limit,
        })?;

        Ok(SqsConsumer {
            client,
            queue_url,
            visibility_timeout,
            heartbeat_interval,
            receive_batch_size,
            receive_message_wait_time,
            processing_concurrency_limit,
            message_processor,
        })
    }
}

impl<M, F> Default for SqsConsumerBuilder<M, F>
where
    M: FnOnce(SqsMessage) -> F + Clone + Sync + Send + 'static,
    F: Future<Output = MessagePostProcessing> + Sync + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
