// third party
use snafu::prelude::*;

// internal
use crate::parser::receive_batch_size::ReceiveBatchSize;

#[derive(Debug, Clone)]
pub struct ProcessingConcurrencyLimit(usize);

#[derive(Debug, Snafu)]
pub enum ProcessingConcurrencyLimitError {
    #[snafu(display("a valid processing concurrency limit must greater than or equal to the batch size ({batch_size}) => got={input}"))]
    MustBeGreaterThanOrEqualToBatchSize { input: usize, batch_size: usize },
}

impl ProcessingConcurrencyLimit {
    pub fn parse(
        limit: usize,
        batch_size: &ReceiveBatchSize,
    ) -> Result<ProcessingConcurrencyLimit, ProcessingConcurrencyLimitError> {
        use ProcessingConcurrencyLimitError::*;

        if limit < batch_size.as_usize() {
            return Err(MustBeGreaterThanOrEqualToBatchSize {
                input: limit,
                batch_size: batch_size.as_usize(),
            });
        };

        Ok(ProcessingConcurrencyLimit(limit))
    }

    pub fn as_usize(&self) -> usize {
        self.0
    }
}
