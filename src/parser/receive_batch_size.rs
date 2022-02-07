// third party
use snafu::prelude::*;

const MIN: usize = 1;
const MAX: usize = 10;

#[derive(Debug, Clone, Copy)]
/// Store both i32 and usize formats to allow quick usage for different APIs
/// The aws-sdk use generated code and so incorrectly accepts an i32.
/// That should be corrected when v1 is released.
pub struct ReceiveBatchSize(i32, usize);

#[derive(Debug, Snafu)]
pub enum ReceiveBatchSizeError {
    #[snafu(display(
        "a valid sqs receive batch size must be between {MIN} and {MAX} => got={input}"
    ))]
    NotWithinSQSAcceptableRange { input: usize },

    #[snafu(display(
        "not compatible with the aws sdk API. Must be a valid i32 value. got={input} => {source}"
    ))]
    MustFitInI32 {
        input: usize,
        source: std::num::TryFromIntError,
    },
}

impl ReceiveBatchSize {
    pub fn parse(size: usize) -> Result<ReceiveBatchSize, ReceiveBatchSizeError> {
        use ReceiveBatchSizeError::*;

        if !(MIN..=MAX).contains(&size) {
            return Err(NotWithinSQSAcceptableRange { input: size });
        };

        let i32_version: i32 = size.try_into().context(MustFitInI32Snafu { input: size })?;

        Ok(ReceiveBatchSize(i32_version, size))
    }

    pub fn as_i32(&self) -> i32 {
        self.0
    }

    pub fn as_usize(&self) -> usize {
        self.1
    }
}
