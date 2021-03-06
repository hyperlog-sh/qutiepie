// third party
use snafu::prelude::*;

const MIN: usize = 0;
const MAX: usize = 20;

#[derive(Debug, Clone, Copy)]
/// Store both i32 and usize formats to allow quick usage for different APIs
/// The aws-sdk use generated code and so incorrectly accepts an i32.
/// That should be corrected when v1 is released.
pub struct ReceiveWaitTime(i32, usize, u64);

#[derive(Debug, Snafu)]
pub enum ReceiveWaitTimeError {
    #[snafu(display(
        "a valid sqs receive wait time value in seconds must be between {MIN} and {MAX} => got={input}"
    ))]
    NotWithinSQSAcceptableRange { input: usize },

    #[snafu(display(
        "not compatible with the aws sdk API. Must be a valid i32 value. got={input} => {source}"
    ))]
    MustFitInI32 {
        input: usize,
        source: std::num::TryFromIntError,
    },

    #[snafu(display(
        "to ease usage with the tokio time APIs, input must fit in a u64 => got={input}"
    ))]
    MustFitInU64 {
        input: usize,
        source: std::num::TryFromIntError,
    },
}

impl ReceiveWaitTime {
    pub fn parse(seconds: usize) -> Result<ReceiveWaitTime, ReceiveWaitTimeError> {
        use ReceiveWaitTimeError::*;

        if !(MIN..=MAX).contains(&seconds) {
            return Err(NotWithinSQSAcceptableRange { input: seconds });
        };

        let i32_version: i32 = seconds
            .try_into()
            .context(MustFitInI32Snafu { input: seconds })?;

        let u64_version: u64 = seconds
            .try_into()
            .context(MustFitInU64Snafu { input: seconds })?;
        Ok(ReceiveWaitTime(i32_version, seconds, u64_version))
    }

    pub fn as_i32_seconds(&self) -> i32 {
        self.0
    }

    pub fn as_usize_seconds(&self) -> usize {
        self.1
    }

    pub fn as_u64_seconds(&self) -> u64 {
        self.2
    }
}
