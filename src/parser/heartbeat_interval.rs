// third party
use snafu::prelude::*;

// internal
use crate::parser::visibility_timeout::VisibilityTimeout;

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatInterval(usize, u64);

#[derive(Debug, Snafu)]
pub enum HeartbeatIntervalError {
    #[snafu(display(
        "heartbeat interval ({input}) must be lower than visibility timeout ({visibility_timeout})"
    ))]
    MustBeLowerThanVisibilityTimeout {
        input: usize,
        visibility_timeout: usize,
    },

    #[snafu(display("visibility timeout ({visibility_timeout}) is too low to have a heartbeat"))]
    VisibilityIntervalTooLowForHeartbeat { visibility_timeout: usize },

    #[snafu(display("heartbeat interval ({input}) must fit in a u64 value => {source}"))]
    MustFitInU64 {
        input: usize,
        source: std::num::TryFromIntError,
    },
}

impl HeartbeatInterval {
    pub fn parse(
        seconds: usize,
        visibility_timeout: &VisibilityTimeout,
    ) -> Result<HeartbeatInterval, HeartbeatIntervalError> {
        use HeartbeatIntervalError::*;

        if visibility_timeout.as_i32_seconds() <= 1 {
            return Err(VisibilityIntervalTooLowForHeartbeat {
                visibility_timeout: visibility_timeout.as_usize_seconds(),
            });
        }

        if seconds >= visibility_timeout.as_usize_seconds() {
            return Err(MustBeLowerThanVisibilityTimeout {
                input: seconds,
                visibility_timeout: visibility_timeout.as_usize_seconds(),
            });
        };

        let u64_version: u64 = seconds
            .try_into()
            .context(MustFitInU64Snafu { input: seconds })?;

        Ok(HeartbeatInterval(seconds, u64_version))
    }

    pub fn as_usize_seconds(&self) -> usize {
        self.0
    }

    pub fn as_u64_seconds(&self) -> u64 {
        self.1
    }
}
