// std lib
use std::sync::Arc;
use std::sync::Mutex;

// third party
use snafu::prelude::*;
use tracing::instrument;

#[derive(Debug)]
pub(crate) struct MessagesBeingProcessedCounter {
    max: usize,
    counter: Arc<Mutex<usize>>,
}

#[derive(Debug, Snafu)]
pub enum CounterError {
    InvalidBounds {
        max: usize,
        counter: usize,
    },

    OverBoundIncrement {
        tried_to_add: usize,
        current_count: usize,
        max: usize,
    },

    UnderBoundDecrement {
        tried_to_remove: usize,
        current_count: usize,
    },
}

#[allow(clippy::mutex_atomic)] // Consider an atomic IF required
impl MessagesBeingProcessedCounter {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            max,
            counter: Arc::new(Mutex::new(0)),
        }
    }

    #[instrument]
    pub(crate) fn decrement(&self) -> Result<(), CounterError> {
        let mut counter = self.counter.lock().unwrap();
        match (*counter).checked_sub(1) {
            None => Err(CounterError::UnderBoundDecrement {
                tried_to_remove: 1,
                current_count: *counter,
            }),
            Some(c) => {
                *counter = c;
                Ok(())
            }
        }
    }

    #[instrument]
    pub(crate) fn increment(&self) -> Result<(), CounterError> {
        let mut counter = self.counter.lock().unwrap();
        match (*counter).checked_add(1) {
            None => Err(CounterError::OverBoundIncrement {
                tried_to_add: 1,
                current_count: *counter,
                max: self.max,
            }),
            Some(c) => {
                *counter = c;
                Ok(())
            }
        }
    }

    #[instrument]
    pub(crate) fn decrement_by(&self, amount: usize) -> Result<(), CounterError> {
        let mut counter = self.counter.lock().unwrap();
        match (*counter).checked_sub(amount) {
            None => Err(CounterError::UnderBoundDecrement {
                tried_to_remove: amount,
                current_count: *counter,
            }),
            Some(c) => {
                *counter = c;
                Ok(())
            }
        }
    }

    #[instrument]
    pub(crate) fn increment_by(&self, amount: usize) -> Result<(), CounterError> {
        let mut counter = self.counter.lock().unwrap();
        match (*counter).checked_add(amount) {
            None => Err(CounterError::OverBoundIncrement {
                tried_to_add: amount,
                current_count: *counter,
                max: self.max,
            }),
            Some(c) => {
                *counter = c;
                Ok(())
            }
        }
    }

    #[instrument]
    pub(crate) fn available_space(&self) -> usize {
        let counter = self.counter.lock().unwrap();
        self.max - *counter
    }

    /// This is marked risky because there's no easy way to guarantee no usize underflow errors without more indirection.
    #[instrument(skip(f))]
    pub(crate) fn risky_run_with_lock<F, R>(&self, f: F) -> Result<R, CounterError>
    where
        F: FnOnce(std::sync::MutexGuard<'_, usize>) -> R,
    {
        let counter = self.counter.lock().unwrap();
        let res = f(counter);

        self.check_bounds().map(|_| res)
    }

    fn check_bounds(&self) -> Result<(), CounterError> {
        let counter = self.counter.lock().unwrap();
        if *counter > self.max {
            Err(CounterError::InvalidBounds {
                counter: *counter,
                max: self.max,
            })
        } else {
            Ok(())
        }
    }
}

impl Clone for MessagesBeingProcessedCounter {
    fn clone(&self) -> Self {
        Self {
            max: self.max,
            counter: Arc::clone(&self.counter),
        }
    }
}
