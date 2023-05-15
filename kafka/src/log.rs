use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use serde::{Deserialize, Serialize};

pub type Offset = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogEntry(Offset, i64);

pub struct Log {
    state: Arc<Mutex<State>>,
    offset_counter: AtomicU64,
}

impl Clone for Log {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            offset_counter: AtomicU64::new(self.offset_counter.load(Ordering::SeqCst)),
        }
    }
}

struct State {
    _key: String,
    committed_offset: Offset,
    log: Vec<LogEntry>,
}

impl Log {
    pub fn new(key: String) -> Self {
        Log {
            offset_counter: AtomicU64::new(0),
            state: Arc::new(Mutex::new(State {
                _key: key,
                committed_offset: 0,
                log: vec![],
            })),
        }
    }

    pub fn append(&mut self, value: i64) -> Offset {
        let offset = self.next_offset();
        self.state.lock().unwrap().log.push(LogEntry(offset, value));

        offset
    }

    pub fn poll(&self, from: Offset) -> Vec<LogEntry> {
        let state = self.state.lock().unwrap();
        state.log[(from as usize)..].to_vec()
    }

    pub fn committed_offset(&self) -> Offset {
        self.state.lock().unwrap().committed_offset
    }

    pub fn commit(&mut self, offset: Offset) {
        self.state.lock().unwrap().committed_offset = offset;
    }

    fn next_offset(&self) -> u64 {
        self.offset_counter.fetch_add(1, Ordering::SeqCst)
    }
}
