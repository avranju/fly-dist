use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use once_cell::sync::OnceCell;

use crate::log::{Log, LogEntry, Offset};

#[derive(Default)]
pub struct State {
    logs: Arc<Mutex<HashMap<String, Log>>>,
}

impl State {
    pub fn append(&self, key: String, value: i64) -> Offset {
        let mut logs = self.logs.lock().unwrap();
        let mut offset = 0;
        logs.entry(key.clone())
            .and_modify(|log| {
                offset = log.append(value);
            })
            .or_insert_with(|| {
                let mut log = Log::new(key);
                offset = log.append(value);
                log
            });

        offset
    }

    pub fn poll(&self, key: &String, offset: Offset) -> Option<Vec<LogEntry>> {
        self.logs
            .lock()
            .unwrap()
            .get(key)
            .map(|log| log.poll(offset))
    }

    pub fn commit(&self, key: &String, offset: Offset) {
        if let Some(log) = self.logs.lock().unwrap().get_mut(key) {
            log.commit(offset);
        }
    }

    pub fn committed_offset(&self, key: &String) -> Option<Offset> {
        self.logs
            .lock()
            .unwrap()
            .get(key)
            .map(|log| log.committed_offset())
    }
}

pub fn state() -> &'static Mutex<State> {
    static INSTANCE: OnceCell<Mutex<State>> = OnceCell::new();
    INSTANCE.get_or_init(|| Mutex::new(Default::default()))
}
