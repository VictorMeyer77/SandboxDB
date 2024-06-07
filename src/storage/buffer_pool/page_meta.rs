use chrono::Local;

#[derive(Debug, Clone, PartialEq)]
pub struct PageMeta {
    pub last_access: usize,
    pub count_access: usize,
}

impl PageMeta {
    pub fn new() -> PageMeta {
        let now = Local::now();
        PageMeta {
            last_access: now.timestamp_millis() as usize,
            count_access: 1,
        }
    }

    pub fn increment_access(&mut self) {
        let now = Local::now();
        self.last_access = now.timestamp_millis() as usize;
        self.count_access += 1;
    }
}
