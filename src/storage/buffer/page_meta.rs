use chrono::Local;

#[derive(Debug, Clone, PartialEq, Copy)]
pub struct PageMeta {
    pub last_access: i64,
    pub count_access: usize,
}

impl PageMeta {
    pub fn build() -> PageMeta {
        let now = Local::now();
        PageMeta {
            last_access: now.timestamp_millis(),
            count_access: 1,
        }
    }

    pub fn increment_access(&mut self) {
        self.last_access = Local::now().timestamp_millis();
        self.count_access += 1;
    }
}

#[cfg(test)]
pub mod tests {
    use crate::storage::buffer::page_meta::PageMeta;

    #[test]
    fn increment_access_should_update_counters() {
        let mut page_meta = PageMeta::build();
        page_meta.increment_access();
        assert_eq!(page_meta.count_access, 2)
    }
}
