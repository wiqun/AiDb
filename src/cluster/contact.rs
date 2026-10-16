use std::collections::HashMap;
use std::time::Instant;

static CONTACTS: std::sync::LazyLock<parking_lot::Mutex<HashMap<u64, Instant>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

/// follower 收到一次 AppendEntries, 含空心跳.
pub fn note_leader_append(group_id: u64) {
    CONTACTS.lock().insert(group_id, Instant::now());
}

/// 距上次 AppendEntries 的秒数. 还没收到过时是 `None`.
pub fn leader_contact_age_secs(group_id: u64) -> Option<f64> {
    CONTACTS
        .lock()
        .get(&group_id)
        .map(|seen| seen.elapsed().as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::{leader_contact_age_secs, note_leader_append};

    #[test]
    fn contact_age_is_none_until_an_append() {
        assert_eq!(leader_contact_age_secs(7), None);
        note_leader_append(7);
        let age = leader_contact_age_secs(7).expect("heard");
        assert!(age >= 0.0);
        assert!(age < 1.0);
    }
}
