//! YouTube Live spec §5 quota accounting and §6 the ban book (red tests 6 and 7, connector
//! half). Pure: explicit times, no network.

use sm_connectors::youtube;
use sm_connectors::ytstate::{pacific_day, QuotaLevel, YtBans, YtCosts, YtQuota};

// 2026-11-07 08:00:00Z is midnight Pacific (PST, UTC-8); 2026-07-01 07:00:00Z is midnight PDT (UTC-7);
// 2026-03-09 07:30:00Z is 00:30 PDT, the day after DST began.
const PST_LAST_SECOND: f64 = 1_794_038_399.0;
const PST_MIDNIGHT: f64 = 1_794_038_400.0;
const PDT_LAST_SECOND: f64 = 1_782_889_199.0;
const PDT_MIDNIGHT: f64 = 1_782_889_200.0;
const AFTER_DST_START: f64 = 1_773_041_400.0;

#[test]
fn the_quota_day_is_the_pacific_day() {
    assert_eq!(pacific_day(PST_LAST_SECOND), "2026-11-06");
    assert_eq!(pacific_day(PST_MIDNIGHT), "2026-11-07");
    assert_eq!(pacific_day(PDT_LAST_SECOND), "2026-06-30");
    assert_eq!(pacific_day(PDT_MIDNIGHT), "2026-07-01");
    assert_eq!(pacific_day(AFTER_DST_START), "2026-03-09", "daylight time has begun: 07:30Z is already the 9th in Pacific");
}

#[test]
fn quota_counts_per_pacific_day_and_resets_at_the_boundary() {
    let q = YtQuota::new(10_000);
    assert_eq!(q.used(PST_LAST_SECOND), 0);
    q.record(PST_LAST_SECOND, 5_000);
    assert_eq!(q.used(PST_LAST_SECOND), 5_000);
    assert_eq!(q.level(PST_LAST_SECOND), QuotaLevel::Ok);
    q.record(PST_LAST_SECOND, 3_000);
    assert_eq!(q.level(PST_LAST_SECOND), QuotaLevel::Warn80, "8,000 of 10,000");
    q.record(PST_LAST_SECOND, 1_500);
    assert_eq!(q.level(PST_LAST_SECOND), QuotaLevel::Low95, "9,500 of 10,000");
    assert_eq!(q.snapshot(PST_LAST_SECOND), ("2026-11-06".to_string(), 9_500));
    // One second later it is a new Pacific day: the count starts over.
    assert_eq!(q.used(PST_MIDNIGHT), 0);
    assert_eq!(q.level(PST_MIDNIGHT), QuotaLevel::Ok);
    q.record(PST_MIDNIGHT, 5);
    assert_eq!(q.snapshot(PST_MIDNIGHT), ("2026-11-07".to_string(), 5));
    // A restart restores today's count, and ignores yesterday's.
    let r = YtQuota::new(10_000);
    r.restore("2026-11-07", 1_234);
    assert_eq!(r.used(PST_MIDNIGHT), 1_234);
    let r = YtQuota::new(10_000);
    r.restore("2026-11-06", 9_999);
    assert_eq!(r.used(PST_MIDNIGHT), 0);
    assert_eq!(q.limit(), 10_000);
}

#[test]
fn request_costs_default_to_the_documented_units() {
    let c = YtCosts::default();
    assert_eq!(c.for_request(&youtube::active_broadcasts()), 1);
    assert_eq!(c.for_request(&youtube::chat_list("L1", None)), 5);
    assert_eq!(c.for_request(&youtube::chat_stream("L1", None)), 5);
    assert_eq!(c.for_request(&youtube::chat_insert("L1", "hi").unwrap()), 50);
    assert_eq!(c.for_request(&youtube::ban("L1", "UC1", None).unwrap()), 200);
    assert_eq!(c.for_request(&youtube::chat_delete("m1").unwrap()), 50);
    assert_eq!(c.for_request(&youtube::unban("b1").unwrap()), 50);
    assert_eq!(c.for_request(&youtube::transition_complete("B1")), 50);
}

// Red test 7, book half.
#[test]
fn the_ban_book_keeps_ids_until_unban_or_expiry() {
    let b = YtBans::default();
    b.put("UC1", "BAN1", None);
    assert!(b.has("UC1", 100.0));
    assert_eq!(b.take("UC1", 100.0).as_deref(), Some("BAN1"));
    assert_eq!(b.take("UC1", 100.0), None, "taken once");
    assert!(!b.has("UC1", 100.0));

    b.put("UC2", "BAN2", Some(200.0));
    assert!(b.has("UC2", 199.0));
    assert!(!b.has("UC2", 200.0), "an expired timeout is no longer a ban to lift");
    assert_eq!(b.take("UC2", 250.0), None);

    b.put("UC3", "BAN3", Some(10.0));
    b.put("UC4", "BAN4", None);
    assert_eq!(b.prune(11.0), 1, "the expired timeout's record is dropped");
    assert_eq!(b.export(), vec![("UC4".to_string(), "BAN4".to_string(), None)]);
    let c = YtBans::default();
    c.import(b.export());
    assert!(c.has("UC4", 11.0));
}

// Amendment 2: a failed unban puts the record back exactly as it was.
#[test]
fn a_removed_record_can_be_restored_with_its_expiry() {
    let b = YtBans::default();
    b.put("UC1", "BAN1", Some(500.0));
    let rec = b.take_record("UC1", 100.0).unwrap();
    assert_eq!(rec, ("BAN1".to_string(), Some(500.0)));
    assert!(!b.has("UC1", 100.0));
    assert!(b.restore("UC1", rec.clone(), 200.0));
    assert_eq!(b.export(), vec![("UC1".to_string(), "BAN1".to_string(), Some(500.0))]);
    // If the expiry has passed meanwhile, the record is dropped instead.
    let rec = b.take_record("UC1", 300.0).unwrap();
    assert!(!b.restore("UC1", rec, 600.0));
    assert!(b.export().is_empty());
}
