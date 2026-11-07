//! Review round 1, core findings 6 and 13d.

use sm_core::outbound::OutboundFilter;
use sm_core::pipeline::RateWindow;

#[test]
fn filter_checks_a_compacted_copy_for_secrets_shapes_email_ip_and_deny() {
    let f = OutboundFilter::new(vec!["s3cr3t-T0ken-value-123456".into()], vec!["Springfield".into()]);
    for t in [
        "s3cr3t\u{200B}-T0ken-value",
        "s 3 c r 3 t - T 0 k e n",
        "sk- abcdefghijklmnop1234",
        "someone @ example . com",
        "192 . 168 . 1 . 20",
        "Spring field",
        "spring\u{00AD}field",
    ] {
        assert!(f.check(t).is_err(), "{t}");
    }
    // A long ordinary sentence is not a "long token" just because spaces were removed.
    assert_eq!(f.check("thanks so much everyone for hanging out tonight, see you tomorrow"), Ok(()));
}

#[test]
fn rate_window_check_does_not_record() {
    let mut r = RateWindow::new(10.0, 4, 60.0);
    assert!(r.check(0.0).is_ok());
    assert!(r.check(0.0).is_ok(), "check alone never fills the window");
    r.try_acquire(0.0).unwrap();
    assert_eq!(r.check(5.0), Err(5.0));
}
