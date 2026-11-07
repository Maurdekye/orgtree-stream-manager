//! secrets.toml bridge tokens may carry the `moderate` scope (moderation-bot pool).

use sm_bin::secrets::load_secrets;
use sm_server::Scopes;

const MODBOT: &str = "modbot-token-0123456789abcdef0123456789ab";
const MODREAD: &str = "modread-token-0123456789abcdef012345678";

#[test]
fn a_moderate_token_maps_to_read_plus_moderate_only() {
    let text = format!(
        "[[bridge_token]]\nname = \"modbot\"\ntoken = \"{MODBOT}\"\nmoderate = true\n\n[[bridge_token]]\nname = \"modread\"\ntoken = \"{MODREAD}\"\n"
    );
    let s = load_secrets(&text).unwrap();
    let t = s.token_table();
    assert_eq!(t.check(MODBOT).unwrap().1, Scopes { read: true, control: false, moderate: true });
    assert_eq!(t.check(MODREAD).unwrap().1, Scopes { read: true, control: false, moderate: false });
}

#[test]
fn control_and_moderate_together_are_refused() {
    let text = format!("[[bridge_token]]\nname = \"x\"\ntoken = \"{MODBOT}\"\ncontrol = true\nmoderate = true\n");
    assert!(load_secrets(&text).is_err());
}
