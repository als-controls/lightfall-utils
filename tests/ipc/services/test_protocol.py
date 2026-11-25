import inspect

import pytest

from lightfall_utils.ipc.services import protocol as p


def test_params_do_not_shadow_functions():
    funcs = {k: v for k, v in vars(p).items() if callable(v) and not k.startswith("_")}
    for name, fn in funcs.items():
        assert not (set(inspect.signature(fn).parameters) & funcs.keys()), name


def test_constants():
    assert p.SVC_PROTOCOL == 1
    assert (p.HEARTBEAT_S, p.PING_S, p.LEASE_S, p.STREAM_IDLE_S, p.DEFAULT_TIMEOUT_S) == (
        2.0,
        5.0,
        30.0,
        10.0,
        2.0,
    )
    assert (p.HDR_SEQ, p.HDR_KIND, p.HDR_FINAL) == ("Svc-Seq", "Svc-Kind", "Svc-Final")


def test_sanitize_token():
    assert p.sanitize_token("BCG-WS1.als.lbl.gov") == "bcg-ws1-als-lbl-gov"
    assert p.sanitize_token("a b*>") == "a-b--"


def test_scope_and_subjects():
    scope = p.local_scope("als.7011", "Tsuru.als", "ab12")
    assert scope == "als.7011.host.tsuru-als.ab12"
    r = p.root(scope, "stxm")
    assert r == f"{scope}.svc.stxm"
    assert p.meta_info(r) == f"{r}.meta.info"
    assert p.heartbeat(r) == f"{r}.heartbeat"
    assert p.event(r, "done") == f"{r}.events.done"
    assert p.discover(scope) == f"{scope}.svc.discover"


PRIV = "_PRIV." + "a" * 26


def test_private_subjects():
    assert p.auth_subject(PRIV) == f"{PRIV}.auth.request"
    s = p.session_subject(PRIV, "tok")
    assert s == f"{PRIV}.session.tok"
    assert p.rpc_subject(s, "ping") == f"{s}.rpc.ping"
    assert p.stream_subject(s, "7") == f"{s}.stream.7"
    assert p.delivery_root(PRIV, "dt") == f"{PRIV}.d.dt"


def test_prefix_helpers():
    assert p.prefix_of_priv_root(PRIV) == "a" * 26
    with pytest.raises(ValueError):
        p.prefix_of_priv_root("_PRIV.nope")
    assert p.prefix_of_inbox("_INBOX.abc.x.y") == "abc"
    assert p.prefix_of_inbox("foo") is None


def test_contract_ok():
    assert p.contract_ok("1.3", "1.2")
    assert not p.contract_ok("1.1", "1.2")
    assert not p.contract_ok("2.0", "1.0")
    assert p.contract_ok("1.0", "1.0")
    assert not p.contract_ok("junk", "1.0")


@pytest.mark.parametrize(
    "bad",
    [
        "junk",
        "",
        "1",
        "1.2.3",
        "-1.0",
        "1.-2",
        "+1.0",
        "1.+0",
        " 1.0",
        "1.0 ",
        "1. 0",
        "1_0.0",
        "١.0",
    ],
)
def test_parse_contract_rejects_odd_forms(bad):
    with pytest.raises(ValueError):
        p._parse_contract(bad)
    assert not p.contract_ok(bad, "1.0")
    assert not p.contract_ok("1.0", bad)


def test_replies():
    r = p.svc_reply("1.0", x=1)
    assert r == {"status": "ok", "x": 1, "contract_version": "1.0"}
    e = p.svc_error("1.0", "incompatible", "m")
    assert e["status"] == "error" and e["code"] == "incompatible"
    assert e["contract_version"] == "1.0"
    assert "incompatible" in p.ERROR_CODES
    with pytest.raises(ValueError):
        p.svc_error("1.0", "bogus", "m")
