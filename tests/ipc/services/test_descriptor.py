import dataclasses

import pytest

from lightfall_utils.ipc.services.descriptor import ServiceDescriptor


def _mk(**kw):
    base = {
        "name": "motion",
        "scope": "{prefix}.host.{hostname}.{client_instance}",
        "contract": "1.0",
        "launch": "local",
        "command": ("python", "-m", "svc"),
    }
    base.update(kw)
    return ServiceDescriptor(**base)


def test_valid_local_descriptor_defaults():
    d = _mk()
    assert d.stop_with_client is True
    assert d.single_instance is True
    assert d.interpreter_pref is None
    assert d.hint == ""
    with pytest.raises(dataclasses.FrozenInstanceError):
        d.name = "x"


def test_external_needs_no_command():
    d = _mk(launch="external", command=(), scope="{prefix}.shared")
    assert d.command == ()


@pytest.mark.parametrize(
    "kw",
    [
        {"name": ""},
        {"name": "Bad Name"},
        {"name": "a.b"},
        {"contract": "1"},
        {"contract": "1.x"},
        {"contract": "v1.0"},
        {"contract": "1.0\n"},
        {"command": ()},
        {"scope": "_INBOX.x"},
        {"scope": "_PRIV.x"},
        {"scope": ""},
        {"scope": ".x"},
    ],
)
def test_invalid_raises(kw):
    with pytest.raises(ValueError):
        _mk(**kw)


def test_scope_first_token_from_prefix_rejected():
    d = _mk(scope="{prefix}.x", launch="external", command=())
    with pytest.raises(ValueError):
        d.resolved_scope("_PRIV", "h", "ab12")
    with pytest.raises(ValueError):
        d.resolved_scope("", "h", "ab12")


def test_resolved_scope():
    d = _mk()
    assert d.resolved_scope("als.7011", "tsuru", "ab12") == "als.7011.host.tsuru.ab12"


def test_resolved_scope_sanitizes_hostname():
    d = _mk()
    assert d.resolved_scope("als", "My_Host.lbl.gov", "ab") == "als.host.my_host-lbl-gov.ab"


@pytest.mark.parametrize(
    "scope", ["x.*", "x.>", "x..y", "x.", "x.a b", "x.{prefix}*", "*.x", ">", "x.\ty"]
)
def test_every_scope_token_validated(scope):
    with pytest.raises(ValueError):
        _mk(scope=scope, launch="external", command=())


@pytest.mark.parametrize("prefix", ["als.*", "als.>", "als..7011", "als.", "a b", "als.\n"])
def test_wildcard_or_empty_tokens_in_prefix_rejected(prefix):
    d = _mk(scope="{prefix}.x", launch="external", command=())
    with pytest.raises(ValueError):
        d.resolved_scope(prefix, "h", "ab12")


def test_multi_token_prefix_accepted():
    d = _mk(scope="{prefix}.x", launch="external", command=())
    assert d.resolved_scope("als.7011", "h", "ab12") == "als.7011.x"
