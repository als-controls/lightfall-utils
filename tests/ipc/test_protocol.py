import pytest

from lightfall_utils.ipc.protocol import (
    BASE_ERROR_CODES,
    CONTRACT_VERSION,
    ErrorCodes,
    error_reply,
    ok_reply,
)


def test_ok_reply_carries_the_contract_version():
    assert ok_reply(status="ok", value=3) == {"status": "ok", "value": 3, "contract_version": 1}
    assert CONTRACT_VERSION == 1


def test_error_reply_shape_for_a_base_code():
    assert error_reply("denied", "nope") == {
        "status": "error", "code": "denied", "message": "nope", "contract_version": 1,
    }


def test_lightfall_s_codes_are_all_in_the_base_set():
    # The seven codes Lightfall's remote-control contract v1 documents.
    assert {"busy", "limits", "timeout", "unknown", "denied", "bad_request",
            "version_mismatch"} <= BASE_ERROR_CODES


def test_an_unknown_code_is_a_programming_error():
    with pytest.raises(ValueError):
        error_reply("not_a_code", "x")


def test_an_app_extends_the_set_without_touching_the_base():
    codes = ErrorCodes("too_large", "confirmation_required")
    assert codes.reply("too_large", "1.2 MiB > 1 MiB")["code"] == "too_large"
    assert codes.reply("denied", "bare subject")["code"] == "denied"  # base still valid
    with pytest.raises(ValueError):
        codes.reply("busy_beaver", "x")
    assert "too_large" not in BASE_ERROR_CODES
