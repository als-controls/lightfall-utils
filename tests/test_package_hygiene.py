"""Guards the one-way dependency arrow: lightfall_utils never imports lightfall."""

import re
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent.parent / "src" / "lightfall_utils"
FORBIDDEN = re.compile(r"^\s*(?:from|import)\s+lightfall\.", re.MULTILINE)


def test_no_lightfall_imports():
    offenders = [
        str(p)
        for p in PACKAGE.rglob("*.py")
        if FORBIDDEN.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == [], f"lightfall_utils must not import from lightfall: {offenders}"


def test_package_imports():
    import lightfall_utils

    assert lightfall_utils.__version__
