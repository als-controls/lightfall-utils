# tests/ipc/test_no_lightfall_imports.py
import pathlib
import re

PKG = pathlib.Path(__file__).resolve().parents[2] / "src" / "lightfall_utils" / "ipc"


def test_ipc_package_imports_nothing_from_lightfall():
    offenders = []
    for path in PKG.glob("*.py"):
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if re.match(r"\s*(from|import)\s+lightfall(\.|\s|$)", line):
                offenders.append(f"{path.name}:{n}: {line.strip()}")
    assert offenders == []
