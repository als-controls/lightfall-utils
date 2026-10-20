import subprocess
import sys


def test_capability_and_services_import_without_qt():
    code = (
        "import sys; import lightfall_utils.ipc.capability, "
        "lightfall_utils.ipc.services, lightfall_utils.ipc.services.core; print('PySide6' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert out == "False"


def test_lazy_ipcservice_still_importable():
    from lightfall_utils.ipc import IPCService, TrustState

    assert IPCService.__name__ == "IPCService"
    assert TrustState is not None
