"""Minimal caproto soft IOC run as a subprocess by the CA bridge tests."""

import sys

from caproto.server import PVGroup, pvproperty, run


class BridgeTestIOC(PVGroup):
    value = pvproperty(value=42.0, name="value")
    text = pvproperty(value="hello", name="text", string_encoding="utf-8")


if __name__ == "__main__":
    prefix = sys.argv[1]
    ioc = BridgeTestIOC(prefix=prefix)
    run(ioc.pvdb, log_pv_names=False)
