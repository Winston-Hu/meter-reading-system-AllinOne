# apps/utils/parsers/hri485_pulse.py
from typing import Dict, Any, List
from datetime import datetime
import uuid
import logging

from apps.utils.catch_mappingLookup import fill_from_mapping
from logs.logging_setup import get_logger


LOG = get_logger(
    "aquasense_v2",
    file_name="parsers.log",
    max_bytes=5 * 1024 * 1024,
    backup_count=5,
    level=logging.INFO,
    also_console=True
)


def _parse_ts(ts_str: str) -> datetime:
    # Normalize to microseconds for fromisoformat compatibility
    if ts_str and "." in ts_str and "+" in ts_str:
        frac, rest = ts_str.split("+", 1)
        left, dot, decimals = frac.partition(".")
        ts_str = left + "." + decimals[:6] + "+" + rest
    return datetime.fromisoformat(ts_str) if ts_str else datetime.utcnow()


def parse(payload: Dict[str, Any], site_name: str) -> List[Dict[str, Any]]:
    schema = site_name

    # deviceInfo
    di = payload.get("deviceInfo", {}) or {}
    dev_eui = di.get("devEui")
    fcnt = payload.get("fCnt")

    ts_str = payload.get("rxInfo", [{}])[0].get("nsTime")
    ts = _parse_ts(ts_str)

    rssi = snr = gateway_id = None
    # rxInfo
    if payload.get("rxInfo"):
        rx0 = payload["rxInfo"][0]
        rssi = rx0.get("rssi")
        snr = rx0.get("snr")
        gateway_id = rx0.get("gatewayId")

    sf = payload.get("txInfo", {}).get("modulation", {}).get("lora", {}).get("spreadingFactor")

    # object
    obj = payload.get("object") or {}
    pulses = [
        obj.get("pulse")[0],  # X0
        obj.get("pulse")[1],  # X1
        obj.get("pulse")[2],  # X2
        obj.get("pulse")[3],  # X3
        obj.get("pulse", [0, 0, 0, 0, 0, 0, 0, 0])[4] if len(obj.get("pulse")) > 4 else 0,  # X4
        obj.get("pulse", [0, 0, 0, 0, 0, 0, 0, 0])[5] if len(obj.get("pulse")) > 5 else 0,  # X5
        obj.get("pulse", [0, 0, 0, 0, 0, 0, 0, 0])[6] if len(obj.get("pulse")) > 6 else 0,  # X6
        obj.get("pulse", [0, 0, 0, 0, 0, 0, 0, 0])[7] if len(obj.get("pulse")) > 7 else 0,  # X7
    ]

    # return [] if 'None' in pulses
    if any(v is None for v in pulses):
        LOG.warning(f"[parser] skip {dev_eui} — pulses contain None: {pulses}")
        return []

    valve_1 = obj.get("motor1")
    valve_2 = obj.get("motor2")
    is_leak = obj.get("leak")
    battery = obj.get("battery", 100)

    cfg1 = str(int(obj.get("cfg1", 2)))
    cfg2 = str(int(obj.get("cfg2", 2)))
    cfg = cfg1+cfg2

    rows: List[Dict[str, Any]] = []
    for end_node_id, reading in enumerate(pulses):
        rows.append({
            "uuid_event_id": str(uuid.uuid4()),
            "meter_serial":  None,
            "site":          None,
            "level":         None,
            "location_id":   None,
            "dev_eui":       dev_eui,
            "end_node_id":   end_node_id,
            "reading":       float(reading) if reading is not None else None,
            "timestamp":     ts,
            "rssi":          rssi,
            "snr":           snr,
            "spreading_factor": sf,
            "gateway_id":    gateway_id,
            "fcnt":          fcnt,
            "litter_factor": None,
            "nmi":           None,
            "valve_1":       valve_1,
            "valve_2":       valve_2,
            "valve_react1":  None,
            "is_leak":       is_leak,
            "battery":       battery,
            "cfg":           cfg,
            "schema":        schema,
        })
        # mapping file info
        """
        litter_factor <-> litter_factor
        nmi <-> HWMETERNMI
        meter_serial <-> HWMETERNO,
        site <-> Address
        level <-> Level,
        location_id <-> ApartmentNo,
        """
        for r in rows:
            fill_from_mapping(r)

    return rows
