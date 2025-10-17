# apps/utils/parsers/hri485_pulse.py
from typing import Dict, Any, List
from datetime import datetime
import uuid

from apps.utils.catch_mappingLookup import fill_from_mapping


def _parse_ts(ts_str: str) -> datetime:
    # Normalize to microseconds for fromisoformat compatibility
    if ts_str and "." in ts_str and "+" in ts_str:
        frac, rest = ts_str.split("+", 1)
        left, dot, decimals = frac.partition(".")
        ts_str = left + "." + decimals[:6] + "+" + rest
    return datetime.fromisoformat(ts_str) if ts_str else datetime.utcnow()


def parse(payload: Dict[str, Any], site_name: str) -> List[Dict[str, Any]]:
    schema = site_name

    di = payload.get("deviceInfo", {}) or {}
    dev_eui = di.get("devEui")
    fcnt = payload.get("fCnt")

    ts_str = (payload.get("rxInfo", [{}])[0].get("nsTime")
              or payload.get("time"))
    ts = _parse_ts(ts_str)

    rssi = snr = gateway_id = None
    if payload.get("rxInfo"):
        rx0 = payload["rxInfo"][0]
        rssi = rx0.get("rssi")
        snr = rx0.get("snr")
        gateway_id = rx0.get("gatewayId")

    sf = payload.get("txInfo", {}).get("modulation", {}).get("lora", {}).get("spreadingFactor")

    obj = payload.get("object") or {}
    pulses = [
        obj.get("X0_PulseCount"),
        obj.get("X1_PulseCount"),
        obj.get("X2_PulseCount"),
        obj.get("X3_PulseCount"),
        obj.get("X4_PulseCount"),
        obj.get("X5_PulseCount"),
        obj.get("X6_PulseCount"),
        obj.get("X7_PulseCount"),
    ]
    if pulses:
        for v in range(len(pulses)):
            if pulses[v] is None:
                print(f"CANNOT GET value in index {v} with devEUI={dev_eui}")

    valve_1 = obj.get("motor1")
    valve_2 = obj.get("motor2")
    is_leak = obj.get("leak")
    battery = obj.get("battery", 100)

    cfg1 = str(obj.get("cfg1", 2))
    cfg2 = str(obj.get("cfg2", 2))
    cfg = cfg1 + cfg2

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
