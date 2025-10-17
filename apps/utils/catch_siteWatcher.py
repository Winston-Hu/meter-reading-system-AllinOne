# utils/site_watcher.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional, Callable
import time
import yaml

from .catch_mqtt import MqttSiteClient, OnMsg

REQUIRED_FILES = ("ca.pem", "cert.pem", "key.pem")


def has_required_files(p: Path) -> bool:
    return all((p / name).exists() for name in REQUIRED_FILES)


def load_site_yml(site_dir: Path) -> dict:
    """
    Load optional site.yml with keys:
      - broker_host, broker_port, topics (list), client_id
    """
    cfg = {}
    yml = site_dir / "site.yml"
    if yml.exists():
        with open(yml, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    return cfg


class SiteWatcher:
    """
    Poll a chirpstack config root, manage one MQTT client per site folder.
    """

    def __init__(self, root: Path, interval: float = 10.0, on_message: Optional[OnMsg] = None):
        self.root = Path(root)
        self.interval = interval
        self.on_message = on_message or (lambda s, t, p: None)
        self.clients: Dict[str, MqttSiteClient] = {}
        print(f"[watcher] root={self.root} interval={self.interval}s")

    def scan_once(self):
        # discover current site dirs
        current: Dict[str, Path] = {p.name: p for p in self.root.iterdir() if p.is_dir()}

        # start new sites
        for name, p in current.items():
            if name not in self.clients:
                if has_required_files(p):
                    cfg = load_site_yml(p)
                    cli = MqttSiteClient(
                        site_name=name,
                        broker_host=cfg.get("broker_host", "localhost"),
                        broker_port=int(cfg.get("broker_port", 8883)),
                        site_dir=p,
                        topics=cfg.get("topics", ["#"]),
                        client_id=cfg.get("client_id"),
                        on_message=self.on_message,
                    )
                    cli.start()
                    self.clients[name] = cli
                    print(f"[watcher] started site: {name}")
                else:
                    print(f"[watcher] skip '{name}': missing TLS files {REQUIRED_FILES}")

        # stop removed sites
        to_stop = [n for n in self.clients.keys() if n not in current]
        for n in to_stop:
            try:
                self.clients[n].stop()
            finally:
                self.clients.pop(n, None)
                print(f"[watcher] removed site: {n}")

    def run_forever(self):
        while True:
            try:
                self.scan_once()
            except Exception as e:
                print(f"[watcher] scan error: {e}")
            time.sleep(self.interval)
