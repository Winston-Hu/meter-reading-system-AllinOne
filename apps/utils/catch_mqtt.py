from __future__ import annotations
import threading
from typing import List, Callable, Optional
import paho.mqtt.client as mqtt
from pathlib import Path
import logging

from logs.logging_setup import get_logger


LOG = get_logger(
    "catch_mqtt",
    file_name="utils.log",
    max_bytes=5 * 1024 * 1024,
    backup_count=5,
    level=logging.INFO,
    also_console=True
)


OnMsg = Callable[[str, str, bytes], None]  # (site_name, topic, payload)


class MqttSiteClient:
    """
    A thin wrapper around paho-mqtt for one site.
    - Encapsulates TLS setup, connect/reconnect, subscribe management, and clean stop.
    - Exposes a simple on_message callback.
    """

    def __init__(
        self,
        site_name: str,
        broker_host: str,
        broker_port: int,
        site_dir: Path,
        topics: Optional[List[str]] = None,
        client_id: Optional[str] = None,
        on_message: Optional[OnMsg] = None,
    ):
        self.site_name = site_name
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.site_dir = Path(site_dir)
        self.topics = topics or ["#"]
        self.client_id = client_id or f"meterwatch-{site_name}"
        self.on_message_cb = on_message or (lambda s, t, p: None)

        # paho client + thread control
        self.client = mqtt.Client(client_id=self.client_id, clean_session=True)
        self._stop_evt = threading.Event()

        # TLS
        ca = str(self.site_dir / "ca.pem")
        cert = str(self.site_dir / "cert.pem")
        key = str(self.site_dir / "key.pem")
        self.client.tls_set(ca_certs=ca, certfile=cert, keyfile=key)
        # self.client.tls_insecure_set(False)  # enable if broker has proper cert chain

        # callbacks
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        # background loop thread
        self._loop_started = False

    # ------ public API ------

    def start(self):
        """Start the client in background (non-blocking)."""
        self.client.connect_async(self.broker_host, int(self.broker_port), keepalive=60)
        if not self._loop_started:
            self.client.loop_start()
            self._loop_started = True

    def stop(self):
        """Gracefully stop the client and background loop."""
        self._stop_evt.set()
        try:
            self.client.loop_stop()
        except Exception:
            pass
        try:
            self.client.disconnect()
        except Exception:
            pass

    def set_topics(self, topics: List[str]):
        """
        Dynamically update subscriptions.
        - Unsubscribe previous topics, subscribe new ones.
        """
        prev = set(self.topics)
        new = set(topics or [])
        # unsubscribe removed
        for t in prev - new:
            try:
                self.client.unsubscribe(t)
            except Exception:
                pass
        # subscribe added
        for t in new - prev:
            self.client.subscribe(t, qos=0)
        self.topics = list(new)

    # ------ paho callbacks ------

    def _on_connect(self, client, userdata, flags, rc):
        LOG.info(f"[{self.site_name}] connected rc={rc}")
        if rc == 0:
            for t in self.topics:
                client.subscribe(t, qos=0)
                LOG.info(f"[{self.site_name}] subscribed: {t}")

    def _on_message(self, client, userdata, msg):
        # forward to external handler
        try:
            self.on_message_cb(self.site_name, msg.topic, msg.payload)
        except Exception as e:
            LOG.exception(f"[{self.site_name}] on_message error: {e}")

    def _on_disconnect(self, client, userdata, rc):
        LOG.info(f"[{self.site_name}] disconnected rc={rc}")
