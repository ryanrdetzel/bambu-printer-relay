import json
import sqlite3
import ssl
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt


def setup_database() -> None:
    conn: sqlite3.Connection = sqlite3.connect("printer_data.db")
    c: sqlite3.Cursor = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_data (
            serial TEXT PRIMARY KEY,
            payload JSON,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            serial TEXT,
            payload JSON,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """
    )
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_raw_history_serial ON raw_history(serial);
    """
    )
    c.execute("DROP TRIGGER IF EXISTS maintain_raw_history_size")
    c.execute(
        """
        CREATE TRIGGER maintain_raw_history_size
        AFTER INSERT ON raw_history
        BEGIN
            DELETE FROM raw_history
            WHERE timestamp < datetime('now', '-48 hours');
        END;
        """
    )
    conn.commit()
    conn.close()


class PrinterConnection:
    def __init__(self, ip: str, serial: str, access_code: str) -> None:
        self.ip: str = ip
        self.serial: str = serial
        self.access_code: str = access_code
        self.client: Optional[mqtt.Client] = None
        self.last_message_time: float = 0
        self.connected: bool = False
        self.lock: threading.Lock = threading.Lock()
        self.last_payload: Optional[Dict[str, Any]] = None
        self.in_memory_payload: Dict[str, Any] = {}
        self.db_lock: threading.Lock = threading.Lock()

    def store_payload(
        self, full_payload: Dict[str, Any], payload: Dict[str, Any]
    ) -> None:
        with self.db_lock:
            try:
                conn: sqlite3.Connection = sqlite3.connect("printer_data.db")
                c: sqlite3.Cursor = conn.cursor()
                c.execute(
                    """
                    INSERT OR REPLACE INTO raw_data (serial, payload, timestamp)
                    VALUES (?, ?, ?)
                """,
                    (self.serial, json.dumps(full_payload), datetime.now().isoformat()),
                )
                c.execute(
                    """
                    INSERT INTO raw_history (serial, payload, timestamp)
                    VALUES (?, ?, ?)
                """,
                    (self.serial, json.dumps(payload), datetime.now().isoformat()),
                )
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Database error: {str(e)}")

    def refresh(self) -> None:
        ANNOUNCE_PUSH: Dict[str, Any] = {
            "pushing": {
                "command": "pushall",
                "push_target": 1,
                "sequence_id": "0",
                "version": 1,
            }
        }

        ANNOUNCE_VERSION: Dict[str, Any] = {
            "info": {"command": "get_version", "sequence_id": "0"}
        }
        self.client.publish(f"device/{self.serial}/request", json.dumps(ANNOUNCE_PUSH))
        self.client.publish(
            f"device/{self.serial}/request",
            json.dumps(ANNOUNCE_VERSION),
        )

    def connect(self) -> None:
        print("Connecting to printer")
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        self.client.tls_set(tls_version=ssl.PROTOCOL_TLS, cert_reqs=ssl.CERT_NONE)
        self.client.tls_insecure_set(True)

        self.client.username_pw_set("bblp", password=self.access_code)

        try:
            self.client.connect(self.ip, 8883, 60)
            self.client.loop_start()
        except Exception as e:
            print(f"Failed to connect to printer {self.serial}: {str(e)}")

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Dict[str, Any],
        reason_code: int,
        properties: Any,
    ) -> None:
        print(f"Connected to printer {self.serial}")
        self.connected = True
        client.subscribe(f"device/{self.serial}/report")
        self.refresh()

    def deep_update(
        self, mapping: Dict[str, Any], *updating_mappings: Dict[str, Any]
    ) -> Dict[str, Any]:
        updated_mapping = mapping.copy()
        for updating_mapping in updating_mappings:
            for k, v in updating_mapping.items():
                if (
                    k in updated_mapping
                    and isinstance(updated_mapping[k], dict)
                    and isinstance(v, dict)
                ):
                    updated_mapping[k] = self.deep_update(updated_mapping[k], v)
                else:
                    updated_mapping[k] = v
        return updated_mapping

    def _on_message(
        self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage
    ) -> None:
        self.last_message_time = time.time()
        try:
            payload: Dict[str, Any] = json.loads(msg.payload.decode())
            if "print" not in payload:
                return
            payload = payload["print"]

            if not self.in_memory_payload:
                self.in_memory_payload = payload
            else:
                self.in_memory_payload = self.deep_update(
                    self.in_memory_payload, payload
                )

            self.store_payload(self.in_memory_payload, payload)

        except Exception as e:
            print(f"Failed to parse message from {self.serial}: {str(e)}")
            print(f"Raw message: {msg.payload.decode()}")
            raise e

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        print(f"Disconnected from printer {self.serial}")
        self.connected = False

    def send_message(self, payload: Dict[str, Any]) -> bool:
        with self.lock:
            if self.connected and self.client:
                try:
                    self.client.publish(
                        f"device/{self.serial}/request", json.dumps(payload)
                    )
                    return True
                except:
                    print(f"Failed to send message to {self.serial}")
        return False


class PrinterManager:
    def __init__(self) -> None:
        self.printers: Dict[str, PrinterConnection] = {}
        self.watchdog_thread: Optional[threading.Thread] = None
        self._running: bool = True

    def add_printer(self, ip: str, serial: str, access_code: str) -> None:
        printer = PrinterConnection(ip, serial, access_code)
        self.printers[serial] = printer
        printer.connect()

    def stop(self) -> None:
        self._running = False
        if self.watchdog_thread:
            self.watchdog_thread.join()

        for serial, printer in self.printers.items():
            if printer.client:
                print(f"Disconnecting from printer {serial}")
                printer.client.loop_stop()
                printer.client.disconnect()

    def start_watchdog(self) -> None:
        def watchdog() -> None:
            while self._running:
                current_time = time.time()
                for serial, printer in self.printers.items():
                    if (
                        not printer.connected
                        or (current_time - printer.last_message_time) > 30
                    ):
                        print(f"Reconnecting to printer {serial}")
                        printer.connect()
                time.sleep(5)

        self.watchdog_thread = threading.Thread(target=watchdog)
        self.watchdog_thread.daemon = True
        self.watchdog_thread.start()

    def send_message(self, serial: str, payload: Dict[str, Any]) -> bool:
        if serial in self.printers:
            return self.printers[serial].send_message(payload)
        return False


def main() -> None:
    setup_database()

    with open("config.local.json") as config_file:
        printers: List[Dict[str, str]] = json.load(config_file)

    manager: PrinterManager = PrinterManager()

    for printer in printers:
        manager.add_printer(printer["ip"], printer["serial"], printer["access_code"])

    manager.start_watchdog()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        manager.stop()


if __name__ == "__main__":
    main()
