import json
import sqlite3
import ssl
import threading
import time
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Collection,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import paho.mqtt.client as mqtt


def setup_database():
    conn = sqlite3.connect("printer_data.db")
    c = conn.cursor()
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
    # -- Create a trigger that deletes older records after insert
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
    def __init__(self, ip: str, serial: str, access_code: str):
        self.ip = ip
        self.serial = serial
        self.access_code = access_code
        self.client = None
        self.last_message_time = 0
        self.connected = False
        self.lock = threading.Lock()
        self.last_payload = None  #
        self.in_memory_payload = {}
        self.db_lock = threading.Lock()

    def store_payload(self, full_payload, payload):
        """Store payload in SQLite database"""
        with self.db_lock:
            try:
                conn = sqlite3.connect("printer_data.db")
                c = conn.cursor()
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

    def refresh(self):
        """
        Triggers a full data refresh from the printer (if it is connected).  You should use this
        method sparingly as resorting to it indicates something is not working properly.
        """
        ANNOUNCE_PUSH = {
            "pushing": {
                "command": "pushall",
                "push_target": 1,
                "sequence_id": "0",
                "version": 1,
            }
        }

        ANNOUNCE_VERSION = {"info": {"command": "get_version", "sequence_id": "0"}}
        self.client.publish(f"device/{self.serial}/request", json.dumps(ANNOUNCE_PUSH))
        self.client.publish(
            f"device/{self.serial}/request",
            json.dumps(ANNOUNCE_VERSION),
        )

    def connect(self):
        print("Connecting to printer")
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        # Setup TLS
        self.client.tls_set(tls_version=ssl.PROTOCOL_TLS, cert_reqs=ssl.CERT_NONE)
        self.client.tls_insecure_set(True)

        # Set credentials
        self.client.username_pw_set("bblp", password=self.access_code)

        try:
            self.client.connect(self.ip, 8883, 60)
            self.client.loop_start()
        except Exception as e:
            print(f"Failed to connect to printer {self.serial}: {str(e)}")

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"Connected to printer {self.serial}")
        self.connected = True
        # Subscribe to updates
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

    def _on_message(self, client, userdata, msg):
        self.last_message_time = time.time()
        try:
            payload = json.loads(msg.payload.decode())
            # If it contains a print section
            if "print" not in payload:
                # print(json.dumps(payload, indent=2))
                return
            payload = payload["print"]

            if not self.in_memory_payload:
                self.in_memory_payload = payload
            else:
                # print(json.dumps(self.in_memory_payload, indent=2))
                self.in_memory_payload = self.deep_update(
                    self.in_memory_payload, payload
                )

            self.store_payload(self.in_memory_payload, payload)

        except Exception as e:
            print(f"Failed to parse message from {self.serial}: {str(e)}")
            print(f"Raw message: {msg.payload.decode()}")
            raise e

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        print(f"Disconnected from printer {self.serial}")
        self.connected = False

    def send_message(self, payload: dict):
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
    def __init__(self):
        self.printers: Dict[str, PrinterConnection] = {}
        self.watchdog_thread = None
        self._running = True  # Add this flag

    def add_printer(self, ip: str, serial: str, access_code: str):
        printer = PrinterConnection(ip, serial, access_code)
        self.printers[serial] = printer
        printer.connect()

    def stop(self):
        """Cleanly stop all printer connections"""
        self._running = False
        if self.watchdog_thread:
            self.watchdog_thread.join()

        for serial, printer in self.printers.items():
            if printer.client:
                print(f"Disconnecting from printer {serial}")
                printer.client.loop_stop()
                printer.client.disconnect()

    def start_watchdog(self):
        def watchdog():
            while self._running:  # Change while True to use flag
                current_time = time.time()
                for serial, printer in self.printers.items():
                    # Check if printer needs reconnection
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

    def send_message(self, serial: str, payload: dict) -> bool:
        if serial in self.printers:
            return self.printers[serial].send_message(payload)
        return False


def main():
    setup_database()

    # TODO: read this from the database
    printers = [
        {"ip": "192.168.1.168", "serial": "00M00A340700227", "access_code": "ce776842"},
        {"ip": "192.168.1.61", "serial": "0309CA471100688", "access_code": "15259417"},
    ]

    manager = PrinterManager()

    # Add all printers
    for printer in printers:
        manager.add_printer(printer["ip"], printer["serial"], printer["access_code"])

    # Start watchdog
    manager.start_watchdog()

    # def send_test_message():
    #     while True:
    #         time.sleep(10)
    #         print("send commands in queue")
    #         # test_payload = {"print": {"command": "get_status"}}
    #         # for serial in manager.printers.keys():
    #         #     manager.send_message(serial, test_payload)

    # test_thread = threading.Thread(target=send_test_message)
    # test_thread.daemon = True
    # test_thread.start()

    # Keep main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        manager.stop()  #


if __name__ == "__main__":
    main()
