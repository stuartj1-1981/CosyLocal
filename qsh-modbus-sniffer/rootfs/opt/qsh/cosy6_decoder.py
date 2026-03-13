#!/usr/bin/env python3
"""
QSH Modbus Passive Sniffer v3 — Home Assistant Add-on
======================================================

Connects to Waveshare RS485-to-WiFi gateway in transparent mode.
Passively captures ALL Modbus RTU traffic between Cosy Hub (master)
and outdoor unit (slave address 10).

Features:
    - Robust socket reconnection with exponential backoff
    - MQTT auto-discovery for Home Assistant
    - CSV logging of all parsed frames
    - JSON register map (auto-updated when new registers seen)
    - Operating state detection (ACTIVE / IDLE / HEARTBEAT)
    - Signed int16 handling for temperature-range registers

Bus parameters: 19200 baud, 8N1, Slave address 10 (0x0A)
Polling cycle (when active):
    1. Write 7 regs at 91    (hub → outdoor, setpoints)
    2. Write 2 regs at 0     (hub → outdoor, commands)
    3. Read 34 regs at 19    (outdoor → hub, main sensors)
    4. Read 28 regs at 53    (outdoor → hub, secondary)
    5. Read 1 reg at 210     (outdoor → hub, status)
"""

import socket
import struct
import time
import json
import csv
import os
import sys
import signal
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

try:
    import paho.mqtt.client as mqtt
    HAS_MQTT = True
except ImportError:
    HAS_MQTT = False
    logging.warning("paho-mqtt not installed. MQTT publishing disabled.")


# =============================================================================
# Configuration
# =============================================================================

DEFAULT_CONFIG = {
    "gateway_host": "192.168.2.73",
    "gateway_port": 8899,
    "mqtt_host": "192.168.2.183",
    "mqtt_port": 1883,
    "mqtt_user": "mqtt",
    "mqtt_pass": "Minbin121781",
    "mqtt_base_topic": "Cosy HP",
    "slave_address": 10,
    "log_dir": "/data/modbus_logs",
    "frame_timeout_ms": 50,
    "reconnect_delay": 5,
    "reconnect_max_delay": 60,
    "publish_interval": 5,
    "socket_timeout": 2.0,
}

FUNCTION_CODES = {
    0x01: "Read Coils",
    0x02: "Read Discrete Inputs",
    0x03: "Read Holding Registers",
    0x04: "Read Input Registers",
    0x05: "Write Single Coil",
    0x06: "Write Single Register",
    0x0F: "Write Multiple Coils",
    0x10: "Write Multiple Registers",
    0x17: "Read/Write Multiple Registers",
}

# Registers that can carry negative values (temperatures, offsets, timers).
SIGNED_REGISTERS = {
    29, 30, 32, 36, 37, 38, 39, 40, 41, 43, 44, 45, 50,
    53, 54, 55, 56, 57, 91,
}

# All known register addresses from the polling cycle
ALL_REGISTERS = set(range(19, 53)) | set(range(53, 81)) | {210} | set(range(91, 98)) | {0, 1}

# =============================================================================
# Confirmed Register Names (from AP mode + Modbus sniffing correlation)
# scale: multiply raw (signed) value by this to get real value
#
# Confidence levels:
#   CONFIRMED  = cross-validated against independent sensor or calculation
#   NAMED      = identified from AP mode labels, not independently verified
#   UNCONFIRMED = behaviour observed but identity not proven
# =============================================================================
REGISTER_NAMES = {
    # --- Temperatures (raw × 0.1 = °C) ---
    # CONFIRMED: r=0.999 vs flow temp sensor (n=1206)
    29: {"name": "Flow Temp",            "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-water",    "class": "temperature"},
    # CONFIRMED: correlates with return pipe sensor
    30: {"name": "Return Temp",          "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # CONFIRMED: r=1.000 vs Octopus API outdoor_temperature (n=402)
    36: {"name": "T1 External Temp",     "scale": 0.1,  "unit": "°C",    "icon": "mdi:home-thermometer",     "class": "temperature"},
    # NAMED: AP mode label. Range 21-37°C suggests indoor/room sensor.
    37: {"name": "T2 Intermediate",      "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED: AP mode label. Cold-side, correlates with evaporator (r=0.81)
    38: {"name": "T3 Suction",           "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED: AP mode label. Coldest sensor in circuit, tracks below outdoor
    39: {"name": "Evaporator Temp",      "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-low",      "class": "temperature"},
    # NAMED: AP mode label. Water return, always below flow temp
    40: {"name": "T5 Return Temp",       "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED: AP mode label. Compressor sump temperature
    41: {"name": "T6 Sump",              "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED: AP mode label. Liquid line temperature
    43: {"name": "T8 Liquid",            "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED: AP mode label. Flow temperature (secondary sensor?)
    44: {"name": "T9 Flow Temp",         "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-water",    "class": "temperature"},
    # NAMED: AP mode label. Compressor discharge
    45: {"name": "T10 Discharge",        "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-high",     "class": "temperature"},
    # CONFIRMED: target flow temperature setpoint (hub → outdoor)
    91: {"name": "Target Flow Temp",     "scale": 0.1,  "unit": "°C",    "icon": "mdi:target",               "class": "temperature"},

    # --- Pressures (raw × 0.01 = bar) ---
    48: {"name": "Discharge Pressure",   "scale": 0.01, "unit": "bar",   "icon": "mdi:gauge-full",           "class": None},
    50: {"name": "Suction Pressure",     "scale": 0.01, "unit": "bar",   "icon": "mdi:gauge",                "class": "pressure"},

    # --- Flow rate (raw × 0.01 = l/min) ---
    # NAMED: Sika VVX20 flow meter built into unit
    47: {"name": "Flow Rate",            "scale": 0.01, "unit": "l/min", "icon": "mdi:water-pump",           "class": None},

    # --- Valve positions / percentages (raw × 0.1 = %) ---
    32: {"name": "V1 Heating",           "scale": 0.1,  "unit": "%",     "icon": "mdi:valve",                "class": None},
    34: {"name": "V3 Defrost",           "scale": 0.1,  "unit": "%",     "icon": "mdi:snowflake-melt",       "class": None},
    35: {"name": "V4 Inverter",          "scale": 0.1,  "unit": "%",     "icon": "mdi:sine-wave",            "class": None},

    # --- Power (W) ---
    # CONFIRMED: r=0.999 vs Shelly EM (n=1206)
    27: {"name": "Electrical Power In",  "scale": 1,    "unit": "W",     "icon": "mdi:flash",                "class": "power"},
    # CONFIRMED: r=0.999 vs flow×ΔT thermal calculation
    64: {"name": "Heat Output",          "scale": 1,    "unit": "W",     "icon": "mdi:fire",                 "class": "power"},

    # --- Fan ---
    61: {"name": "Fan Speed",            "scale": 1,    "unit": "RPM",   "icon": "mdi:fan",                  "class": None},

    # --- Rated specs (constant) ---
    59: {"name": "Rated Heat Capacity",  "scale": 1,    "unit": "W",     "icon": "mdi:information",          "class": None},
    60: {"name": "Rated Elec Input",     "scale": 1,    "unit": "W",     "icon": "mdi:information",          "class": None},

    # --- UNCONFIRMED ---
    # Strong candidate for compressor speed but not proven
    63: {"name": "Unknown 63",           "scale": 1,    "unit": "",      "icon": "mdi:help-circle",          "class": None},

    # --- Hub control registers ---
    92: {"name": "Mode Demand",          "scale": 1,    "unit": "",      "icon": "mdi:thermostat",           "class": None},
    210:{"name": "Status Register",      "scale": 1,    "unit": "",      "icon": "mdi:information",          "class": None},
}


# =============================================================================
# CRC-16/Modbus
# =============================================================================

def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def verify_crc(frame: bytes) -> bool:
    if len(frame) < 4:
        return False
    return crc16_modbus(frame[:-2]) == struct.unpack('<H', frame[-2:])[0]


def to_signed(value: int) -> int:
    return value - 0x10000 if value >= 0x8000 else value


# =============================================================================
# Frame Parser
# =============================================================================

class ModbusFrame:
    def __init__(self, raw: bytes, timestamp: float):
        self.raw = raw
        self.timestamp = timestamp
        self.dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        self.valid_crc = verify_crc(raw)
        self.address = raw[0] if len(raw) > 0 else None
        self.function_code = raw[1] if len(raw) > 1 else None
        self.is_exception = bool(self.function_code and self.function_code & 0x80)
        self.is_request = None
        self.registers = {}
        self.coils = {}
        self.start_register = None
        self.register_count = None
        self.parsed = False
        self.parse_error = None

        if self.valid_crc and not self.is_exception:
            try:
                self._parse()
                self.parsed = True
            except Exception as e:
                self.parse_error = str(e)

    def _parse(self):
        fc = self.function_code
        data = self.raw[2:-2]  # Strip address, FC, CRC

        if fc in (0x01, 0x02):
            # Read Coils / Read Discrete Inputs
            if len(data) == 4:
                self.is_request = True
                self.start_register = struct.unpack('>H', data[0:2])[0]
                self.register_count = struct.unpack('>H', data[2:4])[0]
            elif len(data) >= 1:
                self.is_request = False
                self._coil_bytes = data[1:1 + data[0]]

        elif fc in (0x03, 0x04):
            # Read Holding Registers / Read Input Registers
            if len(data) == 4:
                self.is_request = True
                self.start_register = struct.unpack('>H', data[0:2])[0]
                self.register_count = struct.unpack('>H', data[2:4])[0]
            elif len(data) >= 1:
                self.is_request = False
                byte_count = data[0]
                reg_data = data[1:1 + byte_count]
                num_regs = byte_count // 2
                for i in range(num_regs):
                    val = struct.unpack('>H', reg_data[i*2:(i+1)*2])[0]
                    self.registers[i] = val  # Offset from 0 — paired with request for absolute addr

        elif fc == 0x06:
            # Write Single Register
            if len(data) == 4:
                self.is_request = True
                reg = struct.unpack('>H', data[0:2])[0]
                val = struct.unpack('>H', data[2:4])[0]
                self.start_register = reg
                self.register_count = 1
                self.registers[reg] = val

        elif fc == 0x10:
            # Write Multiple Registers
            if len(data) >= 5 and len(data) > 5:
                # REQUEST: start(2) + count(2) + bytes(1) + data
                self.is_request = True
                self.start_register = struct.unpack('>H', data[0:2])[0]
                self.register_count = struct.unpack('>H', data[2:4])[0]
                byte_count = data[4]
                reg_data = data[5:5 + byte_count]
                for i in range(self.register_count):
                    if (i*2 + 2) <= len(reg_data):
                        val = struct.unpack('>H', reg_data[i*2:(i+1)*2])[0]
                        self.registers[self.start_register + i] = val
            elif len(data) == 4:
                # RESPONSE: start(2) + count(2) — echo
                self.is_request = False
                self.start_register = struct.unpack('>H', data[0:2])[0]
                self.register_count = struct.unpack('>H', data[2:4])[0]


# =============================================================================
# Register Tracker
# =============================================================================

class RegisterTracker:
    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.map_file = self.log_dir / "register_map.json"
        self.current_values = {}     # {reg_num: raw_uint16}
        self.current_coils = {}
        self.seen_registers = set()
        self.seen_coils = set()
        self.lock = Lock()
        self._load_map()

    def _load_map(self):
        if self.map_file.exists():
            try:
                data = json.loads(self.map_file.read_text())
                self.seen_registers = set(int(k) for k in data.get("registers", {}).keys())
                self.seen_coils = set(int(k) for k in data.get("coils", {}).keys())
                logging.info(f"Loaded register map: {len(self.seen_registers)} regs, {len(self.seen_coils)} coils")
            except Exception as e:
                logging.warning(f"Failed to load register map: {e}")

    def save_map(self):
        data = {
            "registers": {str(r): {"name": REGISTER_NAMES.get(r, {}).get("name", f"reg_{r}")}
                         for r in sorted(self.seen_registers)},
            "coils": {str(c): {} for c in sorted(self.seen_coils)},
            "updated": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.map_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logging.error(f"Failed to save register map: {e}")

    def update_registers(self, registers: dict, is_write: bool = False):
        new_discoveries = 0
        with self.lock:
            for reg_num, raw_val in registers.items():
                self.current_values[reg_num] = raw_val
                if reg_num not in self.seen_registers:
                    self.seen_registers.add(reg_num)
                    new_discoveries += 1
                    name = REGISTER_NAMES.get(reg_num, {}).get("name", f"reg_{reg_num}")
                    logging.info(f"🔍 NEW register discovered: {reg_num} ({name}) = {raw_val}")
        return new_discoveries

    def update_coils(self, coils: dict):
        with self.lock:
            discoveries = 0
            for coil_num, val in coils.items():
                self.current_coils[coil_num] = val
                if coil_num not in self.seen_coils:
                    self.seen_coils.add(coil_num)
                    discoveries += 1
                    logging.info(f"🔍 NEW coil discovered: {coil_num} = {val}")
            return discoveries


# =============================================================================
# Operating State Detector
# =============================================================================

class OperatingStateDetector:
    """Detect whether the heat pump is actively running or in idle/heartbeat mode."""

    def __init__(self):
        self.current_state = "UNKNOWN"
        self.last_read_time = 0
        self.last_write_time = 0
        self.transitions = 0

    def update(self, frame: ModbusFrame):
        now = frame.timestamp
        old_state = self.current_state

        if frame.function_code == 0x03:  # Read holding registers
            if frame.is_request and frame.start_register == 19:
                self.last_read_time = now

        elif frame.function_code == 0x10:  # Write multiple registers
            self.last_write_time = now

        # If we've seen a read poll recently, system is active
        if (now - self.last_read_time) < 10:
            new_state = "ACTIVE"
        elif (now - self.last_write_time) < 10:
            new_state = "HEARTBEAT"
        else:
            new_state = "IDLE"

        if new_state != old_state:
            self.current_state = new_state
            self.transitions += 1
            logging.info(f"⚡ State: {old_state} → {new_state}")

        return self.current_state


# =============================================================================
# MQTT Publisher
# =============================================================================

class MQTTPublisher:
    def __init__(self, config: dict):
        self.config = config
        self.client = None
        self.connected = False
        self.discovery_sent = set()
        self.base_topic = config["mqtt_base_topic"]

        if HAS_MQTT:
            self._setup()

    def _setup(self):
        self.client = mqtt.Client(client_id="qsh_modbus_sniffer", protocol=mqtt.MQTTv311)
        if self.config["mqtt_user"]:
            self.client.username_pw_set(self.config["mqtt_user"], self.config["mqtt_pass"])
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.will_set(
            f"{self.base_topic}/status",
            "offline", retain=True
        )
        try:
            self.client.connect(self.config["mqtt_host"], self.config["mqtt_port"], keepalive=60)
            self.client.loop_start()
        except Exception as e:
            logging.error(f"MQTT connection failed: {e}")

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            logging.info("MQTT connected")
            self.client.publish(f"{self.base_topic}/status", "online", retain=True)
            # Re-send discovery on reconnect
            self.discovery_sent.clear()
        else:
            logging.error(f"MQTT connect failed: rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logging.warning(f"MQTT disconnected unexpectedly: rc={rc}")

    def _send_discovery(self, reg_num: int):
        if reg_num in self.discovery_sent or not self.connected:
            return

        info = REGISTER_NAMES.get(reg_num, {})
        name = info.get("name", f"Modbus Reg {reg_num}")
        unit = info.get("unit", "")
        icon = info.get("icon", "mdi:register-outline" if not info else None)
        device_class = info.get("class")
        scale = info.get("scale", 1)

        uid = f"qsh_modbus_reg_{reg_num}"
        state_topic = f"{self.base_topic}/reg_{reg_num}/state"

        config_payload = {
            "name": f"Cosy {name}",
            "unique_id": uid,
            "state_topic": state_topic,
            "device": {
                "identifiers": ["qsh_modbus_sniffer"],
                "name": "QSH Modbus Sniffer",
                "manufacturer": "QSH",
                "model": "Cosy 6 Passive Sniffer",
                "sw_version": "3.0.0",
            },
            "availability": {
                "topic": f"{self.base_topic}/status",
            },
        }

        if unit:
            config_payload["unit_of_measurement"] = unit
        if icon:
            config_payload["icon"] = icon
        if device_class:
            config_payload["device_class"] = device_class
            config_payload["state_class"] = "measurement"

        config_topic = f"homeassistant/sensor/qsh_modbus/reg_{reg_num}/config"
        self.client.publish(config_topic, json.dumps(config_payload), retain=True)
        self.discovery_sent.add(reg_num)
        logging.debug(f"Discovery sent for reg_{reg_num}: {name}")

    def publish_registers(self, values: dict, coils: dict, state: str):
        if not self.connected:
            return

        for reg_num, raw_val in values.items():
            self._send_discovery(reg_num)
            info = REGISTER_NAMES.get(reg_num, {})
            scale = info.get("scale", 1)

            if reg_num in SIGNED_REGISTERS:
                val = to_signed(raw_val)
            else:
                val = raw_val

            if scale != 1:
                val = round(val * scale, 2)

            state_topic = f"{self.base_topic}/reg_{reg_num}/state"
            self.client.publish(state_topic, str(val), retain=True)

        # Publish operating state
        self.client.publish(f"{self.base_topic}/state", state, retain=True)

    def stop(self):
        if self.client:
            self.client.publish(f"{self.base_topic}/status", "offline", retain=True)
            self.client.loop_stop()
            self.client.disconnect()


# =============================================================================
# CSV Logger
# =============================================================================

class CSVLogger:
    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.file_handle = None
        self.writer = None
        self.current_date = None
        self.last_flush = 0
        self._open_file()

    def _open_file(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.current_date:
            if self.file_handle:
                self.file_handle.flush()
                self.file_handle.close()
            filepath = self.log_dir / f"modbus_{today}.csv"
            is_new = not filepath.exists()
            self.file_handle = open(filepath, 'a', newline='')
            self.writer = csv.writer(self.file_handle)
            if is_new:
                self.writer.writerow([
                    "timestamp", "iso_time", "direction", "slave_addr", "function_hex",
                    "function_name", "start_reg", "reg_count", "registers_json",
                    "coils_json", "valid_crc", "raw_hex", "operating_state",
                ])
            self.current_date = today

    def log_frame(self, frame: ModbusFrame, operating_state: str, paired_start: int = None, paired_count: int = None):
        self._open_file()

        direction = "REQUEST" if frame.is_request else "RESPONSE"
        fc_name = FUNCTION_CODES.get(frame.function_code, f"0x{frame.function_code:02X}")

        # For responses to read requests, map register offsets to absolute addresses
        abs_regs = {}
        if frame.registers:
            if frame.is_request is False and paired_start is not None:
                for offset, val in frame.registers.items():
                    abs_regs[paired_start + offset] = {
                        "raw": val,
                        "val": to_signed(val) if (paired_start + offset) in SIGNED_REGISTERS else val,
                    }
            else:
                for reg, val in frame.registers.items():
                    abs_regs[reg] = {
                        "raw": val,
                        "val": to_signed(val) if reg in SIGNED_REGISTERS else val,
                    }

        start = frame.start_register if frame.start_register is not None else (paired_start or "")
        count = frame.register_count if frame.register_count is not None else (paired_count or "")

        self.writer.writerow([
            frame.timestamp,
            frame.dt.isoformat(),
            direction,
            frame.address,
            f"0x{frame.function_code:02X}",
            fc_name,
            start,
            count,
            json.dumps(abs_regs) if abs_regs else "",
            json.dumps({str(k): v for k, v in frame.coils.items()}) if frame.coils else "",
            frame.valid_crc,
            frame.raw.hex(),
            operating_state,
        ])

        now = time.time()
        if now - self.last_flush > 30:
            self.file_handle.flush()
            self.last_flush = now

    def close(self):
        if self.file_handle:
            self.file_handle.flush()
            self.file_handle.close()


# =============================================================================
# Main Sniffer
# =============================================================================

class ModbusSniffer:
    def __init__(self, config: dict):
        self.config = config
        self.running = False
        self.socket = None
        self.tracker = RegisterTracker(config["log_dir"])
        self.state_detector = OperatingStateDetector()
        self.mqtt_pub = MQTTPublisher(config)
        self.csv_logger = CSVLogger(config["log_dir"])
        self.buffer = bytearray()
        self.last_byte_time = 0
        self.frame_timeout = config["frame_timeout_ms"] / 1000.0
        self.pending_request = None
        self.consecutive_failures = 0
        self.stats = {
            "frames_total": 0, "frames_valid": 0, "frames_invalid": 0,
            "requests": 0, "responses": 0, "discoveries": 0,
            "state_transitions": 0, "start_time": time.time(),
            "reconnects": 0,
        }
        self.last_mqtt_publish = 0
        self.last_map_save = 0
        logging.info(f"Sniffer init — gateway {config['gateway_host']}:{config['gateway_port']}")

    def connect(self):
        """Connect to gateway with exponential backoff."""
        delay = self.config["reconnect_delay"]
        max_delay = self.config.get("reconnect_max_delay", 60)

        while self.running:
            try:
                if self.socket:
                    try:
                        self.socket.close()
                    except Exception:
                        pass
                    self.socket = None

                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.settimeout(self.config.get("socket_timeout", 2.0))
                self.socket.connect((self.config["gateway_host"], self.config["gateway_port"]))
                self.buffer.clear()
                self.pending_request = None
                self.consecutive_failures = 0
                self.stats["reconnects"] += 1
                logging.info(f"✅ Connected to gateway {self.config['gateway_host']}:{self.config['gateway_port']}")
                return True
            except Exception as e:
                logging.error(f"Connection failed: {e} — retrying in {delay}s")
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
        return False

    def run(self):
        self.running = True
        logging.info("=" * 60)
        logging.info("QSH MODBUS SNIFFER v3 — HA ADD-ON")
        logging.info(f"  Gateway: {self.config['gateway_host']}:{self.config['gateway_port']}")
        logging.info(f"  Slave: {self.config['slave_address']}")
        logging.info(f"  Log dir: {self.config['log_dir']}")
        logging.info(f"  MQTT: {'enabled' if HAS_MQTT else 'disabled'}")
        logging.info(f"  Signed regs: {sorted(SIGNED_REGISTERS)}")
        logging.info(f"  Named regs: {len(REGISTER_NAMES)}")
        logging.info("=" * 60)

        if not self.connect():
            return

        while self.running:
            try:
                data = self.socket.recv(1024)
                if not data:
                    logging.warning("Connection closed by gateway — reconnecting")
                    if not self.connect():
                        break
                    continue

                now = time.time()
                logging.debug(f"recv {len(data)} bytes: {data.hex()}")
                self._process_bytes(data, now)

                # Periodic tasks
                if now - self.last_mqtt_publish > self.config["publish_interval"]:
                    self._publish_batch()
                    self.last_mqtt_publish = now

                if now - self.last_map_save > 60:
                    self.tracker.save_map()
                    self.last_map_save = now
                    self._log_stats()

            except socket.timeout:
                if self.buffer:
                    self._try_parse_frame(time.time())
                continue
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as e:
                logging.warning(f"Connection error: {e} — reconnecting")
                if not self.connect():
                    break
            except Exception as e:
                logging.error(f"Error in main loop: {e}", exc_info=True)
                self.consecutive_failures += 1
                if self.consecutive_failures > 10:
                    logging.error("Too many consecutive failures — reconnecting")
                    if not self.connect():
                        break
                time.sleep(0.1)

    def _process_bytes(self, data: bytes, now: float):
        for byte in data:
            if self.buffer and (now - self.last_byte_time) > self.frame_timeout:
                self._try_parse_frame(now)
            self.buffer.append(byte)
            self.last_byte_time = now

    def _try_parse_frame(self, now: float):
        if len(self.buffer) < 4:
            self.buffer.clear()
            return

        raw = bytes(self.buffer)
        self.buffer.clear()
        self.stats["frames_total"] += 1

        frame = ModbusFrame(raw, now)
        if not frame.valid_crc:
            self.stats["frames_invalid"] += 1
            return

        self.stats["frames_valid"] += 1

        if frame.address != self.config["slave_address"]:
            return

        # State detection
        state = self.state_detector.update(frame)
        if self.state_detector.transitions > self.stats["state_transitions"]:
            self.stats["state_transitions"] = self.state_detector.transitions

        # Pair request/response
        if frame.is_request:
            self.stats["requests"] += 1

            # Write requests contain register data directly
            if frame.function_code == 0x10 and frame.registers:
                self.stats["discoveries"] += self.tracker.update_registers(frame.registers, is_write=True)
                # Log write
                if frame.start_register in (0, 91):
                    regs_str = ", ".join(f"reg_{k}={v}" for k, v in sorted(frame.registers.items()))
                    logging.info(f"📝 HUB WRITE: {regs_str}")

            self.csv_logger.log_frame(frame, state)
            self.pending_request = frame

        elif frame.is_request is False:
            self.stats["responses"] += 1

            # Read responses: map offsets to absolute addresses using pending request
            if self.pending_request and frame.function_code in (0x03, 0x04) and frame.registers:
                start = self.pending_request.start_register
                count = self.pending_request.register_count
                abs_regs = {}
                for offset, val in frame.registers.items():
                    abs_regs[start + offset] = val
                self.stats["discoveries"] += self.tracker.update_registers(abs_regs)

                # Resolve coils if applicable
                if hasattr(frame, '_coil_bytes') and self.pending_request:
                    coil_start = self.pending_request.start_register
                    coil_count = self.pending_request.register_count
                    coils = {}
                    for i in range(coil_count):
                        byte_idx = i // 8
                        bit_idx = i % 8
                        if byte_idx < len(frame._coil_bytes):
                            coils[coil_start + i] = bool(frame._coil_bytes[byte_idx] & (1 << bit_idx))
                    if coils:
                        frame.coils = coils
                        self.stats["discoveries"] += self.tracker.update_coils(coils)

                self.csv_logger.log_frame(frame, state, paired_start=start, paired_count=count)
            else:
                self.csv_logger.log_frame(frame, state)

            self.pending_request = None

    def _publish_batch(self):
        self.mqtt_pub.publish_registers(
            self.tracker.current_values,
            self.tracker.current_coils,
            self.state_detector.current_state
        )

    def _log_stats(self):
        elapsed = time.time() - self.stats["start_time"]
        hours = elapsed / 3600
        logging.info(
            f"📊 {self.stats['frames_valid']}/{self.stats['frames_total']} frames, "
            f"{self.stats['requests']} req, {self.stats['responses']} rsp, "
            f"{self.stats['discoveries']} disc, {self.stats['state_transitions']} trans, "
            f"{len(self.tracker.seen_registers)} regs, {len(self.tracker.seen_coils)} coils, "
            f"{self.stats['reconnects']} reconnects, uptime {hours:.1f}h"
        )

    def stop(self):
        logging.info("Shutting down sniffer...")
        self.running = False
        self.tracker.save_map()
        self.csv_logger.close()
        self.mqtt_pub.stop()
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
        logging.info("Sniffer stopped.")


# =============================================================================
# Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="QSH Modbus Passive Sniffer")
    parser.add_argument("--gateway", default=None, help="Waveshare gateway IP")
    parser.add_argument("--port", type=int, default=None, help="Gateway TCP port")
    parser.add_argument("--mqtt-host", default=None, help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=None, help="MQTT broker port")
    parser.add_argument("--mqtt-user", default=None, help="MQTT username")
    parser.add_argument("--mqtt-pass", default=None, help="MQTT password")
    parser.add_argument("--log-dir", default=None, help="Log directory")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    # Build config
    config = DEFAULT_CONFIG.copy()

    # Environment variable overrides (for add-on options)
    env_map = {
        "GATEWAY_HOST": ("gateway_host", str),
        "GATEWAY_PORT": ("gateway_port", int),
        "MQTT_HOST": ("mqtt_host", str),
        "MQTT_PORT": ("mqtt_port", int),
        "MQTT_USER": ("mqtt_user", str),
        "MQTT_PASS": ("mqtt_pass", str),
        "LOG_DIR": ("log_dir", str),
        "PUBLISH_INTERVAL": ("publish_interval", int),
    }
    for env_key, (config_key, converter) in env_map.items():
        val = os.environ.get(env_key)
        if val is not None:
            try:
                config[config_key] = converter(val)
            except ValueError:
                pass

    # CLI overrides (highest priority)
    if args.gateway:
        config["gateway_host"] = args.gateway
    if args.port:
        config["gateway_port"] = args.port
    if args.mqtt_host:
        config["mqtt_host"] = args.mqtt_host
    if args.mqtt_port:
        config["mqtt_port"] = args.mqtt_port
    if args.mqtt_user:
        config["mqtt_user"] = args.mqtt_user
    if args.mqtt_pass:
        config["mqtt_pass"] = args.mqtt_pass
    if args.log_dir:
        config["log_dir"] = args.log_dir

    # Setup logging
    log_level = logging.DEBUG if args.debug or os.environ.get("DEBUG") == "true" else logging.INFO
    Path(config["log_dir"]).mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.handlers.RotatingFileHandler(
                Path(config["log_dir"]) / "sniffer.log",
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                mode='a',
            ),
        ]
    )

    sniffer = ModbusSniffer(config)

    def shutdown(signum, frame):
        logging.info(f"Signal {signum} received — shutting down")
        sniffer.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    sniffer.run()


if __name__ == "__main__":
    # Need this import for RotatingFileHandler
    import logging.handlers
    main()
