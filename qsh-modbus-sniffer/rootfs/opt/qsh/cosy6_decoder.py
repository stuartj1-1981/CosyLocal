#!/usr/bin/env python3
"""
QSH Modbus Passive Sniffer v4 — Home Assistant Add-on
======================================================

Connects to Waveshare RS485-to-WiFi gateway in transparent mode.
Passively captures ALL Modbus RTU traffic between Cosy Hub (master)
and outdoor unit (slave address 10).

Features:
    - Scanning frame parser: extracts multiple frames from concatenated TCP chunks
    - Robust socket reconnection with exponential backoff
    - MQTT auto-discovery for Home Assistant (with LWT availability)
    - CSV logging of all parsed frames
    - JSON register map with min/max/sample tracking
    - Register-based operating state detection (HEATING/DEFROST/DHW/OFF/...)
    - Signed int16 handling for temperature-range registers
    - Request/response pairing via pair_response()

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
    "recv_timeout": 30,
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
    # STATISTICAL: Tracks outdoor ambient +5°C offset (r=0.953 vs reg_39) — enclosure heat from inverter/compressor. AP label: "T3 Suction"
    38: {"name": "Internal Unit Temp",   "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # STATISTICAL: Range −5.5–17.4°C consistent with UK ambient. AP label: "Evaporator Temp"
    39: {"name": "Outdoor Ambient Temp", "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-low",      "class": "temperature"},
    # STATISTICAL: r=0.950 vs flow temp; mean offset 3.72°C matches system ΔT. AP label: "T5 Return Temp"
    40: {"name": "Return Water Temp",    "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED: AP mode label. Compressor sump temperature
    41: {"name": "T6 Sump",              "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED: AP mode label. Liquid line temperature
    43: {"name": "T8 Liquid",            "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED: AP mode label. Flow temperature (secondary sensor?)
    44: {"name": "T9 Flow Temp",         "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-water",    "class": "temperature"},
    # STATISTICAL: Only register >60°C (max 84.1°C); r=0.922 vs flow temp; mean 7.6°C above condensing temp. AP label: "T10 Discharge"
    45: {"name": "Discharge Gas Temp",   "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-high",     "class": "temperature"},
    # CONFIRMED: target flow temperature setpoint (hub → outdoor)
    91: {"name": "Target Flow Temp",     "scale": 0.1,  "unit": "°C",    "icon": "mdi:target",               "class": "temperature"},

    # --- Pressures (raw × 0.01 = bar) ---
    48: {"name": "Discharge Pressure",   "scale": 0.01, "unit": "bar",   "icon": "mdi:gauge-full",           "class": None},
    # STATISTICAL: Range 2.75–5.97 (mean 4.23) consistent with COP; negative correlation with flow temp. Previously "Suction Pressure"
    50: {"name": "Reported COP",         "scale": 0.01, "unit": "",      "icon": "mdi:gauge",                "class": None},

    # --- Compressor ---
    # STATISTICAL: Exact 0–100 range
    51: {"name": "Compressor Speed",     "scale": 1,    "unit": "%",     "icon": "mdi:speedometer",          "class": None},
    # STATISTICAL: Raw 0–600 = 0–60 Hz; r=0.708 vs compressor speed %
    53: {"name": "Compressor Frequency", "scale": 0.1,  "unit": "Hz",    "icon": "mdi:sine-wave",            "class": "frequency"},

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

    # --- Operating mode ---
    # STATISTICAL: Discrete values 0/1/2/3 — likely Off / Heating / DHW / Defrost (requires operational confirmation)
    66: {"name": "Operating Mode",       "scale": 1,    "unit": "",      "icon": "mdi:state-machine",        "class": None},

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
                reg_data = data[1:1 + data[0]]
                self._response_words = [
                    struct.unpack('>H', reg_data[i:i+2])[0]
                    for i in range(0, len(reg_data) - 1, 2)
                ]

        elif fc == 0x05:
            # Write Single Coil
            if len(data) == 4:
                self.is_request = True
                self.start_register = struct.unpack('>H', data[0:2])[0]
                self.coils[self.start_register] = (struct.unpack('>H', data[2:4])[0] == 0xFF00)

        elif fc == 0x06:
            # Write Single Register
            if len(data) == 4:
                self.is_request = True
                self.start_register = struct.unpack('>H', data[0:2])[0]
                self.registers[self.start_register] = struct.unpack('>H', data[2:4])[0]

        elif fc == 0x0F:
            # Write Multiple Coils
            if len(data) >= 5:
                self.is_request = True
                self.start_register = struct.unpack('>H', data[0:2])[0]
                coil_count = struct.unpack('>H', data[2:4])[0]
                coil_data = data[5:5 + data[4]]
                for i in range(coil_count):
                    byte_idx, bit_idx = i // 8, i % 8
                    if byte_idx < len(coil_data):
                        self.coils[self.start_register + i] = bool(coil_data[byte_idx] & (1 << bit_idx))

        elif fc == 0x10:
            # Write Multiple Registers
            if len(data) >= 5:
                # REQUEST: start(2) + count(2) + bytes(1) + data
                self.is_request = True
                self.start_register = struct.unpack('>H', data[0:2])[0]
                self.register_count = struct.unpack('>H', data[2:4])[0]
                for i in range(self.register_count):
                    offset = 5 + i * 2
                    if offset + 1 < len(data):
                        self.registers[self.start_register + i] = struct.unpack('>H', data[offset:offset+2])[0]
            elif len(data) == 4:
                # RESPONSE: start(2) + count(2) — echo
                self.is_request = False
                self.start_register = struct.unpack('>H', data[0:2])[0]
                self.register_count = struct.unpack('>H', data[2:4])[0]

    def pair_response(self, request):
        """Map response data to absolute register addresses using the paired request."""
        if not request.is_request or request.function_code != self.function_code:
            return
        if self.function_code in (0x03, 0x04) and hasattr(self, '_response_words'):
            for i, value in enumerate(self._response_words):
                self.registers[request.start_register + i] = value
            self.start_register = request.start_register
            self.register_count = request.register_count
        elif self.function_code in (0x01, 0x02) and hasattr(self, '_coil_bytes'):
            for i in range(request.register_count):
                byte_idx, bit_idx = i // 8, i % 8
                if byte_idx < len(self._coil_bytes):
                    self.coils[request.start_register + i] = bool(self._coil_bytes[byte_idx] & (1 << bit_idx))
            self.start_register = request.start_register
            self.register_count = request.register_count


# =============================================================================
# Register Tracker
# =============================================================================

class RegisterTracker:
    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.map_file = self.log_dir / "register_map.json"
        self.seen_registers = {}
        self.seen_coils = {}
        self.seen_function_codes = {}
        self.current_values = {}
        self.current_coils = {}
        self.write_registers = {}
        self.lock = Lock()
        self._load_map()

    def _load_map(self):
        if self.map_file.exists():
            try:
                with open(self.map_file) as f:
                    saved = json.load(f)
                self.seen_registers = saved.get("registers", {})
                self.seen_coils = saved.get("coils", {})
                self.seen_function_codes = saved.get("function_codes", {})
                self.write_registers = saved.get("write_registers", {})
                logging.info(f"Loaded map: {len(self.seen_registers)} regs, {len(self.seen_coils)} coils")
            except Exception as e:
                logging.warning(f"Failed to load register map: {e}")

    def save_map(self):
        with self.lock:
            data = {
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "registers": self.seen_registers,
                "coils": self.seen_coils,
                "function_codes": self.seen_function_codes,
                "write_registers": self.write_registers,
            }
        try:
            with open(self.map_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save register map: {e}")

    def update_from_frame(self, frame: ModbusFrame) -> list:
        """Update tracker from a parsed frame. Returns list of discovery strings."""
        discoveries = []
        now_iso = frame.dt.isoformat()

        with self.lock:
            # Track function codes
            fc_key = str(frame.function_code)
            if fc_key not in self.seen_function_codes:
                fc_name = FUNCTION_CODES.get(frame.function_code, "UNKNOWN")
                self.seen_function_codes[fc_key] = {"name": fc_name, "first_seen": now_iso, "count": 0}
                discoveries.append(f"NEW FC: 0x{frame.function_code:02X} ({fc_name})")
            self.seen_function_codes[fc_key]["count"] = self.seen_function_codes[fc_key].get("count", 0) + 1

            # Track registers
            is_write = frame.function_code in (0x06, 0x10)
            for reg, value in frame.registers.items():
                reg_key = str(reg)
                self.current_values[reg] = value
                signed_val = to_signed(value) if reg in SIGNED_REGISTERS else value

                if reg_key not in self.seen_registers:
                    self.seen_registers[reg_key] = {
                        "first_seen": now_iso, "last_seen": now_iso,
                        "min_raw": value, "max_raw": value,
                        "min_signed": signed_val, "max_signed": signed_val,
                        "sample_count": 0, "is_written": is_write, "fc_seen": [],
                    }
                    discoveries.append(f"NEW REG: {reg} = {value} (signed: {signed_val})")

                e = self.seen_registers[reg_key]
                e["last_seen"] = now_iso
                e["min_raw"] = min(e.get("min_raw", value), value)
                e["max_raw"] = max(e.get("max_raw", value), value)
                e["min_signed"] = min(e.get("min_signed", signed_val), signed_val)
                e["max_signed"] = max(e.get("max_signed", signed_val), signed_val)
                e["sample_count"] = e.get("sample_count", 0) + 1
                e["latest_raw"] = value
                e["latest_signed"] = signed_val
                fc_str = f"0x{frame.function_code:02X}"
                if fc_str not in e.get("fc_seen", []):
                    e.setdefault("fc_seen", []).append(fc_str)
                if is_write:
                    self.write_registers[reg_key] = {"last_written": now_iso, "last_value": value}
                    e["is_written"] = True

            # Track coils
            for coil, value in frame.coils.items():
                coil_key = str(coil)
                self.current_coils[coil] = value
                if coil_key not in self.seen_coils:
                    self.seen_coils[coil_key] = {"first_seen": now_iso, "sample_count": 0, "values_seen": []}
                    discoveries.append(f"NEW COIL: {coil} = {value}")
                e = self.seen_coils[coil_key]
                e["last_seen"] = now_iso
                e["sample_count"] = e.get("sample_count", 0) + 1
                e["latest_value"] = value
                if value not in e.get("values_seen", []):
                    e["values_seen"].append(value)

        return discoveries


# =============================================================================
# Operating State Detector
# =============================================================================

class OperatingStateDetector:
    """Detect heat pump operating state from register values."""

    def __init__(self):
        self.current_state = "UNKNOWN"
        self.state_history = []
        self.state_entered_at = time.time()
        self.transitions = 0

    def update(self, registers: dict, timestamp: float) -> str:
        """Update state from current register values. Returns new state name on transition, else None."""
        prev_state = self.current_state

        r19 = registers.get(19, 0)
        r25 = registers.get(25, 0)
        r29 = to_signed(registers.get(29, 0))
        r30 = to_signed(registers.get(30, 0))
        r57 = to_signed(registers.get(57, 0))
        r92 = registers.get(92, 0)
        delta = r29 - r30

        if r19 == 0 and r25 == 0:
            new_state = "OFF"
        elif delta < 0 and r19 > 0:
            new_state = "DEFROST"
        elif r92 == 4 and r19 > 0:
            new_state = "DHW"
        elif r92 == 2 and r19 > 0:
            new_state = "HEATING"
        elif r92 == 2 and r19 == 0:
            new_state = "HEATING_IDLE"
        elif r19 > 0 and r19 < 15:
            new_state = "OIL_RECOVERY"
        elif r19 > 0:
            new_state = "ACTIVE_UNKNOWN"
        else:
            new_state = "UNKNOWN"

        if new_state != prev_state:
            duration = timestamp - self.state_entered_at
            self.state_history.append({
                "from": prev_state, "to": new_state,
                "timestamp": timestamp, "duration_s": round(duration, 1),
                "trigger_registers": {
                    "reg_19": r19, "reg_25": r25, "reg_29": r29,
                    "reg_30": r30, "delta": round(delta, 1),
                    "reg_57": r57, "reg_92": r92,
                }
            })
            self.state_entered_at = timestamp
            self.current_state = new_state
            self.transitions += 1
            return new_state

        self.current_state = new_state
        return None


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
            # Publish operating state discovery
            self._send_discovery_custom("operating_state", "Modbus Operating State", "", "mdi:state-machine", None)
        else:
            logging.error(f"MQTT connect failed: rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logging.warning(f"MQTT disconnected unexpectedly: rc={rc}")

    def _send_discovery_custom(self, sensor_id, name, unit, icon, device_class):
        """Send MQTT discovery for a custom (non-register) sensor."""
        if not self.client or sensor_id in self.discovery_sent:
            return
        payload = {
            "name": name,
            "state_topic": f"{self.base_topic}/{sensor_id}",
            "unique_id": f"qsh_modbus_{sensor_id}",
            "device": {
                "identifiers": ["qsh_modbus_sniffer"],
                "name": "QSH Modbus Sniffer",
                "manufacturer": "QSH",
                "model": "Cosy 6 Passive Sniffer",
                "sw_version": "4.0.0",
            },
            "availability": {
                "topic": f"{self.base_topic}/status",
            },
        }
        if unit:
            payload["unit_of_measurement"] = unit
        if icon:
            payload["icon"] = icon
        if device_class:
            payload["device_class"] = device_class
            payload["state_class"] = "measurement"
        self.client.publish(
            f"homeassistant/sensor/qsh_modbus/{sensor_id}/config",
            json.dumps(payload), retain=True
        )
        self.discovery_sent.add(sensor_id)

    def _send_discovery(self, reg_num: int):
        """Send MQTT discovery for a register sensor."""
        if reg_num in self.discovery_sent or not self.connected:
            return

        info = REGISTER_NAMES.get(reg_num, {})
        name = info.get("name", f"Modbus Reg {reg_num}")
        unit = info.get("unit", "")
        icon = info.get("icon", "mdi:numeric" if not info else None)
        device_class = info.get("class")

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
                "sw_version": "4.0.0",
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

        # Publish coils
        for coil, value in coils.items():
            sensor_id = f"coil_{coil}"
            if sensor_id not in self.discovery_sent:
                self._send_discovery_custom(sensor_id, f"Modbus Coil {coil}", "", "mdi:toggle-switch", None)
            self.client.publish(f"{self.base_topic}/coil_{coil}", "ON" if value else "OFF", retain=True)

        # Publish operating state
        if state:
            self.client.publish(f"{self.base_topic}/operating_state", state, retain=True)

    def publish_state_transition(self, transition: dict):
        if self.connected and self.client:
            self.client.publish(f"{self.base_topic}/state_transition", json.dumps(transition))

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

    def log_frame(self, frame: ModbusFrame, operating_state: str):
        self._open_file()

        direction = "REQUEST" if frame.is_request else "RESPONSE"
        fc_name = FUNCTION_CODES.get(frame.function_code, f"0x{frame.function_code:02X}")

        reg_data = {}
        for reg, raw_val in frame.registers.items():
            signed_val = to_signed(raw_val) if reg in SIGNED_REGISTERS else raw_val
            reg_data[str(reg)] = {"raw": raw_val, "val": signed_val}

        self.writer.writerow([
            f"{frame.timestamp:.3f}",
            frame.dt.isoformat(),
            direction,
            frame.address,
            f"0x{frame.function_code:02X}" if frame.function_code else "",
            fc_name,
            frame.start_register,
            frame.register_count,
            json.dumps(reg_data) if reg_data else "",
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

                # TCP keepalive to detect half-open connections (~60s worst case)
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

                # Recv timeout: hub sends every ~2.5s, 30s silence = dead connection
                self.socket.settimeout(self.config.get("recv_timeout", 30))
                self.buffer.clear()
                self.pending_request = None
                self.consecutive_failures = 0
                self.stats["reconnects"] += 1
                logging.info(f"Connected to gateway {self.config['gateway_host']}:{self.config['gateway_port']}")
                return True
            except Exception as e:
                logging.error(f"Connection failed: {e} — retrying in {delay}s")
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
        return False

    def run(self):
        self.running = True
        logging.info("=" * 60)
        logging.info("QSH MODBUS SNIFFER v4 — HA ADD-ON")
        logging.info(f"  Gateway: {self.config['gateway_host']}:{self.config['gateway_port']}")
        logging.info(f"  Slave: {self.config['slave_address']}")
        logging.info(f"  Log dir: {self.config['log_dir']}")
        logging.info(f"  MQTT: {'enabled' if HAS_MQTT else 'disabled'}")
        logging.info(f"  Signed regs: {sorted(SIGNED_REGISTERS)}")
        logging.info(f"  Named regs: {len(REGISTER_NAMES)}")
        logging.info("=" * 60)

        if not self.connect():
            return

        last_recv_time = time.time()
        last_watchdog_log = time.time()

        while self.running:
            try:
                data = self.socket.recv(1024)
                if not data:
                    logging.warning("Connection closed by gateway — reconnecting")
                    if not self.connect():
                        break
                    last_recv_time = time.time()
                    continue

                now = time.time()
                last_recv_time = now
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

                # Watchdog: confirm recv loop is alive
                if now - last_watchdog_log >= 60:
                    ago = now - last_recv_time
                    logging.info(f"Recv loop alive — last frame {ago:.0f}s ago")
                    last_watchdog_log = now

            except socket.timeout:
                # Hub sends every ~2.5s; 30s silence means connection is dead
                if self.buffer:
                    self._try_parse_frame(time.time())
                ago = time.time() - last_recv_time
                logging.warning(f"Recv timeout ({ago:.0f}s no data) — reconnecting")
                if not self.connect():
                    break
                last_recv_time = time.time()
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
        # Eagerly scan for complete frames in the buffer (handles TCP concatenation)
        if len(self.buffer) >= 5:
            self._try_parse_frame(now)

    def _try_parse_frame(self, now: float):
        """Scanning parser: extract valid Modbus frames from buffer by CRC probing."""
        if len(self.buffer) < 4:
            self.buffer.clear()
            return

        raw = bytes(self.buffer)
        self.buffer.clear()

        offset = 0
        while offset < len(raw) - 3:
            # Look for a byte that could be a valid slave address
            if raw[offset] not in (self.config["slave_address"], 0x00):
                offset += 1
                continue
            frame_parsed = False
            # Try increasing frame lengths until we find a valid CRC
            for end in range(offset + 4, min(offset + 260, len(raw) + 1)):
                candidate = raw[offset:end]
                if verify_crc(candidate):
                    frame = ModbusFrame(candidate, now)
                    self._handle_frame(frame)
                    offset = end
                    frame_parsed = True
                    break
            if not frame_parsed:
                offset += 1

    def _handle_frame(self, frame: ModbusFrame):
        """Process a single validated Modbus frame."""
        self.stats["frames_total"] += 1
        if not frame.valid_crc:
            self.stats["frames_invalid"] += 1
            return
        self.stats["frames_valid"] += 1

        if frame.is_request:
            self.stats["requests"] += 1
            self.pending_request = frame
        elif frame.is_request is False and self.pending_request:
            self.stats["responses"] += 1
            frame.pair_response(self.pending_request)
            self.pending_request = None

        # Update tracker and log discoveries
        discoveries = self.tracker.update_from_frame(frame)
        for d in discoveries:
            self.stats["discoveries"] += 1
            logging.warning(f"\U0001f50d {d}")

        # State detection from register values (after response pairing)
        state_change = None
        if frame.registers and not frame.is_request:
            state_change = self.state_detector.update(self.tracker.current_values, frame.timestamp)
            if state_change:
                self.stats["state_transitions"] = self.state_detector.transitions
                t = self.state_detector.state_history[-1]
                logging.info(f"\u26a1 {t['from']} \u2192 {t['to']} (was {t['duration_s']:.0f}s)")
                self.mqtt_pub.publish_state_transition(t)

        # CSV log
        self.csv_logger.log_frame(frame, self.state_detector.current_state)

        # Log hub writes
        if frame.function_code in (0x06, 0x10) and frame.registers:
            parts = []
            for reg, val in sorted(frame.registers.items()):
                v = to_signed(val) if reg in SIGNED_REGISTERS else val
                parts.append(f"reg_{reg}={v}")
            logging.info(f"\U0001f4dd HUB WRITE: {', '.join(parts)}")

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
            f"\U0001f4ca {self.stats['frames_valid']}/{self.stats['frames_total']} frames, "
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
