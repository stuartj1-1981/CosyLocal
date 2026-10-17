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
import re
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
    "mqtt_pass": "",
    "mqtt_base_topic": "Cosy HP",
    "slave_address": 10,
    "log_dir": "/config",
    "csv_max_total_mb": 500,    # upper limit for ALL modbus_*.csv files together
    "frame_timeout_ms": 50,
    "reconnect_delay": 5,
    "reconnect_max_delay": 60,
    "publish_interval": 5,
    "socket_timeout": 2.0,
    "recv_timeout": 30,
    "app_version": "unknown",
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

# Registers that can carry negative values (temperatures, offsets, valve steps).
# Names updated 4.12.0 (2026-10-17 capture). Membership unchanged except reg 58.
SIGNED_REGISTERS = {
    19,                                  # Unknown 19 (was compressor frequency)
    24,                                  # Unknown 24 (was suction pressure)
    29, 30,                              # Compressor area temps 1, 2
    32,                                  # Main EEV position (homing reads −586)
    36, 37, 38, 39,                      # OAT, unknown temp 37, suction line, unknown temp 39
    40, 41, 43, 44, 45,                  # Return, sump, liquid line, flow, discharge gas
    50, 53, 54, 55,                      # Suction pressure, comp. demand, unknown temps 54/55
    56, 57, 58,                          # Tc, Te, defrost Te threshold (raw 65416 = −12.0 °C)
    64,                                  # Heat Output (negative during reverse-cycle defrost)
    91,                                  # Target flow temp
}

# All known register addresses from the polling cycle
ALL_REGISTERS = set(range(19, 53)) | set(range(53, 81)) | {210} | set(range(91, 98)) | {0, 1}

# =============================================================================
# Register Names (from AP mode labels + Modbus sniffing correlation)
# scale: multiply raw (signed) value by this to get real value
#
# Confidence levels:
#   CONFIRMED   = cross-validated against an independent sensor or a physical
#                 calculation (e.g. R290 saturation)
#   STATISTICAL = strong internal evidence (correlation, ratio, timing) on one
#                 installation; needs a second installation to promote
#   NAMED       = identified from AP mode / installer page labels, data fits
#   UNCONFIRMED = behaviour observed but identity not proven. Refuted names are
#                 replaced by "Unknown N" / "Unknown Temp N"
#
# Evidence dated 2026-10-17 comes from the cold-weather capture (slave 10,
# 12,126 poll cycles, 00:00-08:25 UTC, OAT 2.0-4.6 °C, two defrosts) and the
# findings document cosy6-cold-weather-defrost-register-findings-2026-10-17.md.
# Pressures (regs 48, 50) are GAUGE: absolute = value + 1.013 bar.
#
# Optional key "state_class": sets the HA state_class for a numeric register
# that has no HA device class (see _send_discovery).
# =============================================================================
REGISTER_NAMES = {
    # --- Compressor ---
    # STATISTICAL (2026-10-17): compressor speed, ×0.01. Unit open (rps or Hz).
    # log(power) fit with both pressures: R²=0.97 (reg 19: R²=0.61). Fixed 89.9
    # in defrost step 7.2; holds 59.9 in steps 1.2, 6.2, 8.1, 8.2. Max 104.8
    # (DHW). Falls to 0 in standby. Use reg 63 > 0 as "compressor running".
    # Previously "Energy Counter" (Wh) — refuted: not a counter.
    63: {"name": "Compressor Speed",      "scale": 0.01, "unit": "",      "icon": "mdi:speedometer",          "class": None, "state_class": "measurement", "display_precision": 1},
    # STATISTICAL (2026-10-17): compressor demand, ×0.1, range 0-60.0.
    # 0 in standby; 59.9 when flow is below target; 15-42 near target; frozen
    # during defrost; r=0.96 with reg 63.
    # Previously "DHW Tank Temp" (°C) — refuted. Not a temperature.
    53: {"name": "Compressor Demand",     "scale": 0.1,  "unit": "",      "icon": "mdi:gauge",                "class": None, "state_class": "measurement", "display_precision": 1},
    # UNCONFIRMED (2026-10-17). Previously "Compressor Frequency" (CONFIRMED) —
    # refuted: reads 0 for several minutes of each defrost while reg 63 = 89.9
    # and input power is 1.0-2.7 kW (0 in all 145 step-7.2 cycles). Figure of
    # defrost 1 suggests the zero window is the window where reg 64 < 0 (flow
    # temp below return), not step 7.2 — not yet tested. Probably a non-negative
    # water-side quantity. Published raw (no scale, no unit) until identified.
    # Test: in states 6-8, does (reg 19 == 0) agree with (reg 64 < 0)?
    19: {"name": "Unknown 19",            "scale": 1,    "unit": "",      "icon": "mdi:help-circle",          "class": None},

    # --- Refrigerant pressures (GAUGE) and saturation temperatures ---
    # CONFIRMED (2026-10-17, R290 saturation): discharge (high-side) pressure,
    # GAUGE, bar ×0.01. reg 56 = Tsat(reg 48 + 1.013 bar) to −0.08 K median over
    # 12,126 cycles (p5-p95 −0.17 to −0.01 K). Absolute assumption: +7.4 K.
    # R1270 / R32 / R410A excluded. Firmware may use 1.00 bar for atmosphere
    # (residual −0.01 K) — not resolved at the 0.1 K register resolution.
    # Observed 3.6-23.7 bar g (two defrosts at OAT 2.3-2.4 °C).
    # Earlier comment used absolute values (13 bar → 35.1 °C) — wrong.
    # Gauge, CoolProp Propane: 13.0 bar g → 41.0 °C; 5.0 bar g → 8.0 °C.
    # History: reinstated 2026-04-11 from the 4.4.0 demotion.
    48: {"name": "Discharge Pressure",    "scale": 0.01, "unit": "bar",   "icon": "mdi:gauge-full",           "class": "pressure", "display_precision": 2},
    # CONFIRMED (2026-10-17, R290 saturation): suction (low-side) pressure,
    # GAUGE, bar ×0.01. reg 57 = Tsat(reg 50 + 1.013 bar) to −0.08 K median
    # (p5-p95 −0.17 to +0.03 K). Standby equalised: reg 48 3.83, reg 50
    # 3.89 bar g. Defrost: 0.04 bar g at the 4-way reversal; 14.0 bar g at the
    # reversal back.
    # Previously "Reported COP" — refuted: 2.24 when reg 64 / reg 20 = 3.49;
    # 5.64 in reverse cycle while heat output is −8 kW.
    50: {"name": "Suction Pressure",      "scale": 0.01, "unit": "bar",   "icon": "mdi:gauge-low",            "class": "pressure", "display_precision": 2},
    # CONFIRMED (2026-10-17): saturated condensing temperature (Tc), calculated
    # by the unit from reg 48 (gauge + atmosphere). −0.08 K median, all cycles.
    # Previously "Discharge Temp" — refuted. Discharge gas temp is reg 45.
    56: {"name": "Saturated Condensing Temp", "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-high",     "class": "temperature"},
    # CONFIRMED (2026-10-17): saturated evaporating temperature (Te), calculated
    # by the unit from reg 50 (gauge + atmosphere). −0.08 K median, all cycles.
    # Signed ×0.1 °C. Frost signal: both 2026-10-17 defrosts started 10 s after
    # reg 57 reached −12.0 °C (reg 58). Swings −41 to +44 °C at the reversals.
    # Previously "Defrost Accumulator" (raw, no unit) — refuted.
    57: {"name": "Saturated Evaporating Temp", "scale": 0.1,  "unit": "°C",    "icon": "mdi:snowflake-thermometer", "class": "temperature"},
    # UNCONFIRMED — hypothesis (2026-10-17): demand-defrost Te threshold, signed
    # ×0.1 °C. Constant raw 65416 (signed −120 → −12.0 °C) for the full capture.
    # Both defrosts started 10 s after reg 57 ≤ reg 58. n=2 at one OAT
    # (2.3-2.4 °C): a fixed Te threshold and an approach criterion
    # (OAT − Te ≈ 14.3 K) cannot yet be told apart.
    # Test: capture defrosts at OAT < 0 °C and at 4-6 °C.
    58: {"name": "Defrost Te Threshold (hypothesis)", "scale": 0.1,  "unit": "°C",    "icon": "mdi:snowflake-alert",      "class": "temperature"},
    # UNCONFIRMED (2026-10-17): constant 6,080 in heating and in both defrosts
    # while reg 50 moves 0.04-14.0 bar g. Steps to 4,800-5,170 at start, in
    # standby and at low load. Not a pressure. Published raw until identified.
    # Previously "Suction Pressure" (kPa ×0.1) — refuted. Suction is reg 50.
    24: {"name": "Unknown 24",            "scale": 1,    "unit": "",      "icon": "mdi:help-circle",          "class": None},

    # --- Refrigerant-side temperatures (raw × 0.1 = °C) ---
    # CONFIRMED (2026-10-17): discharge gas temperature (AP label "T10
    # Discharge"). 93.0 °C at DHW stop 00:31:39 UTC; 62.4 °C 8.5 min later;
    # 27.3 °C after 4.5 h standby. Rises 27 → 48 °C in 4 min after start.
    # Discharge superheat (reg 45 − reg 56) median 21.7 K, IQR 18.3-23.8 K.
    # A cylinder cannot do this. Closes the 4.10.0 DISPUTED item: regs 45 and
    # 56 renamed together. Previously "DHW Cylinder Temp". No register is
    # identified as cylinder temperature.
    45: {"name": "Discharge Gas Temp",    "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-high",     "class": "temperature"},
    # CONFIRMED: suction line temperature (AP label "T3 Suction"). 2026-10-17:
    # suction superheat (reg 38 − reg 57) held at 6.8 K, IQR 6.3-7.3 K.
    38: {"name": "Suction Line Temp",     "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED (AP label "T8"): liquid line temperature. 3.1 °C in standby at
    # OAT 2.4 °C (outdoor side). Defrost role (2026-10-17): in reverse cycle it
    # reads the outdoor-coil outlet — about 5 °C while ice melts, then a steep
    # rise.
    # Step 7.2 ended at reg 43 = 56.2 / 51.7 °C. Termination candidate:
    # reg 43 ≥ ≈50 °C (two events; value not fixed).
    43: {"name": "Liquid Line Temp",      "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED (2026-10-17): compressor sump temperature (AP label "T6 Sump").
    # +26.6 °C when Te = −11.9 °C; 40.6-41.0 °C for 1 h after DHW.
    # Previously "Evaporator Inlet Temp" — refuted (an evaporator inlet reads
    # near Te). The refrigerant is R290, not R32.
    41: {"name": "Sump Temp",             "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # UNCONFIRMED (2026-10-17): slow temperature in the compressor area.
    # 48.9 °C (reg 29) / 43.9 °C (reg 30) after 4.5 h standby at OAT 2.4 °C;
    # no response to defrost; lags reg 45. Candidates: compressor body, or
    # inverter module (reg 35 "V4 Inverter" opens when reg 29 ≥ ≈72 °C).
    # Test: fit reg 29 to compressor power with a first-order lag; compare
    # with the fit to reg 45.
    # Previously "Condenser Inlet Temp" — refuted. identified.md "29 = HP LWT,
    # r=0.999" also refuted: 58 °C at 31 °C water flow.
    29: {"name": "Compressor Area Temp 1", "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-chevron-up", "class": "temperature"},
    # UNCONFIRMED (2026-10-17): see reg 29. Tracks reg 29, about 5 K lower.
    # Previously "Condenser Outlet Temp" — refuted.
    30: {"name": "Compressor Area Temp 2", "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-chevron-down", "class": "temperature"},
    # UNCONFIRMED (2026-10-17): +6.6 °C in heating when Te = −11.9 °C and OAT
    # 2.3 °C; 20.5 °C in standby; spikes at reversal; floors at 0 (19 cycles).
    # Previously "Evaporator Temp" (CONFIRMED, P0 frost signal) — refuted.
    # DO NOT use as a frost signal. Use reg 57 (Te).
    55: {"name": "Unknown Temp 55",       "scale": 0.1,  "unit": "°C",    "icon": "mdi:help-circle",          "class": "temperature"},

    # --- Air and enclosure temperatures (raw × 0.1 = °C) ---
    # CONFIRMED: r=1.000 vs Octopus API outdoor_temperature (n=402, max diff
    # 0.8 °C). 2026-10-17: reads ≈2 K high for ≈5 min after each defrost (the
    # fan stops and the sensor sees defrost heat). Filter post-defrost samples.
    36: {"name": "OAT External",          "scale": 0.1,  "unit": "°C",    "icon": "mdi:home-thermometer",     "class": "temperature"},
    # UNCONFIRMED (2026-10-17): 36-43 °C during DHW while room sensors read
    # 18.5-21.5 °C. Slow water-side or enclosure temperature.
    # Previously "Indoor Ambient" (CONFIRMED) — refuted.
    37: {"name": "Unknown Temp 37",       "scale": 0.1,  "unit": "°C",    "icon": "mdi:help-circle",          "class": "temperature"},
    # UNCONFIRMED (2026-10-17): 20.6 °C in standby at OAT 2.4 °C; 7.6-22 °C in
    # heating at OAT 2-2.5 °C; rises when the fan stops. Not outdoor air.
    # Previously "Outdoor Coil Air Temp" — refuted.
    39: {"name": "Unknown Temp 39",       "scale": 0.1,  "unit": "°C",    "icon": "mdi:help-circle",          "class": "temperature"},
    # UNCONFIRMED (2026-10-17): 19.5 °C in standby at OAT 2.4 °C; spikes to
    # 44-66 °C at the reversals. Not outdoor ambient. Now published ×0.1 °C
    # (was raw). Previously "Outdoor Ambient Raw" — refuted.
    54: {"name": "Unknown Temp 54",       "scale": 0.1,  "unit": "°C",    "icon": "mdi:help-circle",          "class": "temperature"},

    # --- Water side ---
    # CONFIRMED: water return temperature (AP label "T5"). 2026-10-17: r=0.999
    # vs primary return sensor, +0.18 K median (4,369 heating cycles).
    # Earlier: r=0.974 over 1,700+ samples (41 h), mean offset +0.17 °C.
    40: {"name": "T5 Return Temp",        "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # CONFIRMED: water flow temperature (AP label "T9"). 2026-10-17: r=0.973 vs
    # primary flow sensor, +1.86 K median (heating circuit).
    44: {"name": "T9 Flow Temp",          "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-water",    "class": "temperature"},
    # CONFIRMED: target flow temperature setpoint (hub → outdoor). 2026-10-17:
    # hub write equals Octopus fixed_target_flow_temperature ×10 at all 9
    # heating changes (36.7-42.1 °C). 65.0 °C during DHW.
    91: {"name": "Target Flow Temp",      "scale": 0.1,  "unit": "°C",    "icon": "mdi:target",               "class": "temperature"},
    # NAMED: unit flow sensor, ×0.01 L/min. 19.3 L/min in reverse cycle
    # (2026-10-17). Reg 52 is the target for this value (ratio 1.00).
    47: {"name": "Flow Rate",             "scale": 0.01, "unit": "L/min", "icon": "mdi:water-pump",           "class": "volume_flow_rate", "display_precision": 2},
    # STATISTICAL (2026-10-17): water flow target, ×0.1 L/min.
    # (reg 52 / 10) / (reg 47 / 100) median 1.00, IQR 0.99-1.01 in run.
    # Floor 6.5 L/min. Held at 12.0 in states 6-7 while the pump runs 100 %.
    # Previously "Fan Suction" (Pa, NAMED from installer page label, 4.10.0) —
    # refuted by the ratio above.
    52: {"name": "Water Flow Target",     "scale": 0.1,  "unit": "L/min", "icon": "mdi:water-pump",           "class": "volume_flow_rate", "display_precision": 1},
    # STATISTICAL (2026-10-17): circulation pump speed, %. r=0.976 with reg 47.
    # 100 % in reverse cycle; 33 % in standby pump runs with compressor off.
    # identified.md previously named it "Compressor Speed" — refuted.
    51: {"name": "Circulation Pump Speed", "scale": 1,    "unit": "%",     "icon": "mdi:pump",                 "class": None},
    # CONFIRMED: heat output, W, signed Int16. 2026-10-17: reg 64 =
    # reg 47 × 4.18 × (reg 44 − reg 40): ratio 1.001, r=0.9994 in run; ratio
    # 0.98 in defrost. Minimum −8,057 W. Signed reading confirmed on two
    # installations (13 Apr 2026: AdamLC defrost peaks −8,276 / −8,870 W, raw
    # 57,260 / 56,666; Stu start-up transients −4 to −893 W).
    64: {"name": "Heat Output",           "scale": 1,    "unit": "W",     "icon": "mdi:fire",                 "class": "power"},

    # --- Electrical (W) ---
    # STATISTICAL (2026-10-17): total electrical input, W. vs Shelly EM: median
    # ratio 1.002, r=0.993 (5,521 cycles > 100 W). Standby pump run reads 30 W
    # (Shelly 0-9 W). Use this register as the COP denominator.
    # Previously "Runtime Counter" (s) — refuted.
    20: {"name": "Electrical Power Input", "scale": 1,    "unit": "W",     "icon": "mdi:flash",                "class": "power"},
    # STATISTICAL (2026-10-17): also total input. Median ratio 1.012 vs Shelly
    # EM. The difference from reg 20 is not explained.
    26: {"name": "Electrical Power Total", "scale": 1,    "unit": "W",     "icon": "mdi:flash",                "class": "power"},
    # STATISTICAL (2026-10-17): compressor power — NOT total input. Median ratio
    # 0.933 vs Shelly EM (−106 W). The earlier r=0.999 (n=1206) is shape, not
    # level. A COP from reg 64 / reg 27 reads ≈7 % high: use reg 20.
    27: {"name": "Compressor Power",      "scale": 1,    "unit": "W",     "icon": "mdi:flash",                "class": "power"},
    # UNCONFIRMED (2026-10-17): candidate DC-link voltage, ×0.1 V. Range
    # 369.8-384.8 V; r=−0.84 with input power (sag under load); never zero.
    # Previously "Heat Output (suspect)" — refuted.
    25: {"name": "DC Link Voltage (candidate)", "scale": 0.1,  "unit": "V",     "icon": "mdi:flash-triangle",       "class": "voltage", "display_precision": 1},
    # UNCONFIRMED (2026-10-17): tracks power non-linearly (ratio 0.67 to Shelly
    # EM, r=0.87). Current-like. Test: clamp meter on the compressor lead.
    # Previously "Heat Output 2" — refuted. Published raw until identified.
    28: {"name": "Unknown 28",            "scale": 1,    "unit": "",      "icon": "mdi:help-circle",          "class": None},

    # --- Valves and fan ---
    # STATISTICAL (2026-10-17): main EEV position, steps (raw, signed).
    # Homing at start: −586 → 392 → 624 → 200 (same as reg 35). 0 in reverse
    # cycle; 160-390 in heating. Suction superheat (reg 38 − reg 57) held at
    # 6.8 K. Previously "V1 Heating Valve" (×0.1 %).
    32: {"name": "Main EEV Position",     "scale": 1,    "unit": "steps", "icon": "mdi:valve",                "class": None},
    # STATISTICAL (2026-10-17): valve position used in reverse cycle only.
    # 0 in heating and in standby; ≈96 at the reversal; 61-87 in steps 7.2-8.x;
    # 0 again within 2 min after the defrost ends. Valve identity open (a 4-way
    # reversing valve is on/off). Scale and unit kept (×0.1 %).
    # April 2026 HA history showed 96 / 48 in heating and 6464.x values at
    # defrost transitions — not seen on 2026-10-17.
    # Previously "Reversing Valve". For "defrost active" use reg 65 in
    # (6, 7, 8).
    34: {"name": "Defrost Valve Position", "scale": 0.1,  "unit": "%",     "icon": "mdi:snowflake-melt",       "class": None},
    # UNCONFIRMED: second valve, steps. Homes with reg 32 at start.
    # 2026-10-17: opens 80-114 (raw) when reg 29 is high — open in 0 % of
    # cycles below 66 °C, 47 % at 70-72 °C, 97-100 % at ≥ 72 °C.
    # Candidates: liquid injection for compressor temperature, or refrigerant
    # cooling of the inverter module (fits the AP label "V4 Inverter").
    # Scale and unit kept (×0.1 %) until identified.
    35: {"name": "V4 Inverter",           "scale": 0.1,  "unit": "%",     "icon": "mdi:sine-wave",            "class": None},
    # CONFIRMED: fan speed (AP label; both sniffers agree). 2026-10-17: 0 rpm in
    # defrost steps 7.x-8.1; restarts at ≈700 rpm in step 8.2.
    61: {"name": "Fan Speed",             "scale": 1,    "unit": "RPM",   "icon": "mdi:fan",                  "class": None},
    # STATISTICAL (2026-10-17): fan speed command, %. r=0.982 with reg 61 in
    # run; 0 when the fan stops. Previously "EEV Opening" — refuted (the main
    # EEV is reg 32).
    62: {"name": "Fan Speed Command",     "scale": 1,    "unit": "%",     "icon": "mdi:fan",                  "class": None},

    # --- Controller state ---
    # CONFIRMED: HP controller operating state.
    # Start:   10 → 3 → 1 → 2      (standby → starting → init → running)
    # Stop:    2 → 4 → 10          (running → stopping → standby)
    # Defrost: 2 → 6 → 7 → 8 → 2   (seen twice on 2026-10-17)
    #   1 = Initialising, 2 = Running, 3 = Starting, 4 = Stopping,
    #   6 = Pre-defrost, 7 = Defrost (reverse cycle), 8 = Recovery,
    #   10 = Standby
    # Sub-step index is reg 66; sub-step timer is reg 67.
    # Defrost sub-steps (2026-10-17): 6.1 compressor to 59.9; 6.2 hold 20 s;
    # 7.1 4-way reversal, fan 0, pump 100 % (60 s); 7.2 reverse cycle at 89.9
    # (ended at reg 43 ≈ 52-56 °C); 8.0 compressor to 59.9; 8.1 hold 20 s;
    # 8.2 reversal back, fan restarts (20 s); 8.3 ramp.
    # READ register (FC 0x03) — independent of the hub command (reg 92).
    65: {"name": "HP Controller State",   "scale": 1,    "unit": "",      "icon": "mdi:state-machine",        "class": None},
    # STATISTICAL (2026-10-17): sub-step index within the reg 65 state.
    # Defrost: 6.1, 6.2, 7.1, 7.2, 8.0, 8.1, 8.2, 8.3. Changes coincide with
    # plant actions (compressor speed steps, fan stop, 4-way reversal); reg 67
    # resets at each change. The identified.md reading "Off/Heat/DHW/Defrost"
    # is refuted. Held below CONFIRMED: one installation, and the step
    # boundaries were defined from this register.
    66: {"name": "HP State Sub-step",     "scale": 1,    "unit": "",      "icon": "mdi:numeric",              "class": None},
    # STATISTICAL (2026-10-17): sub-step timer, s. Counts 1 s/s from 0 in each
    # sub-step (slope 0.99-1.01, offset ≤ 1.8 s, 19 timed blocks). Reads 0 in
    # state 2 and in defrost step 7.2. Counts in standby (16,410 s seen).
    # Explains the AdamLC capture (0 in heating). OPEN: Stu April 2026 data
    # showed 1,648 counts over 1,650 s before a defrost — check whether reg 65
    # was 2 in that window. Previously "Runtime Counter (provisional)".
    # Unsigned — not in SIGNED_REGISTERS.
    67: {"name": "Sub-step Timer",        "scale": 1,    "unit": "s",     "icon": "mdi:timer-outline",        "class": "duration"},
    # STATISTICAL (2026-10-17): standby pump-run flag. 2 in 937 of 961 standby
    # pump-on cycles; 0 in 5,594 of 5,615 pump-off cycles. In standby at
    # OAT ≈ 2 °C the pump runs 5 min every 30 min.
    # Previously "Config Param 77".
    77: {"name": "Standby Pump Run Flag", "scale": 1,    "unit": "",      "icon": "mdi:pump",                 "class": None},
    # UNRESOLVED (2026-04-11): binary flag, all 0 over 10 days (43 samples).
    # Same slow poll group as reg 210 and reg 77. 2026-10-17: 0 all capture.
    75: {"name": "Unknown 75",            "scale": 1,    "unit": "",      "icon": "mdi:help-circle",          "class": None},

    # --- Configuration ---
    # CONFIG: constant 5500 W in all captures (2026-10-17 also). Matches the
    # Cosy 6 rated heating capacity (5.5 kW).
    59: {"name": "Rated Heat Capacity",   "scale": 1,    "unit": "W",     "icon": "mdi:information",          "class": None},
    # UNCONFIRMED (2026-10-17): not constant. Steps 1100/1200/1300/1400,
    # inversely with Te (r=−0.89). Not condensing pressure.
    # Previously "Rated Elec Input" (constant 1300 W) — refuted.
    60: {"name": "Unknown 60",            "scale": 1,    "unit": "",      "icon": "mdi:help-circle",          "class": None},

    # --- Hub control registers ---
    # CONFIRMED: hub demand state (FC 0x10 write, hub → outdoor).
    #   1 = idle (no demand)       — CONFIRMED
    #   2 = space-heating demand   — CONFIRMED 2026-10-17: 1 → 2 at 05:07:24
    #                                UTC; reg 65 10 → 3 three seconds later;
    #                                heating-circuit primary flow rises
    #   4 = hot water active       — CONFIRMED 2026-06-19 (Octopus Kraken
    #                                WATER-zone cross-check; history-6.csv)
    # During 4 the heating circuit stays cold (primary flow median 24.4 °C vs
    # unit flow 55.1 °C). reg 91 ramps to 65 °C during DHW.
    # The derived hot_water_active binary_sensor stays reg 92 == 4 (never != 1).
    92: {"name": "DHW Demand State",      "scale": 1,    "unit": "",      "icon": "mdi:thermostat",           "class": None},
    # Status register (slow poll group). 2026-10-17: 0 all capture.
    210:{"name": "Status Register",       "scale": 1,    "unit": "",      "icon": "mdi:information",          "class": None},
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

        r63 = registers.get(63, 0)               # Compressor speed (×0.01); 0 = stopped
        r19 = registers.get(19, 0)               # Unknown 19 — diagnostic only
        r57 = to_signed(registers.get(57, 0))    # Te, saturated evaporating temp (×0.1 °C)
        r65 = registers.get(65, 0)               # HP controller state
        r66 = registers.get(66, 0)               # HP state sub-step
        r92 = registers.get(92, 0)               # Hub demand state
        running = r63 > 0

        # Primary detection: reg 65 HP controller state.
        # "Compressor running" = reg 63 > 0 (4.12.0). Reg 19 is not used: it
        # reads 0 during defrost while the compressor runs at fixed speed.
        if r65 == 10 and not running:
            new_state = "OFF"
        elif r65 == 3:
            new_state = "STARTING"
        elif r65 == 1:
            new_state = "INITIALISING"
        elif r65 == 4:
            new_state = "STOPPING"
        # Defrost: reg 65 only. The old fallback (reg 29 − reg 30 < 0) is
        # removed — regs 29/30 are not condenser temperatures (4.12.0).
        elif r65 in (6, 7, 8):
            new_state = "DEFROST"
        # Mode-specific active states
        elif r92 == 4 and running:
            new_state = "DHW"
        elif r92 == 2 and running:
            new_state = "HEATING"
        elif r92 == 2 and not running:
            new_state = "HEATING_IDLE"
        elif r92 == 1 and not running:
            new_state = "IDLE"
        # OIL_RECOVERY (reg 19 < 15) removed in 4.12.0 — it had no evidence
        # and reg 19 is not compressor speed.
        elif running:
            new_state = "ACTIVE_UNKNOWN"
        else:
            new_state = "UNKNOWN"

        if new_state != prev_state:
            duration = timestamp - self.state_entered_at
            self.state_history.append({
                "from": prev_state, "to": new_state,
                "timestamp": timestamp, "duration_s": round(duration, 1),
                "trigger_registers": {
                    "reg_19": r19, "reg_57": r57, "reg_63": r63,
                    "reg_65": r65, "reg_66": r66, "reg_92": r92,
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
            "qsh_modbus/status",
            payload="offline", qos=1, retain=True
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
            self.client.publish("qsh_modbus/status", "online", retain=True)
            # Re-send discovery on reconnect
            self.discovery_sent.clear()
            # Publish gateway status binary_sensor discovery
            self._send_gateway_status_discovery()
            # Publish operating state discovery
            self._send_discovery_custom("operating_state", "Modbus Operating State", "", "mdi:state-machine", None)
        else:
            logging.error(f"MQTT connect failed: rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            logging.warning(f"MQTT disconnected unexpectedly: rc={rc}")

    def _send_gateway_status_discovery(self):
        """Send MQTT discovery for the gateway connectivity binary_sensor."""
        if not self.client:
            return
        payload = {
            "name": "Modbus Gateway Status",
            "unique_id": "qsh_modbus_gateway_status",
            "state_topic": "qsh_modbus/status",
            "payload_on": "online",
            "payload_off": "offline",
            "device_class": "connectivity",
            "entity_category": "diagnostic",
            "device": {
                "identifiers": ["qsh_modbus_sniffer"],
                "name": "QSH Modbus Sniffer",
                "manufacturer": "QSH",
                "model": "Cosy 6 Passive Sniffer",
                "sw_version": self.config["app_version"],
            },
        }
        self.client.publish(
            "homeassistant/binary_sensor/qsh_modbus_gateway_status/config",
            json.dumps(payload), retain=True
        )

    def publish_gateway_offline(self):
        """Publish offline status when Waveshare TCP connection is lost."""
        if self.connected and self.client:
            self.client.publish("qsh_modbus/status", "offline", retain=True)
            logging.info("Published gateway status: offline")

    def _send_discovery_custom(self, sensor_id, name, unit, icon, device_class):
        """Send MQTT discovery for a custom (non-register) sensor."""
        if not self.client or sensor_id in self.discovery_sent:
            return
        payload = {
            "name": name,
            "state_topic": f"{self.base_topic}/{sensor_id}",
            "unique_id": f"qsh_modbus_{sensor_id}",
            "expire_after": 60,
            "device": {
                "identifiers": ["qsh_modbus_sniffer"],
                "name": "QSH Modbus Sniffer",
                "manufacturer": "QSH",
                "model": "Cosy 6 Passive Sniffer",
                "sw_version": self.config["app_version"],
            },
            "availability": {
                "topic": "qsh_modbus/status",
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
        state_class = info.get("state_class")
        display_precision = info.get("display_precision")

        uid = f"qsh_modbus_reg_{reg_num}"
        state_topic = f"{self.base_topic}/reg_{reg_num}/state"

        config_payload = {
            "name": f"Cosy {name}",
            "unique_id": uid,
            "state_topic": state_topic,
            "expire_after": 60,
            "device": {
                "identifiers": ["qsh_modbus_sniffer"],
                "name": "QSH Modbus Sniffer",
                "manufacturer": "QSH",
                "model": "Cosy 6 Passive Sniffer",
                "sw_version": self.config["app_version"],
            },
            "availability": {
                "topic": "qsh_modbus/status",
            },
        }

        if unit:
            config_payload["unit_of_measurement"] = unit
        if icon:
            config_payload["icon"] = icon
        if device_class:
            config_payload["device_class"] = device_class
            config_payload["state_class"] = "measurement"
        elif state_class:
            # Numeric register with no HA device class (e.g. reg 63 speed,
            # reg 53 demand). state_class gives a line graph and statistics.
            config_payload["state_class"] = state_class
        if display_precision is not None:
            config_payload["suggested_display_precision"] = display_precision

        config_topic = f"homeassistant/sensor/qsh_modbus/reg_{reg_num}/config"
        self.client.publish(config_topic, json.dumps(config_payload), retain=True)
        self.discovery_sent.add(reg_num)
        logging.debug(f"Discovery sent for reg_{reg_num}: {name}")

    def _send_hw_active_discovery(self):
        """Send MQTT discovery for the derived hot_water_active binary_sensor.

        ON iff reg 92 == 4 (hot water active). Published in ADDITION to the raw
        reg 92 sensor, never replacing it. Same availability topic + expire_after
        as the register sensors so a sniffer/MQTT dropout marks it `unavailable`
        rather than collapsing to a stale or false OFF.
        """
        key = "reg_92_hw_active"
        if not self.client or key in self.discovery_sent:
            return
        payload = {
            "name": "Cosy Hot Water Active",
            "unique_id": "qsh_modbus_reg_92_hw_active",
            "state_topic": f"{self.base_topic}/reg_92_hw_active/state",
            "payload_on": "ON",
            "payload_off": "OFF",
            "device_class": "heat",
            "expire_after": 60,
            "device": {
                "identifiers": ["qsh_modbus_sniffer"],
                "name": "QSH Modbus Sniffer",
                "manufacturer": "QSH",
                "model": "Cosy 6 Passive Sniffer",
                "sw_version": self.config["app_version"],
            },
            "availability": {
                "topic": "qsh_modbus/status",
            },
        }
        self.client.publish(
            "homeassistant/binary_sensor/qsh_modbus/reg_92_hw_active/config",
            json.dumps(payload), retain=True
        )
        self.discovery_sent.add(key)
        logging.debug("Discovery sent for reg_92_hw_active (hot_water_active)")

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

            # Derived hot_water_active boolean, published alongside (not
            # instead of) the raw reg 92 sensor. Only emitted when reg 92 is
            # present; absence is left to expire_after / availability rather
            # than publishing a stale OFF.
            # Explicit `== 4`: value 2 (non-idle, non-HW) must read OFF, not ON.
            # Never use `!= 1`.
            if reg_num == 92:
                self._send_hw_active_discovery()
                hw_active = "ON" if raw_val == 4 else "OFF"
                self.client.publish(
                    f"{self.base_topic}/reg_92_hw_active/state", hw_active, retain=True
                )

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
            self.client.publish("qsh_modbus/status", "offline", retain=True)
            self.client.loop_stop()
            self.client.disconnect()


# =============================================================================
# CSV Logger — circular (ring) log
# =============================================================================
#
# Frames are written to segment files in log_dir:
#
#     modbus_YYYY-MM-DD.csv        first segment of a UTC day
#     modbus_YYYY-MM-DD_001.csv    next segment, started when a segment is full
#     modbus_YYYY-MM-DD_002.csv    ... and so on
#
# A new segment starts at UTC midnight, or when the active segment reaches
# 1/CSV_SEGMENTS_PER_RING of the size limit. When the files together would
# exceed the size limit, the OLDEST segments are deleted first. The active
# segment is never deleted. Total CSV size therefore has a fixed upper limit
# (csv_max_total_mb), plus at most about 30 s of new data (the flush interval).
#
# Only files that match the segment name pattern are counted or deleted.
# Other files in log_dir (sniffer.log, register_map.json, files you add by
# hand) are never touched.
#
# PASSIVE ONLY: this class writes to local files. It must never write to the
# Modbus socket.

CSV_SEGMENT_RE = re.compile(r"^modbus_(\d{4}-\d{2}-\d{2})(?:_(\d{3,}))?\.csv$")
CSV_SEGMENTS_PER_RING = 10
CSV_MIN_TOTAL_MB = 10
CSV_FLUSH_INTERVAL_S = 30
CSV_HEADER = [
    "timestamp", "iso_time", "direction", "slave_addr", "function_hex",
    "function_name", "start_reg", "reg_count", "registers_json",
    "coils_json", "valid_crc", "raw_hex", "operating_state",
]


class CSVLogger:
    def __init__(self, log_dir: str, max_total_mb: int = 500):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.max_total_bytes = max(CSV_MIN_TOTAL_MB, int(max_total_mb)) * 1024 * 1024
        self.max_segment_bytes = self.max_total_bytes // CSV_SEGMENTS_PER_RING
        self.file_handle = None
        self.writer = None
        self.current_path = None
        self.current_date = None
        self.last_flush = 0
        self._open_file()

    def _segment_path(self, date_str: str, index: int) -> Path:
        if index == 0:
            return self.log_dir / f"modbus_{date_str}.csv"
        return self.log_dir / f"modbus_{date_str}_{index:03d}.csv"

    def _segments(self):
        """Return (date, index, path) for every ring segment in log_dir, oldest first."""
        found = []
        for p in self.log_dir.iterdir():
            m = CSV_SEGMENT_RE.match(p.name)
            if m and p.is_file():
                found.append((m.group(1), int(m.group(2) or 0), p))
        found.sort(key=lambda t: (t[0], t[1]))
        return found

    def _close_file(self):
        if self.file_handle:
            self.file_handle.flush()
            self.file_handle.close()
        self.file_handle = None
        self.writer = None

    def _open_file(self, new_segment: bool = False):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today == self.current_date and self.file_handle is not None and not new_segment:
            return
        self._close_file()

        # Newest segment of today. After a restart, resume it if it has room.
        todays = [(i, p) for d, i, p in self._segments() if d == today]
        if todays:
            index, path = todays[-1]
            if new_segment or path.stat().st_size >= self.max_segment_bytes:
                index += 1
                path = self._segment_path(today, index)
        else:
            path = self._segment_path(today, 0)

        is_new = not path.exists() or path.stat().st_size == 0
        self.file_handle = open(path, 'a', newline='')
        self.writer = csv.writer(self.file_handle)
        if is_new:
            self.writer.writerow(CSV_HEADER)
        self.current_path = path
        self.current_date = today
        self._prune()

    def _prune(self):
        """Delete the oldest segments until all segments fit in the size limit."""
        segments = self._segments()
        sizes = {}
        for _, _, p in segments:
            try:
                sizes[p] = p.stat().st_size
            except OSError:
                sizes[p] = 0

        # Keep room for the active segment to grow to its full size.
        limit = self.max_total_bytes - self.max_segment_bytes
        others = sum(s for p, s in sizes.items() if p != self.current_path)

        for _, _, p in segments:  # oldest first
            if others <= limit:
                break
            if p == self.current_path:
                continue
            try:
                p.unlink()
            except OSError as e:
                logging.warning(f"CSV ring: cannot remove {p.name}: {e}")
                continue
            others -= sizes[p]
            logging.info(
                f"CSV ring: removed {p.name} ({sizes[p] / (1024 * 1024):.1f} MB) "
                f"- limit {self.max_total_bytes // (1024 * 1024)} MB"
            )

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
        if now - self.last_flush > CSV_FLUSH_INTERVAL_S:
            self.file_handle.flush()
            self.last_flush = now
            # Size check at flush time: no extra cost per frame.
            if os.fstat(self.file_handle.fileno()).st_size >= self.max_segment_bytes:
                self._open_file(new_segment=True)

    def close(self):
        self._close_file()


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
        self.csv_logger = CSVLogger(config["log_dir"], config["csv_max_total_mb"])
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
                try:
                    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                except AttributeError:
                    logging.warning("TCP keepalive tuning not available on this platform")

                # Recv timeout: hub sends every ~2.5s, 30s silence = dead connection
                self.socket.settimeout(self.config.get("recv_timeout", 30))
                self.buffer.clear()
                self.pending_request = None
                self.consecutive_failures = 0
                self.stats["reconnects"] += 1
                logging.info(f"Connected to gateway {self.config['gateway_host']}:{self.config['gateway_port']}")
                # Restore gateway online status after successful TCP reconnect
                if self.mqtt_pub.connected and self.mqtt_pub.client:
                    self.mqtt_pub.client.publish("qsh_modbus/status", "online", retain=True)
                    logging.info("Published gateway status: online")
                return True
            except Exception as e:
                logging.error(f"Connection failed: {e} — retrying in {delay}s")
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
        return False

    def run(self):
        self.running = True
        logging.info("=" * 60)
        logging.info(f"QSH MODBUS SNIFFER v{self.config['app_version']} — HA ADD-ON")
        logging.info(f"  Gateway: {self.config['gateway_host']}:{self.config['gateway_port']}")
        logging.info(f"  Slave: {self.config['slave_address']}")
        logging.info(f"  Log dir: {self.config['log_dir']}")
        logging.info(f"  CSV ring limit: {self.csv_logger.max_total_bytes // (1024 * 1024)} MB")
        logging.info(f"  MQTT: {'enabled' if HAS_MQTT else 'disabled'}")
        logging.info(f"  Signed regs: {sorted(SIGNED_REGISTERS)}")
        logging.info(f"  Named regs: {len(REGISTER_NAMES)}")
        logging.info("=" * 60)

        if not self.connect():
            return

        last_recv_time = time.time()
        last_watchdog_log = time.time()
        watchdog_frame_count = 0

        while self.running:
            try:
                data = self.socket.recv(1024)
                if not data:
                    logging.warning("Connection closed by gateway — reconnecting")
                    self.mqtt_pub.publish_gateway_offline()
                    if not self.connect():
                        break
                    last_recv_time = time.time()
                    continue

                now = time.time()
                last_recv_time = now
                watchdog_frame_count += 1
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

                # Watchdog: confirm recv loop is alive every 5 minutes
                if now - last_watchdog_log >= 300:
                    logging.info(f"Watchdog: sniffer active, {watchdog_frame_count} frames received in last 5 min")
                    last_watchdog_log = now
                    watchdog_frame_count = 0

            except socket.timeout:
                # Hub sends every ~2.5s; 30s silence means connection is dead
                if self.buffer:
                    self._try_parse_frame(time.time())
                logging.warning("Socket timeout — no data for 30s, reconnecting")
                self.mqtt_pub.publish_gateway_offline()
                if not self.connect():
                    break
                last_recv_time = time.time()
                continue
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as e:
                logging.warning(f"Connection error: {e} — reconnecting")
                self.mqtt_pub.publish_gateway_offline()
                if not self.connect():
                    break
            except Exception as e:
                logging.error(f"Error in main loop: {e}", exc_info=True)
                self.consecutive_failures += 1
                if self.consecutive_failures > 10:
                    logging.error("Too many consecutive failures — reconnecting")
                    self.mqtt_pub.publish_gateway_offline()
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
        "CSV_MAX_TOTAL_MB": ("csv_max_total_mb", int),
        "PUBLISH_INTERVAL": ("publish_interval", int),
        "APP_VERSION": ("app_version", str),
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
