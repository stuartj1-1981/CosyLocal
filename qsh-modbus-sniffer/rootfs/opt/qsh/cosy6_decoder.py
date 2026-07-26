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
    "mqtt_pass": "",
    "mqtt_base_topic": "Cosy HP",
    "slave_address": 10,
    "log_dir": "/config",
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

# Registers that can carry negative values (temperatures, offsets, timers).
SIGNED_REGISTERS = {
    19,                                  # Compressor frequency
    24,                                  # Suction pressure
    29, 30,                              # Condenser inlet, condenser outlet
    32,                                  # V1 heating valve
    36, 37, 38, 39,                      # OAT, indoor ambient, suction line, outdoor coil
    40, 41, 43, 44, 45,                  # Condenser outlet, evap inlet, liquid, condenser mid, shell
    50, 53, 54, 55, 56, 57,              # Reported COP, DHW, OU ambient, evaporator, discharge, defrost accum
    64,                                  # Heat Output (negative during reverse-cycle defrost)
    91,                                  # Target flow temp
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
    # --- Compressor ---
    # CONFIRMED (defrost validated): 32-34 Hz steady, stops during defrost, ramps 59-64 Hz at recovery.
    # Scale ×0.1 confirmed by physics: raw 323-338 → 32.3-33.8 Hz (scroll compressor 20-80 Hz range).
    # Second sniffer matched from HP installer page display "Compressor Speed".
    # Second sniffer value 324 Hz during active SH — consistent with Stuart's data.
    19: {"name": "Compressor Frequency",  "scale": 0.1,  "unit": "Hz",    "icon": "mdi:sine-wave",            "class": "frequency", "display_precision": 1},

    # --- Temperatures (raw × 0.1 = °C) ---
    # NAMED: Originally labelled "flow_temp" by second sniffer but DISPROVEN
    # by 36-hour time-series: range 50.9-74.0°C, always 20-40°C above actual
    # water flow (reg 44 / primary sensor). Tracks reg 30 at r=0.999 with
    # stable 5.3°C ΔT — condenser heat exchanger refrigerant side.
    # Reg 29 = hot gas inlet (superheated discharge entering condenser).
    29: {"name": "Condenser Inlet Temp", "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-chevron-up",   "class": "temperature"},
    # NAMED: Originally labelled "return_temp" by second sniffer but DISPROVEN
    # by 36-hour time-series: range 46.3-67.7°C, always 12-37°C above actual
    # water return (reg 40 / primary sensor). Tracks reg 29 at r=0.999.
    # Reg 30 = subcooled liquid outlet (leaving condenser toward EEV).
    30: {"name": "Condenser Outlet Temp", "scale": 0.1, "unit": "°C",    "icon": "mdi:thermometer-chevron-down", "class": "temperature"},
    # CONFIRMED: r=1.000 vs Octopus API outdoor_temperature (n=402, max diff 0.8°C).
    # Second sniffer mislabelled as "fixed_param_60" but their own data shows
    # variation (24-25 raw = 2.4-2.5°C) confirming it is NOT fixed. Stuart's
    # mapping validated.
    36: {"name": "OAT External",          "scale": 0.1,  "unit": "°C",    "icon": "mdi:home-thermometer",     "class": "temperature"},
    # CONFIRMED: Both sniffers agree "indoor_ambient". Second sniffer value
    # 25.8°C. AP mode label matched. Range 21-37°C consistent with indoor sensor.
    37: {"name": "Indoor Ambient",        "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # CONFIRMED: Both sniffers agree. Stuart's AP mode label "T3 Suction",
    # second sniffer "refrigerant_temp" / "Suction Line Temp" from HP installer
    # page. Cold-side, correlates with evaporator (r=0.81).
    38: {"name": "Suction Line Temp",     "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # CONFIRMED (defrost validated): -15 to +4°C range. Swings during defrost.
    # Previously labelled "Evaporator Temp" but R55 is the actual evap coil sensor.
    # Tracks below outdoor — likely outdoor coil air temp or calculated evaporating temp.
    39: {"name": "Outdoor Coil Air Temp", "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-low",      "class": "temperature"},
    # CONFIRMED: Water circuit return temperature. r=0.974 vs independent
    # return pipe sensor over 1,700+ samples (41 hours). Mean offset +0.17°C.
    # Previously misnamed "Condenser Outlet Temp" — regs 29/30 are the actual
    # condenser (refrigerant) sensors. This is the water side.
    40: {"name": "T5 Return Temp",        "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # CONFIRMED: Stuart "T6 Sump", second sniffer "Evaporator Inlet" from HP
    # installer page. Value 4.2°C plausible for R32 evaporator inlet in
    # heating mode.
    41: {"name": "Evaporator Inlet Temp", "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # NAMED (AP mode T8): Liquid line temp, 2-43°C range. Dynamic during defrost.
    43: {"name": "Liquid Line Temp",      "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer",          "class": "temperature"},
    # CONFIRMED: Water circuit flow temperature. 38.1-39.6°C during active
    # heating in raw Modbus frames, consistently ~5°C above reg 40 (return).
    # Previously misnamed "Condenser Mid Temp" — regs 29/30 are the actual
    # condenser (refrigerant) sensors. This is the water side.
    44: {"name": "T9 Flow Temp",          "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-water",    "class": "temperature"},
    # CONFIRMED: Compressor discharge gas temp. Matches HP installer page.
    # Previously misnamed "DHW Cylinder Temp" - not available via Modbus
    # Previously misnamed "Compressor Shell Temp" — reg 56 is the actual
    # discharge temp (40.8°C), confirming this is NOT a compressor sensor.
    45: {"name": "Discharge Gas Temp",     "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-high",     "class": "temperature"},
    # CONFIRMED (defrost validated, R290 cross-check): Primary frost signal.
    # Steady state 5.8-7.8°C. Drops to 0°C at defrost trigger.
    # Spikes to 37-55°C during hot gas defrost, decays as ice melts.
    # R290 sat at R24 suction pressure (608 kPa) = 4.7°C → superheat 2.8°C. Closed.
    # Second sniffer labels "evaporator_temp" at 7.5°C. Distinct from
    # reg 39 (evaporator/outdoor coil) and reg 41 (T6 sump/evap inlet).
    # Could be evaporator mid-point or outlet.
    55: {"name": "Evaporator Temp",       "scale": 0.1,  "unit": "°C",    "icon": "mdi:snowflake-thermometer", "class": "temperature"},
    # CONFIRMED (defrost validated): 38-41°C steady, drops to -3°C, spikes to 51°C during defrost.
    # Second sniffer labels "discharge_temp" at 40.8°C. This SUPPORTS
    # Stuart's reg 45 = DHW cylinder interpretation — if reg 56 is the actual
    # discharge temp, then reg 45 at 54.4°C cannot also be discharge.
    56: {"name": "Discharge Temp",        "scale": 0.1,  "unit": "°C",    "icon": "mdi:thermometer-high",     "class": "temperature"},
    # CONFIRMED: target flow temperature setpoint (hub → outdoor)
    91: {"name": "Target Flow Temp",      "scale": 0.1,  "unit": "°C",    "icon": "mdi:target",               "class": "temperature"},

    # --- Pressures ---
    # CONFIRMED (defrost validated, R290 cross-check):
    # Steady 608 kPa (×0.1), drops to 480 kPa at defrost. R290 sat at 480 kPa = 0°C matches R55.
    # Second sniffer matched from HP installer page "Suction Pressure".
    # Second sniffer value 6080 kPa (≈60.8 bar). Plausible for R32 suction side.
    24: {"name": "Suction Pressure",      "scale": 0.1,  "unit": "kPa",   "icon": "mdi:gauge",                "class": "pressure"},
    # CONFIRMED (defrost analysis 2026-04-11, reinstated from 4.4.0 demotion):
    # Discharge (high-side) pressure. Second sniffer matched "Discharge Pressure"
    # from HP installer page. Two defrost events confirm:
    #   Normal heating (OAT 6–11°C, Stu Apr 2026): ~13 bar (R290 sat ≈ 35°C,
    #     consistent with 30°C flow)
    #   Cold-OAT heating peak (OAT ~2.6°C, AdamLC 13 Apr 2026): 20.83 bar
    #     (R290 sat ≈ 56°C, consistent with elevated flow demand at cold OAT)
    #   Defrost entry (compressor briefly off): drops to ~5 bar (Stu Apr)
    #     / 3.85 bar (AdamLC at OAT 2.6°C, R290 sat ≈ −2°C, equalized evap)
    #   Defrost active (compressor reversed): climbs to ~16 bar (R290 sat
    #     ≈ 47°C) as reverse-cycle high-side pressurises the outdoor coil
    #     to melt ice (AdamLC ~4 min after entry)
    #   Full observed envelope across two installations: 3.85–20.83 bar
    #   Post-defrost recovery: 9-12 bar over ~10 min
    # Scale 0.01: raw 450-1350 → 4.5-13.5 bar. R290 saturation cross-check:
    # 13 bar → 35.1°C (matches condenser inlet R29). 5 bar → 4.8°C (matches
    # equalized evaporator R55). Closed.
    48: {"name": "Discharge Pressure",   "scale": 0.01, "unit": "bar",   "icon": "mdi:gauge-full",           "class": "pressure"},
    # CONFIRMED: Cross-validated with HP installer page during DWH cycle.
    # Previously Reported COP but unlikely to be provided via Cosy
    50: {"name": "Suction",          "scale": 0.01, "unit": "bar",      "icon": "mdi:gauge",                "class": "pressure"},
    # CONFIRMED: Cross-validated with HP installer page during DHW cycle
    52: {"name": "Fan Suction",           "scale": 1, "unit": "Pa",    "icon": "mdi:gauge",                "class": "pressure"},
    
    # --- Flow Rate ---
    # NAMED: Second sniffer matched to "Flow Rate" from HP installer page.
    # Value 1714. Units unclear — if ×0.01 = 17.14 l/min.
    # Cross-check against Sika VVX20 flow meter data when available.
    # Stable during defrost (slight rise) — consistent with circulation pump continuing.
    47: {"name": "Flow Rate",             "scale": 0.01, "unit": "L/min", "icon": "mdi:water-pump",           "class": "volume_flow_rate",   "display_precision": 2},

    # --- Valve Positions ---
    # CONFIRMED (defrost validated): Valve position during heating. Scale ×0.1 confirmed
    # (Feb raw 240-258 → HA 24.0-25.8, Mar range 0-115).
    32: {"name": "V1 Heating Valve",      "scale": 0.1,  "unit": "%",     "icon": "mdi:valve",                "class": None},
    # CONFIRMED (defrost validated): Reversing valve / defrost status register.
    # Scale ×0.1 from Feb sniffer (raw 1000 → 100.0, raw 502 → 50.2).
    # NOTE: From HA gateway history, this register shows bit-packed behaviour
    # that the ×0.1 scaling partially obscures:
    #   96.0 = Normal heating (stable, long duration)
    #   48.0 = Standby / transition state (brief)
    #   0.0  = Off / compressor stopped
    #   6464.x / 6494.x / 6525.x = Defrost transition (bit 12 high = valve
    #         solenoid energised: 2^12 = 4096 + lower status bits)
    #   88.x / 92.x = Post-defrost recovery states
    # Multiple actuations per night — not only during confirmed defrost events.
    34: {"name": "Reversing Valve",       "scale": 0.1,  "unit": "%",     "icon": "mdi:snowflake-melt",       "class": None},
    # UNCONFIRMED: 0-103% range. May be inverter duty cycle or valve position.
    # NOTE (2026-04-11): Shows same 6464.x bit pattern as R34 during defrost
    # transitions. Normal operation: 8.0-9.6 (slowly incrementing — possibly
    # inverter board temperature or internal counter at ×0.1 scale).
    # The matching 6464 value shared with R34 confirms a controller-wide
    # defrost-active bitmask read from multiple register addresses.
    35: {"name": "V4 Inverter",           "scale": 0.1,  "unit": "%",     "icon": "mdi:sine-wave",            "class": None},
    # CONFIRMED (defrost validated): EEV position. 54-75% heating, drops to 0% during defrost.
    # Reopens during recovery. Classic EEV behaviour.
    62: {"name": "EEV Opening",           "scale": 1,    "unit": "%",     "icon": "mdi:valve-open",           "class": None},

    # --- Power (W) ---
    # UNCONFIRMED: Total electrical including fans/pumps. R26 > R27 by ~150W consistently.
    # COP against R25: R26 → 2.04, R27 → 2.23. Difference = ancillary load.
    # Second sniffer value 1865W, labelled "electrical_power_1".
    # Stuart confirmed reg 27 vs Shelly EM at r=0.999. Reg 26 is ~150W higher
    # than reg 27 — likely total system power (compressor + circulation pump +
    # controls) vs compressor-only at reg 27. Needs Shelly EM cross-check.
    26: {"name": "Electrical Power Total","scale": 1,    "unit": "W",     "icon": "mdi:flash",                "class": "power"},
    # CONFIRMED: r=0.999 vs Shelly EM (n=1206). Compressor electrical.
    27: {"name": "Compressor Power",      "scale": 1,    "unit": "W",     "icon": "mdi:flash",                "class": "power"},
    # UNCONFIRMED: Second sniffer labels "heat_output" at 3764W. Stuart has
    # heat output CONFIRMED at reg 64 (r=0.999 vs Q=flow×4.18×ΔT). This may
    # be a different calculation method or rated/nominal output. Do NOT promote
    # to CONFIRMED — conflicts with reg 64 evidence.
    # SUSPECT (2026-04-05): Raw range 3706-3939 across 77,090 samples — NEVER
    # zero. Heat output should be 0 when compressor is off. Slight inverse
    # correlation with load (idle: 3823, heating: 3793, DHW: 3785). Behaviour
    # is more consistent with a refrigerant property (possibly low-side pressure
    # in kPa at some scale) or a configuration/status register. Second sniffer
    # labelled "heat_output" at 3764W — but that was a single snapshot, not
    # time-series validated.
    # DO NOT use for heat output calculations until resolved.
    # Reg 64 was previously suggested as heat output (identified.md) but has
    # conflicting evidence from defrost analysis (wild uint16 swings).
    25: {"name": "Heat Output (suspect)", "scale": 1,    "unit": "W",     "icon": "mdi:fire",                 "class": "power"},
    # UNCONFIRMED: Second sniffer labels "heat_output_2" at 1148W. Purpose
    # unclear — possibly a secondary thermal calculation or DHW-specific output.
    28: {"name": "Heat Output 2",         "scale": 1,    "unit": "W",     "icon": "mdi:fire",                 "class": "power"},

    # --- Defrost / Operating State ---
    # CONFIRMED (defrost validated): Signed accumulator, NOT a simple countdown.
    # Normal: -160 to -190. Trigger: drops to -421. Counts up through 0 to +145/+246.
    # Resets to -40 to -57 after defrost. Likely integrates (evap_temp - threshold) × dt.
    57: {"name": "Defrost Accumulator",   "scale": 1,    "unit": "",      "icon": "mdi:snowflake-alert",      "class": None},
    # CONFIRMED (defrost validated + 2026-04-05 lifecycle analysis):
    # HP controller operating state — full state machine.
    # Startup lifecycle:  10 → 3 → 1 → 2  (standby → starting → init → running)
    # Shutdown lifecycle: 2 → 4 → 10       (running → stopping → standby)
    # Defrost lifecycle:  2 → 6 → 7 → 8 → 2 (running → pre-defrost → active → recovery → running)
    #
    # Complete enum:
    #   1  = Initialising (~2 min, compressor ramping — 1.0% of time)
    #   2  = Running (active heating or DHW — 22.7% of time)
    #   3  = Starting (~30s transition from standby — 0.4%)
    #   4  = Stopping (~40s transition to standby — 0.6%)
    #   6  = Pre-defrost (from defrost validation)
    #   7  = Defrost active (from defrost validation)
    #   8  = Post-defrost recovery (from defrost validation)
    #   10 = Standby (compressor off, resting — 75.2% of time)
    #
    # Values 1/3/4/10 from 53.6h analysis (77,075 samples).
    # Values 6/7/8 from earlier defrost validation.
    # This register is READ (FC 0x03) — reflects HP controller state,
    # independent of hub mode command (reg 92).
    65: {"name": "HP Controller State",   "scale": 1,    "unit": "",      "icon": "mdi:state-machine",        "class": None},

    # --- Counters ---
    # UNCONFIRMED: Monotonically increasing counter. Second sniffer saw 17780→18370
    # over ~5 min capture. Likely cycle runtime in seconds. Not verified.
    20: {"name": "Runtime Counter",       "scale": 1,    "unit": "s",     "icon": "mdi:timer-outline",        "class": None},
    63: {"name": "Energy Counter",        "scale": 1,    "unit": "Wh",    "icon": "mdi:counter",              "class": None},
    # CONFIRMED (2026-04-13, two-installation cross-validation — resolves 4.7.0 conflict):
    # Heat Output in watts, signed Int16. Positive during heating (r=0.999 vs
    # flow × 4.18 × ΔT thermal calculation, original identified.md analysis).
    # Negative during reverse-cycle defrost and at compressor-start transients.
    # Evidence (AdamLC Cosy 6, 13 Apr 2026, two defrost cycles at OAT ~2.6°C):
    #   Defrost has two distinct pressure phases, verified from raw timestamps:
    #     Phase 1 — defrost entry, compressor briefly off. R48 equalizes to
    #       ~3.85 bar (R290 sat −2°C, consistent with equalized evaporator).
    #       R64 near zero or mildly positive (+155 W at Event 1, +5923 W at
    #       Event 2 — compressor decelerating from heating).
    #     Phase 2 — compressor running in reverse (~3.8–4.1 min later). R48
    #       climbs to ~16 bar as reverse-cycle high-side pressurises the
    #       outdoor coil to melt ice. R64 reaches peak negative:
    #         Event 1: −8,276 W @ 01:32:58 (R48=16.24 bar co-incident)
    #         Event 2: −8,870 W @ 05:36:50 (R48=16.06 bar co-incident)
    #       Mean −5.8 / −5.9 kW sustained ~5.5 min per cycle.
    #   Raw register values at R64 peak-negative: 57,260 and 56,666 — classic
    #   unsigned-overflow signature of signed Int16.
    # Corroborating evidence (Stu Cosy 6, 13 Apr 2026): six compressor-start
    #   transients, signed values −4 to −893 W, durations 7–76 s. Correlate
    #   with R65 state sequence 10 → 3 → 1 → 2 (standby → starting → init →
    #   running). Same signed-overflow signature at much smaller magnitude.
    # Previously named "Heat Output (unverified)" with a TODO. The TODO is
    # now closed: reg 64 added to SIGNED_REGISTERS above.
    64: {"name": "Heat Output",           "scale": 1,    "unit": "W",     "icon": "mdi:fire",                 "class": "power"},

    # --- Fan ---
    # CONFIRMED: Fan speed. Both sniffers agree. Second sniffer value 727-739
    # range. Stuart confirmed from AP mode label.
    61: {"name": "Fan Speed",             "scale": 1,    "unit": "RPM",   "icon": "mdi:fan",                  "class": None},

    # --- Rated Specs (constant) ---
    # CONFIG: Constant 5500W across all captures. Matches Cosy 6 rated heating
    # capacity (5.5 kW). Configuration/nameplate register.
    59: {"name": "Rated Heat Capacity",   "scale": 1,    "unit": "W",     "icon": "mdi:information",          "class": None},
    # CONFIG: Constant 1300W across all captures. Matches Cosy 6 rated
    # electrical input. Configuration/nameplate register.
    60: {"name": "Rated Elec Input",      "scale": 1,    "unit": "W",     "icon": "mdi:information",          "class": None},

    # --- DHW ---
    # REVISED (2026-04-05): NOT a constant 60°C as second sniffer suggested.
    # Raw range 0-600 across 53.6h. Mode-dependent behaviour:
    #   Idle:    mean ~12 (1.2°C scaled — essentially zero/noise)
    #   Heating: mean ~428 (42.8°C)
    #   DHW:     mean ~554 (55.4°C)
    # Rises to 600 (60.0°C) only at peak DHW. This is NOT the cylinder temp
    # (that is reg 45, confirmed). Behaviour suggests this is either:
    # (a) a demand/target that tracks operating mode, or
    # (b) a calculated condensing temperature.
    # identified.md labelled it "Compressor Frequency" (×0.1, 0-60 Hz) which
    # would also fit the 0-600 range — but conflicts with reg 19 (confirmed
    # compressor freq). Keeping current name pending further investigation.
    53: {"name": "DHW Tank Temp",         "scale": 0.1,  "unit": "°C",    "icon": "mdi:water-boiler",         "class": "temperature"},
    # NAMED: Second sniffer labels "outdoor_unit_ambient" at 1.9°C. Close to
    # but distinct from reg 36 (confirmed OAT at 2.5°C). Likely the outdoor
    # unit's own ambient sensor vs the system OAT sensor at reg 36.
    # WARNING: 0-307 range during defrost — possibly packed register or sensor affected by defrost heat.
    # Published raw (no scale, no device_class) until byte decomposition tested.
    54: {"name": "Outdoor Ambient Raw",   "scale": 1,    "unit": "",      "icon": "mdi:thermometer",          "class": None},

    # --- Hub Control Registers ---
    # CONFIRMED (DHW demand state — promoted 2026-06-19):
    # Hub demand-state register. The earlier "4 = normal/standby" reading was
    # WRONG: 4 is the ACTIVE hot-water state, not standby.
    # Value map (1/2/4):
    #   1 = idle (no demand)                                — CONFIRMED
    #   4 = hot water active                                — CONFIRMED
    #   2 = non-idle, non-HW demand (space-heat candidate)  — UNCONFIRMED
    #       Leave annotated; do NOT classify until a reg 92 = 2 window is
    #       captured alongside a known space-heating call (see follow-up).
    # Evidence:
    #   - Cross-validated against the Octopus Kraken WATER-zone heatDemand /
    #     relaySwitchedOn signal (INSTRUCTION-351 / QSH). Reg 92 transitions to
    #     4 coincident with API hot_water_active=True, holds for the cycle
    #     envelope, and reverts to 1 on completion.
    #   - Time-series confirmation (history-6.csv, 18 Jun 2026): 1 → 4 at
    #     23:01:50Z, held ~39 min, → 1 at 23:40:35Z.
    # During DHW, reg_91 ramps from WC setpoint (38-42°C) to 65°C.
    # A derived `hot_water_active` boolean (reg 92 == 4) is published as a
    # separate binary_sensor (see _send_hw_active_discovery / publish_registers).
    # The raw numeric register is still published unchanged alongside it.
    92: {"name": "DHW Demand State",      "scale": 1,    "unit": "",      "icon": "mdi:thermostat",           "class": None},
    210:{"name": "Status Register",       "scale": 1,    "unit": "",      "icon": "mdi:information",          "class": None},

    # --- Unidentified (tracked for future analysis) ---
    # PROVISIONAL (downgraded 2026-04-13 — firmware-dependent):
    # On Stu installation (Cosy 6, firmware as of 4.7.0 ship): accumulates
    # seconds of compressor operation since last defrost (Event 2:
    # 1,648 increments over 1,650 s). Resets to zero at defrost initiation.
    # During defrost itself, shows sub-cycling (0→140 repeating).
    # On AdamLC installation (Cosy 6, firmware revision not yet queried):
    # R67 remains at 0 across an entire 24h capture (~107k rows), including
    # through two confirmed defrost cycles and extensive HEATING-state
    # operation. Cannot be universal Cosy 6 semantics at this time.
    # Action: query AdamLC firmware revision (unit sticker or controller
    # status register) and compare against Stu's to isolate cause. Firmware
    # delta is the leading hypothesis; confirming or ruling it out is the
    # fastest path to resolving provisional status.
    # Kept as "Runtime Counter" (rather than reverting to "Unknown 67") to
    # preserve existing HA automations from 4.7.0. Marked provisional until
    # a third installation (Connor or other) confirms or refutes.
    # Unsigned — not in SIGNED_REGISTERS.
    67: {"name": "Runtime Counter (provisional)", "scale": 1, "unit": "s", "icon": "mdi:timer-outline",    "class": "duration"},
    # UNRESOLVED (2026-04-11): Binary flag, 43 samples over 10 days, all 0.0.
    # Same slow poll rate as R210 (Status Register) and R77 (Config Param) —
    # these three registers share a polling group, suggesting they are
    # configuration or status registers read on a long cycle.
    # Candidate identities: defrost demand flag, error code, DHW priority,
    # or compressor protection latch. Never triggered during observation.
    75: {"name": "Unknown 75",            "scale": 1,    "unit": "",      "icon": "mdi:help-circle",          "class": None},
    77: {"name": "Config Param 77",       "scale": 1,    "unit": "",      "icon": "mdi:cog",                  "class": None},
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
        r29 = to_signed(registers.get(29, 0))   # Condenser inlet temp
        r30 = to_signed(registers.get(30, 0))   # Condenser outlet temp
        r57 = to_signed(registers.get(57, 0))    # Defrost accumulator
        r65 = registers.get(65, 0)               # HP controller state
        r92 = registers.get(92, 0)               # Hub mode demand
        delta = r29 - r30  # Condenser ΔT — inverts during defrost

        # Primary detection: use reg_65 HP controller state when available
        if r65 == 10 and r19 == 0:
            new_state = "OFF"
        elif r65 == 3:
            new_state = "STARTING"
        elif r65 == 1:
            new_state = "INITIALISING"
        elif r65 == 4:
            new_state = "STOPPING"
        # Defrost states (from defrost validation)
        elif r65 in (6, 7, 8) or (delta < 0 and r19 > 0):
            new_state = "DEFROST"
        # Mode-specific active states
        elif r92 == 4 and r19 > 0:
            new_state = "DHW"
        elif r92 == 2 and r19 > 0:
            new_state = "HEATING"
        elif r92 == 2 and r19 == 0:
            new_state = "HEATING_IDLE"
        elif r92 == 1 and r19 == 0:
            new_state = "IDLE"
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
                    "reg_19": r19, "reg_29": r29,
                    "reg_30": r30, "delta": round(delta, 1),
                    "reg_57": r57, "reg_65": r65, "reg_92": r92,
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
