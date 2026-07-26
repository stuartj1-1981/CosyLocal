# Changelog

## 4.10.0 - 2026-07-26

### Revise registers based on cross-validation

Updated the following registers based on HP installer page
`Cosy Modbus Reg 52` -> `Fan suction`
`Cosy DHW Cylinder Temp` -> `Discharge Gas Temp`
`Cosy Reported COP` -> `Suction`

## 4.9.0 - 2026-07-26

### Breaking

Dropped `i386`, `armhf` and `armv7` support — Home Assistant has wound down
32-bit systems and the base images no longer receive Alpine updates.

### Support for suggested display precision for sensors

Enable support for setting suggested display precision for sensors
Set `compressor frequency` suggested display precision as `1`
Set  `flow rate` suggested display precision as `2`

### Housekeeping

Bump base image to alpine3.24
Use app version throughout to avoid any hardcoded versions

## 4.8.0 — 2026-06-19

### Reg 92 — promoted to CONFIRMED DHW demand state

"Mode Demand" → "DHW Demand State". The earlier "4 = normal/standby" reading
was wrong: 4 is the ACTIVE hot-water state. Value map now annotated inline:
`1` = idle (CONFIRMED), `4` = hot water active (CONFIRMED), `2` = non-idle /
non-HW demand (space-heat candidate, UNCONFIRMED — left annotated, not
classified pending a reg 92 = 2 capture alongside a known space-heating call).

Evidence: cross-validated against the Octopus Kraken WATER-zone
`heatDemand` / `relaySwitchedOn` signal (INSTRUCTION-351 / QSH) — reg 92 → 4
co-incident with API `hot_water_active=True`, holds for the cycle, reverts to
1 on completion. Time-series confirmation (history-6.csv, 18 Jun 2026):
1 → 4 at 23:01:50Z, held ~39 min, → 1 at 23:40:35Z.

### Derived `hot_water_active` binary_sensor

New MQTT discovery entity (`unique_id` `qsh_modbus_reg_92_hw_active`,
`device_class: heat`), ON iff reg 92 == 4 (explicit `== 4`, never `!= 1`).
Published in addition to the raw reg 92 sensor, which is unchanged. Same
availability topic + `expire_after` as the register sensors, so a sniffer /
MQTT dropout marks it `unavailable` rather than collapsing to a stale OFF.
Only emitted when reg 92 is present in the decoded frame.

Passive-only: read-side decode + MQTT publish. No bus writes added.

## 4.7.2 — 2026-04-13

### R64 — signed Int16 fix, resolves 4.7.0 conflict

Register 64 added to `SIGNED_REGISTERS` and renamed "Heat Output (unverified)"
→ "Heat Output". The 4.7.0 conflict between "CONFIRMED heat output" and
"wild uint16 swings in defrost" is resolved: both described the same signed
Int16 register in its positive and negative ranges. TODO in decoder closed.

Cross-installation evidence (AdamLC and Stu Cosy 6, 13 Apr 2026): peak
defrost values −8,276 W and −8,870 W; startup transients −4 to −893 W. Full
evidence and two-phase defrost physics in decoder comments.

### R67 — provisional downgrade

"Compressor Runtime" → "Runtime Counter (provisional)". AdamLC 24h capture
shows R67 pinned at 0 through two defrost cycles and all HEATING operation,
contradicting the 1:1 accumulation observed on Stu's unit. Held provisional
pending third installation. Name retained to preserve 4.7.0 HA automations.

### R48 — evidence envelope widened

Discharge Pressure comment now cites full observed envelope 3.85–20.83 bar
across two installations and two OAT regimes. Two-phase defrost pressure
behaviour documented (equalized entry → reverse-cycle high-side). No
register-dict changes.

Files affected: `rootfs/opt/qsh/cosy6_decoder.py`, `identified.md`,
`CHANGELOG.md`, `config.yaml`, `build.yaml` (if versioned).

## 4.7.1 — 2026-04-13

Register 64 is still unverified, but suspected to be heat output power - state class changed to power state.

## 4.7.0 — 2026-04-11

### Register map update — 10-day defrost event analysis

Two defrost events analysed from 10-day HA history capture (31 Mar – 11 Apr
2026, 38 entities, ~850k samples). Cross-correlation during defrost events
identified unknown registers and confirmed defrost behaviour patterns.

**Register promotions / identifications:**
- Reg 48: Reinstated "Discharge Pressure" (demoted to "Unknown 48" in 4.4.0).
  Two defrost events confirm: ~13 bar normal heating (R290 sat 35°C, matches
  condenser), ~5 bar during defrost (equalized). Added device_class "pressure".
- Reg 67: "Unknown 67" → "Compressor Runtime" (seconds counter). 1:1 second
  counting confirmed by Event 2 time analysis. Resets at defrost initiation.
  Added device_class "duration", unit "s".

**Documentation updates:**
- Reg 34 (Reversing Valve): Documented bit-packed status values from HA
  gateway perspective (96=heating, 48=standby, 6464.x=defrost transition
  with bit 12 = valve solenoid).
- Reg 35 (V4 Inverter): Documented shared 6464 defrost bitmask pattern.
- Reg 75: Added observation notes (43 samples, all 0.0, unresolved).
- Reg 57 (Defrost Accumulator): Two-event defrost characterisation added
  to register_map_confirmed document (not in this changeset).

Files affected:
- `rootfs/opt/qsh/cosy6_decoder.py` (REGISTER_NAMES: R48, R67, R75, R34, R35)
- `identified.md` (R48 and R67 added)
- `CHANGELOG.md`

## 4.6.5 — 2026-04-10

### Flow Rate sensor improvement (community contribution)

- Reg 47 (Flow Rate): Changed unit from `l/min` to `L/min` (SI-recommended
  uppercase symbol) and added `device_class: volume_flow_rate` for proper
  Home Assistant integration — enables long-term statistics, unit conversion
  support, and correct entity categorisation.

Contributed by Adam Curtis (#15).

Files affected:
- `rootfs/opt/qsh/cosy6_decoder.py` (REGISTER_NAMES)

## 4.6.4 — 2026-04-05

### Register map update — Mode registers confirmed, state machine enhanced

53.6-hour Modbus capture analysis (765,506 frames) with cross-reference
against HA history, HW schedule, and 12 room sensors.

**High priority:**
- Reg 92 (Mode Demand): Documented enum values 1=Idle, 2=Heating, 4=DHW.
  DHW confirmed against HW schedule binary sensor (2 events, ~3min delay).
- Reg 65: Expanded from 4 defrost values to full 8-value lifecycle enum
  (1/2/3/4/6/7/8/10). Renamed "Operating Mode" → "HP Controller State"
  to distinguish from reg 92. Disproved "Month" hypothesis in unknown.md.
- State machine (OperatingStateDetector): Fixed dead-code bug where
  `r25 == 0` could never trigger (reg 25 raw is always 3706-3939).
  Added reg 65 for startup/stopping sub-state detection. New states:
  OFF, STARTING, INITIALISING, STOPPING, IDLE.

**Medium priority:**
- Reg 25: Flagged "Heat Output" label as suspect (never zero at idle).
- Reg 50: Updated from "Unknown 50" to "Reported COP" (converging evidence).
- Reg 53: Revised comment — NOT a constant 60°C, varies by mode.
- Reg 64: Documented conflict between identified.md and defrost validation.

**Documentation:**
- Updated identified.md with reg 92 and reg 65 CONFIRMED entries.
- Removed disproven reg 65 "Month" hypothesis from unknown.md.
- Added "Registers Under Review" section to identified.md.

Files affected:
- `rootfs/opt/qsh/cosy6_decoder.py` (REGISTER_NAMES, OperatingStateDetector)
- `identified.md`
- `unknown.md`

## 4.6.3 — 2026-04-03

### Log path fix — write to addon_config persistent storage

Changed log output directory from `/data/modbus_logs` to `/config` so
that sniffer.log, daily modbus CSV, and register_map.json are written
to the add-on's persistent `addon_config` storage instead of the
ephemeral `/data` volume.

Removed hardcoded MQTT password from DEFAULT_CONFIG. Credentials are
injected at runtime via the S6 run script (HA service discovery or
add-on options).

Files affected:
- `rootfs/etc/services.d/qsh-sniffer/run`
- `rootfs/opt/qsh/cosy6_decoder.py`

## 4.6.2 — 2026-03-31

### Register correction — Regs 40, 44, 45

Reverted incorrect refrigerant-circuit naming applied to water circuit
sensors. These registers were previously confirmed as water-side sensors
and should never have been renamed:

- Reg 40: "Condenser Outlet Temp" → "T5 Return Temp" (r=0.974 vs
  independent return sensor, CONFIRMED)
- Reg 44: "Condenser Mid Temp" → "T9 Flow Temp" (38-40°C in raw frames,
  ~5°C above return during active heating, CONFIRMED)
- Reg 45: "Compressor Shell Temp" → "DHW Cylinder Temp" (52-54°C during
  active DHW cycle, CONFIRMED)

Evidence: 41-hour time-series correlation against independent pipe sensors,
raw Modbus frame analysis, thermodynamic ΔT validation.

## 4.6.1 — 2026-03-31

### Register correction — Reg 29 & 30

Corrected reg 29 and 30 labels. Previously labelled as water flow/return
temperatures based on a second sniffer's short capture. Disproven by
36-hour time-series comparison against primary flow/return sensors:

- Reg 29: "Flow Temp" → "Condenser Inlet Temp" (superheated gas, 50-74°C)
- Reg 30: "Return Temp" → "Condenser Outlet Temp" (subcooled liquid, 46-68°C)

Evidence: r=0.999 mutual correlation, stable 5.3°C ΔT (condenser approach),
values 20-40°C above actual water circuit temps. Confirmed water flow/return
remain at reg 44 (T9 Flow) and reg 40 (T5 Return).

No functional changes to frame parsing, MQTT publishing, or socket handling.

## 4.6.0 — 2026-03-31

### Register map update — cross-referenced second sniffer

Updated register annotations with evidence from independent second sniffer operator
who cross-referenced the HP outdoor unit's installer/AP mode page against
simultaneous Modbus captures:

**Updated annotations with second-sniffer corroboration:**
- Reg 19: Compressor Frequency — second sniffer matched "Compressor Speed" from HP installer page
- Reg 20: Runtime Counter — second sniffer saw monotonically increasing counter
- Reg 24: Suction Pressure — second sniffer matched "Suction Pressure" from HP installer page
- Reg 25: Heat Output — second sniffer "heat_output" (conflicts with confirmed reg 64)
- Reg 26: Electrical Power Total — second sniffer "electrical_power_1" (needs Shelly EM cross-check)
- Reg 28: Heat Output 2 — second sniffer "heat_output_2"
- Reg 47: Flow Rate — second sniffer matched "Flow Rate" from HP installer page
- Reg 48: Unknown 48 — second sniffer matched "Discharge Pressure" from HP installer page
- Reg 53: DHW Tank Temp — second sniffer constant 60.0°C, likely DHW target setpoint
- Reg 54: Outdoor Ambient Raw — second sniffer "outdoor_unit_ambient"
- Reg 55: Evaporator Temp — second sniffer "evaporator_temp"
- Reg 56: Discharge Temp — supports reg 45 = DHW cylinder interpretation
- Reg 59: Rated Heat Capacity — CONFIG constant 5500W
- Reg 60: Rated Elec Input — CONFIG constant 1300W
- Regs 36, 37, 38, 40, 41, 44, 45, 61: confidence comments updated with
  second-sniffer corroboration

**SIGNED_REGISTERS:**
- Added reg 54 (outdoor unit ambient temp can go negative)

**Housekeeping:**
- `sw_version` in discovery payloads updated from 4.5.1 to 4.6.0

No functional changes to frame parsing, MQTT publishing, or socket handling.

## 4.5.1

Fix MQTT availability topic mismatch causing permanent entity unavailability.

### Bug fix
- Fixed `availability.topic` in `_send_discovery` and `_send_discovery_custom` — was using `{base_topic}/status` (resolves to `Cosy HP/status`) instead of the correct `qsh_modbus/status` where LWT and gateway status are actually published
- All sensor entities now correctly track the gateway online/offline status

### Housekeeping
- `sw_version` in discovery payloads updated from 4.5.0 to 4.5.1

## 4.5.0

Stale data protection and gateway connectivity status.

### MQTT discovery: expire_after
- All sensor discovery payloads now include `expire_after: 60` — Home Assistant marks entities unavailable after 60s of no updates, preventing stale data from appearing live during gateway outages

### Gateway connectivity status topic
- New `qsh_modbus/status` topic publishes `online`/`offline` (retained) reflecting Waveshare TCP socket state
- MQTT Last Will and Testament (LWT) ensures broker publishes `offline` if the sniffer process dies unexpectedly
- Explicit `offline` published when entering TCP reconnect loop; `online` restored on successful reconnect
- New `binary_sensor.modbus_gateway_status` (device_class: connectivity, entity_category: diagnostic) enables HA automations to detect prolonged outages and trigger remedial action (e.g. smart plug power-cycle)

### Housekeeping
- `sw_version` in discovery payloads updated from 4.3.0 to 4.5.0

## 4.4.0

Major register map update validated by two confirmed defrost events (2026-03-26 04:22 and 07:16 UTC) plus multiple reversing-valve actuations.

### Register renames (defrost validated)
- **Reg 29**: "HP LWT" → "Flow Temp"
- **Reg 30**: "Condenser Temp" → "Return Temp" (correlates with return pipe sensor)
- **Reg 38**: "Internal Unit Temp" → "Suction Line Temp" (wide range -13 to +27°C)
- **Reg 39**: "Outdoor Ambient Temp" → "Outdoor Coil Air Temp" (tracks below outdoor, swings during defrost)
- **Reg 40**: "System Return Temp" → "Condenser Outlet Temp" (water return path, always below flow)
- **Reg 41**: "T6 Sump" → "Evaporator Inlet Temp" (goes to -10°C during defrost, R290 cross-check)
- **Reg 43**: "T8 Liquid" → "Liquid Line Temp"
- **Reg 44**: "T9 Flow Temp" → "Condenser Mid Temp"
- **Reg 45**: "DHW Cylinder Temp" → "Compressor Shell Temp" (52-54°C steady, drops 18°C during defrost)
- **Reg 32**: "V1 Heating" → "V1 Heating Valve"
- **Reg 34**: "V3 Defrost" → "Reversing Valve" (100% heating, drops to 0% during defrost)
- **Reg 36**: "T1 External Temp" → "OAT External"
- **Reg 37**: "T2 Intermediate" → "Indoor Ambient"
- **Reg 27**: "Electrical Power In" → "Compressor Power"

### Critical correction: R64
- **Reg 64**: "Heat Output" → "State Accumulator" — wild uint16 swings during defrost prove this is NOT thermal output. R25 is the actual heat output register.

### New registers (previously published as "Modbus Reg XX")
- **Reg 19**: Compressor Frequency (scale ×0.1 Hz, confirmed by physics)
- **Reg 24**: Suction Pressure (scale ×0.1 kPa, R290 saturation cross-check)
- **Reg 25**: Heat Output (primary thermal measurement)
- **Reg 26**: Electrical Power Total (includes fans/pumps, ~150W above R27)
- **Reg 28**: Heat Output 2 (secondary heat metric)
- **Reg 55**: Evaporator Temp (primary frost signal, defrost validated)
- **Reg 56**: Discharge Temp (defrost validated)
- **Reg 57**: Defrost Accumulator (signed integrator, not countdown)
- **Reg 62**: EEV Opening (54-75% heating, 0% during defrost)
- **Reg 65**: Operating Mode (state machine: 2=heating, 6=pre-defrost, 7=defrost, 8=recovery)
- **Reg 20**: Runtime Counter
- **Reg 63**: Energy Counter (was "Unknown 63")
- **Reg 53**: DHW Tank Temp (constant 60.0°C, was misidentified as Compressor Frequency)
- **Reg 54**: Outdoor Ambient Raw (unsigned, no device_class — suspect packed register)
- **Reg 67, 75, 77**: Tracked for future analysis

### Removed registers
- **Reg 51**: Compressor Speed % — removed (R19 is the confirmed compressor register)
- **Reg 66**: Operating Mode — removed (R65 is the confirmed state machine)

### Demoted to Unknown
- **Reg 48**: "Discharge Pressure" → "Unknown 48" (plausible but not closed)
- **Reg 50**: "Reported COP" → "Unknown 50" (may not be a pressure)

### SIGNED_REGISTERS changes
- Added R19 (compressor Hz), R24 (suction pressure)
- Removed R54 (scaling suspect during defrost, published raw unsigned)

### HA entity impact
- Existing entities retain history (unique_id unchanged)
- Scale changes cause expected discontinuities for R19, R24, R55, R54
- R64 name change from "Heat Output" → "State Accumulator" will affect dashboards using friendly name

## 4.3.0

- **Reg 29**: Renamed "Flow Temp" → "HP LWT" — condenser outlet leaving water temp; reads 2–4°C above system flow before volumiser mixing
- **Reg 30**: Renamed "Return Temp" → "Condenser Temp" — condenser plate / refrigerant-side temp, NOT a water circuit measurement. Reads 40°C when off (retained heat), drops during operation, inverts vs reg 40
- **Reg 40**: Renamed "Return Water Temp" → "System Return Temp" — actual system EWT; tracks Shelly return within −0.4°C. Upgraded from STATISTICAL to CONFIRMED

## 4.2.0

- **Reg 45**: Remapped from "Discharge Gas Temp" to "DHW Cylinder Temp" (CONFIRMED from 2026-02-18 DHW heating cycle — 52–55°C range consistent with cylinder storage)
- **Regs 0/1**: Annotated as unresolved — always {0,0} during both space heat and DHW; may be write-once demand latch whose transition was not captured
- **Reg 92**: Added annotation — always 4 across both modes; likely operating mode flag, pending transition capture
- Wrapped TCP keepalive tuning (TCP_KEEPIDLE/INTVL/CNT) in try/except AttributeError for platform safety
- Changed watchdog log interval from 60s to 5 minutes with frame count reporting
- Updated socket timeout log message for consistency

## 4.1.1

- Fixed half-open TCP socket hang: recv() would block indefinitely when the Waveshare gateway connection dropped silently, causing hours of undetected data loss
- Added TCP keepalive (KEEPIDLE=30s, KEEPINTVL=10s, KEEPCNT=3) to detect dead connections at the OS level within ~60s
- Increased recv timeout to 30s and wired it into reconnection logic (hub sends every ~2.5s, so 30s silence = dead connection)
- Added periodic watchdog log line ("Recv loop alive") every 60s to make future hangs immediately visible in logs

## 4.1.0

- Added register identifications from statistical analysis of HA history data (24h, ~3s interval)
- **High confidence**: reg_38 Internal Unit Temp, reg_39 Outdoor Ambient Temp, reg_40 Return Water Temp, reg_45 Discharge Gas Temp, reg_50 Reported COP, reg_51 Compressor Speed %, reg_53 Compressor Frequency Hz, reg_66 Operating Mode
- **Medium confidence** (documented in unknown.md): reg_26 alt power, reg_56 alt heat output, reg_60 condensing pressure, reg_65 month
- Corrected reg_50 from "Suction Pressure" to "Reported COP" (mean 4.23, cross-validated within 7% of implied COP)
- Corrected reg_38 from "T3 Suction" to "Internal Unit Temp" (tracks outdoor +5°C, r=0.953)
- Corrected reg_39 from "Evaporator Temp" to "Outdoor Ambient Temp" (range matches UK ambient)
- Added identified.md and unknown.md register documentation

## 4.0.0

- Fixed Modbus framing: scanning parser extracts multiple frames from concatenated TCP recv() chunks via CRC probing (fixes 0% CRC pass rate)
- Added pair_response() on ModbusFrame for clean request/response register mapping
- Added FC 0x05 (Write Single Coil) and FC 0x0F (Write Multiple Coils) parsing
- Upgraded OperatingStateDetector from timing-based (ACTIVE/IDLE/HEARTBEAT) to register-based (OFF/DEFROST/DHW/HEATING/HEATING_IDLE/OIL_RECOVERY) with state history and trigger register logging
- Upgraded RegisterTracker with min/max values, sample counts, function code tracking, and write register tracking
- Added MQTT state transition publishing and coil publishing
- Added MQTT discovery for unknown registers (as "Modbus Reg XX")
- Added debug logging of raw recv() hex data (enable with --debug or DEBUG=true)

## 3.0.0

- Migrated from standalone script to Home Assistant add-on
- Added S6 process supervision with automatic restart on crash
- Added exponential backoff reconnection to Waveshare gateway
- Added RotatingFileHandler (10MB × 5 backups) to prevent log disk fill
- Added MQTT auto-discovery from HA Supervisor API (no manual credentials needed)
- Added reconnect counter to stats logging
- Handles ConnectionResetError, BrokenPipeError, OSError gracefully
- Daily CSV log rotation

## 2.1.0

- Added reg_27 as "Electrical Power In" (CONFIRMED r=0.999 vs Shelly EM)
- Fixed reg_64 from "Energy Elec Consumed" to "Heat Output" (CONFIRMED r=0.999 vs flow×ΔT)
- Parked reg_63 as "Unknown 63" pending verification

## 2.0.0

- Complete rewrite with raw register naming (reg_XX)
- Added confidence annotations (CONFIRMED / NAMED / UNCONFIRMED)
- Added signed int16 handling for temperature registers
- Added operating state detection (ACTIVE / HEARTBEAT / IDLE)
- Added CSV frame logging

## 1.0.0

- Initial passive sniffer with hardcoded register names
- MQTT publishing with HA auto-discovery
- Waveshare RS485-to-WiFi gateway support
