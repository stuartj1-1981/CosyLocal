# Changelog

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
