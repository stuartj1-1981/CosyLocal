# Changelog

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
