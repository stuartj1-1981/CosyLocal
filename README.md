# QSH Modbus Sniffer

Passive Modbus RTU sniffer for the **Octopus Energy Cosy heat pump.

Captures all traffic between the Cosy Hub (master) and outdoor unit (slave) via a Waveshare RS485-to-WiFi gateway in transparent transmission mode. Publishes decoded register values to Home Assistant via MQTT auto-discovery.

## Features

- **Passive sniffing** — zero interference with the bus, no active polling
- **Auto-discovery** — registers appear automatically as HA sensors
- **Robust reconnection** — exponential backoff on gateway disconnection
- **S6 process supervision** — auto-restart on crash
- **CSV logging** — full frame-level logs for analysis
- **Operating state detection** — tracks ACTIVE / HEARTBEAT / IDLE modes
- **Signed int16 handling** — correct negative temperature values

## Hardware Required

- Waveshare RS485-to-WiFi gateway (transparent mode, 19200 baud 8N1)
- Wired in parallel to the RS485 bus between Cosy Hub and outdoor unit

## Register Map

Registers are published as `sensor.qsh_modbus_sniffer_*` entities. See [identified.md](identified.md) for full details and evidence, [unknown.md](unknown.md) for registers under investigation.

Home Assistant sets the entity ID when it first discovers a register. A rename in a later version changes the display name only, not the entity ID. See the 4.12.0 changelog.

To make the entity IDs agree with the names, use `tools/cosy_ha_migrate.py` (see the 4.13.0 changelog). It renames through the HA entity registry, so the recorder history moves with each entity:

```bash
python3 tools/cosy_ha_migrate.py grep /config /addon_configs
```

```bash
HA_TOKEN=<token> python3 tools/cosy_ha_migrate.py rename --url ws://homeassistant.local:8123/api/websocket --apply
```

Derived entities (defrost active, defrost margin, controller state text) are in `homeassistant/packages/cosy_derived.yaml`.

| Register | Name | Scale | Unit | Confidence |
|----------|------|-------|------|------------|
| 20 | Electrical Power Input | ×1 | W | CONFIRMED (5.20 vs 5.19 kWh Shelly EM, 2026-11-08) |
| 27 | Compressor Power | ×1 | W | STATISTICAL (ratio 0.933 vs Shelly EM — not total input) |
| 36 | OAT External | ×0.1 | °C | CONFIRMED (r=1.000 vs API) |
| 40 | T5 Return Temp | ×0.1 | °C | CONFIRMED (r=0.999 vs primary return) |
| 44 | T9 Flow Temp | ×0.1 | °C | CONFIRMED (r=0.973 vs primary flow) |
| 45 | Discharge Gas Temp | ×0.1 | °C | PROBABLE (2026-11-08) |
| 47 | Flow Rate | ×0.01 | L/min | NAMED |
| 48 | Discharge Pressure (gauge) | ×0.01 | bar g | CONFIRMED (R290 saturation) |
| 50 | Suction Pressure (gauge) | ×0.01 | bar g | CONFIRMED (R290 saturation) |
| 55 | Suction Superheat (calc) | ×0.1 | K | CONFIRMED (= reg 38 − reg 57) |
| 56 | Saturated Condensing Temp (calc) | ×0.1 | °C | CONFIRMED (= Tsat(reg 48)) |
| 57 | Saturated Evaporating Temp (calc) | ×0.1 | °C | CONFIRMED (= Tsat(reg 50)) |
| 58 | Defrost Te Threshold | ×0.1 | °C | CONFIRMED (−12.0; 3 of 3 defrosts) |
| 63 | Compressor Speed | ×0.01 | — | CONFIRMED (unit open: rps or Hz) |
| 64 | Heat Output | ×1 | W | CONFIRMED (r=0.999 vs flow×ΔT) |
| 65 | HP Controller State | ×1 | enum | CONFIRMED |
| 91 | Target Flow Temp | ×0.1 | °C | CONFIRMED |
| 92 | DHW Demand State | ×1 | enum | CONFIRMED (1 idle, 2 heating, 4 DHW) |

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `gateway_host` | `192.168.2.73` | Waveshare gateway IP |
| `gateway_port` | `8899` | Gateway TCP port |
| `mqtt_host` | *(auto)* | MQTT broker (auto-detected from HA if empty) |
| `mqtt_port` | `1883` | MQTT port |
| `mqtt_user` | *(auto)* | MQTT username |
| `mqtt_pass` | *(auto)* | MQTT password |
| `publish_interval` | `5` | Seconds between MQTT publishes |
| `csv_max_total_mb` | `500` | Maximum total size of all CSV frame logs, in MB (50-10000) |
| `debug` | `false` | Enable debug logging |

## Useful Template Sensors

```yaml
# CoP calculation — heat output (reg 64) / total electrical input (reg 20).
# Do not use reg 27 (Compressor Power): it reads ~7 % below the total input.
template:
  - sensor:
      - name: "Cosy CoP"
        unit_of_measurement: ""
        state: >
          {% set heat = states('sensor.qsh_modbus_sniffer_cosy_heat_output') | float(0) %}
          {% set power = states('sensor.qsh_modbus_sniffer_cosy_electrical_power_input') | float(0) %}
          {% if power > 50 %}
            {{ (heat / power) | round(2) }}
          {% else %}
            0
          {% endif %}
```

On an install from before 4.12.0, reg 20 keeps its old entity ID (for example `sensor.qsh_modbus_sniffer_cosy_runtime_counter`). Check the entity ID in Home Assistant before you use the template.

## Logs

The add-on writes all logs to its `addon_config` folder (`/config` inside the add-on container).

| File | Content | Size limit |
|------|---------|------------|
| `sniffer.log` | Add-on messages | 6 files of 10 MB (rotating) |
| `modbus_YYYY-MM-DD.csv`, `modbus_YYYY-MM-DD_001.csv`, ... | Every Modbus frame | `csv_max_total_mb` (circular) |
| `register_map.json` | Register map | None (fixed size) |

The CSV log is a circular log. A new CSV file starts at UTC midnight, and also when the active file reaches 1/10 of `csv_max_total_mb`. When the CSV files together exceed the limit, the add-on deletes the oldest CSV file first. The active file is never deleted. The add-on counts and deletes only files that match the `modbus_YYYY-MM-DD[_NNN].csv` pattern. It does not touch other files in the folder.

To keep a capture, copy the CSV files out of `addon_config` before the limit removes them.


