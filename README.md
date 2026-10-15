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

| Register | Name | Scale | Unit | Confidence |
|----------|------|-------|------|------------|
| 27 | Electrical Power In | ×1 | W | CONFIRMED (r=0.999 vs Shelly EM) |
| 29 | HP LWT | ×0.1 | °C | CONFIRMED |
| 30 | Condenser Temp | ×0.1 | °C | REVISED |
| 36 | T1 External Temp | ×0.1 | °C | CONFIRMED (r=1.000 vs API) |
| 38 | Internal Unit Temp | ×0.1 | °C | STATISTICAL |
| 39 | Outdoor Ambient Temp | ×0.1 | °C | STATISTICAL |
| 40 | System Return Temp | ×0.1 | °C | CONFIRMED (tracks Shelly return) |
| 45 | Discharge Gas Temp | ×0.1 | °C | STATISTICAL (r=0.922 vs flow) |
| 47 | Flow Rate | ×0.01 | l/min | NAMED |
| 50 | Reported COP | ×0.01 | — | STATISTICAL (mean 4.23) |
| 51 | Compressor Speed | ×1 | % | STATISTICAL |
| 53 | Compressor Frequency | ×0.1 | Hz | STATISTICAL |
| 64 | Heat Output | ×1 | W | CONFIRMED (r=0.999 vs flow×ΔT) |
| 66 | Operating Mode | ×1 | enum | STATISTICAL |
| 91 | Target Flow Temp | ×0.1 | °C | CONFIRMED |

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
# CoP calculation
template:
  - sensor:
      - name: "Cosy CoP"
        unit_of_measurement: ""
        state: >
          {% set heat = states('sensor.qsh_modbus_sniffer_cosy_heat_output') | float(0) %}
          {% set power = states('sensor.qsh_modbus_sniffer_cosy_electrical_power_in') | float(0) %}
          {% if power > 50 %}
            {{ (heat / power) | round(2) }}
          {% else %}
            0
          {% endif %}
```

## Logs

The add-on writes all logs to its `addon_config` folder (`/config` inside the add-on container).

| File | Content | Size limit |
|------|---------|------------|
| `sniffer.log` | Add-on messages | 6 files of 10 MB (rotating) |
| `modbus_YYYY-MM-DD.csv`, `modbus_YYYY-MM-DD_001.csv`, ... | Every Modbus frame | `csv_max_total_mb` (circular) |
| `register_map.json` | Register map | None (fixed size) |

The CSV log is a circular log. A new CSV file starts at UTC midnight, and also when the active file reaches 1/10 of `csv_max_total_mb`. When the CSV files together exceed the limit, the add-on deletes the oldest CSV file first. The active file is never deleted. The add-on counts and deletes only files that match the `modbus_YYYY-MM-DD[_NNN].csv` pattern. It does not touch other files in the folder.

To keep a capture, copy the CSV files out of `addon_config` before the limit removes them.


