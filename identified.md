# Cosy 6 — Identified Modbus Registers

Registers with an identification supported by an independent sensor, R290
refrigerant properties, or strong statistical evidence.

Last full review: 4.12.0, from the 2026-10-17 cold-weather capture (slave 10,
12,126 poll cycles, 00:00–08:25 UTC, OAT 2.0–4.6 °C, two defrosts) and the
findings document `cosy6-cold-weather-defrost-register-findings-2026-10-17.md`.
Evidence marked "2026-10-17" comes from that capture. One installation, one OAT
band: every STATISTICAL row needs a second installation.

Registers with no accepted identity are in [unknown.md](unknown.md).

## Confidence Criteria

- **CONFIRMED**: cross-validated against an independent sensor or a physical
  calculation (statistic and sample size stated)
- **STATISTICAL**: strong internal evidence (correlation, ratio, timing) on one
  installation
- **NAMED**: from AP-mode or installer-page labels, with data that fits

Pressures (regs 48, 50) are **gauge**. Absolute pressure = value + 1.013 bar.

## Register Table

| Register | Name | Scale | Unit | Range (scaled) | Confidence | Evidence |
|----------|------|-------|------|----------------|------------|----------|
| 20 | Electrical Power Input | ×1 | W | — | STATISTICAL | vs Shelly EM: median ratio 1.002, r=0.993 (5,521 cycles > 100 W). Use as the COP denominator. Was "Runtime Counter" |
| 26 | Electrical Power Total | ×1 | W | — | STATISTICAL | Also total input: median ratio 1.012 vs Shelly EM. Difference from reg 20 not explained |
| 27 | Compressor Power | ×1 | W | 0–2716 | STATISTICAL | Median ratio 0.933 vs Shelly EM (−106 W): NOT total input. The earlier r=0.999 (n=1206) holds for shape only. Was "Electrical Power In" (CONFIRMED) in this file |
| 32 | Main EEV Position | ×1 (signed) | steps | −586 – 624 | STATISTICAL | Homing −586 → 392 → 624 → 200 at start (with reg 35). 0 in reverse cycle; 160–390 in heating. Superheat (reg 38 − reg 57) held at 6.8 K, IQR 6.3–7.3 K. Was "V1 Heating Valve" (×0.1 %) |
| 34 | Defrost Valve Position | ×0.1 | % | 0–96 | STATISTICAL | 0 in heating and standby; ≈96 at reversal; 61–87 in defrost steps 7.2–8.x; 0 within 2 min after. Valve identity open. Was "Reversing Valve". Use reg 65 for "defrost active" |
| 36 | OAT External | ×0.1 | °C | — | CONFIRMED | r=1.000 vs Octopus API (n=402). Reads ≈2 K high for ≈5 min after each defrost (2026-10-17) |
| 38 | Suction Line Temp | ×0.1 | °C | — | CONFIRMED | AP label "T3 Suction". Superheat vs reg 57 held at 6.8 K (2026-10-17). The earlier "Internal Unit Temp" suggestion is withdrawn |
| 40 | T5 Return Temp | ×0.1 | °C | 16.9–52.8 | CONFIRMED | r=0.999 vs primary return sensor, +0.18 K median (4,369 heating cycles, 2026-10-17) |
| 41 | Sump Temp | ×0.1 | °C | — | NAMED | AP label "T6 Sump". +26.6 °C when Te = −11.9 °C; 40.6–41.0 °C for 1 h after DHW. Was "Evaporator Inlet Temp" (refuted) |
| 43 | Liquid Line Temp | ×0.1 | °C | — | NAMED | AP label "T8". 3.1 °C in standby at OAT 2.4 °C. In reverse cycle reads the outdoor-coil outlet: ≈5 °C while ice melts, then 51.7–56.2 °C at the end of step 7.2 (termination candidate, two events) |
| 44 | T9 Flow Temp | ×0.1 | °C | — | CONFIRMED | r=0.973 vs primary flow sensor, +1.86 K median (heating circuit, 2026-10-17) |
| 45 | Discharge Gas Temp | ×0.1 | °C | 17.3–93.0 | CONFIRMED | AP label "T10 Discharge". 93.0 °C at DHW stop; 62.4 °C 8.5 min later; 27.3 °C after 4.5 h standby. Discharge superheat (reg 45 − reg 56) median 21.7 K, IQR 18.3–23.8 K. Closes the 4.10.0 DISPUTED item |
| 47 | Flow Rate | ×0.01 | L/min | — | NAMED | Unit flow sensor. 19.3 L/min in reverse cycle. Reg 52 is its target |
| 48 | Discharge Pressure | ×0.01 | bar g | 3.6–23.7 | CONFIRMED | reg 56 = Tsat_R290(reg 48 + 1.013 bar) to −0.08 K median, 12,126 cycles (p5–p95 −0.17 to −0.01 K). Absolute assumption gives +7.4 K; R1270, R32, R410A excluded |
| 50 | Suction Pressure | ×0.01 | bar g | 0.04–14.0 | CONFIRMED | reg 57 = Tsat_R290(reg 50 + 1.013 bar) to −0.08 K median (p5–p95 −0.17 to +0.03 K). Standby equalised with reg 48 (3.89 vs 3.83). Was "Reported COP" — refuted: 2.24 when reg 64 / reg 20 = 3.49 |
| 51 | Circulation Pump Speed | ×1 | % | 0–100 | STATISTICAL | r=0.976 with reg 47. 100 % in reverse cycle; 33 % in standby pump runs. Was "Compressor Speed" in this file |
| 52 | Water Flow Target | ×0.1 | L/min | 6.5 (floor) – | STATISTICAL | (reg 52 / 10) / (reg 47 / 100) median 1.00, IQR 0.99–1.01 in run. Held at 12.0 in states 6–7. Was "Fan Suction" (Pa) |
| 53 | Compressor Demand | ×0.1 | — | 0–60.0 | STATISTICAL | 0 in standby; 59.9 when flow is below target; 15–42 near target; frozen in defrost; r=0.96 with reg 63. Was "DHW Tank Temp" (decoder) / "Compressor Frequency" (this file) |
| 56 | Saturated Condensing Temp | ×0.1 | °C | — | CONFIRMED | Tsat of reg 48 (gauge + atmosphere), −0.08 K median, all cycles. Was "Discharge Temp" |
| 57 | Saturated Evaporating Temp | ×0.1 (signed) | °C | −41.3 – +44.0 | CONFIRMED | Tsat of reg 50, −0.08 K median, all cycles. Both 2026-10-17 defrosts started 10 s after reg 57 reached −12.0 °C (reg 58). Was "Defrost Accumulator" |
| 61 | Fan Speed | ×1 | RPM | — | CONFIRMED | AP label; both sniffers agree. 0 rpm in defrost steps 7.x–8.1; restarts in step 8.2 |
| 62 | Fan Speed Command | ×1 | % | — | STATISTICAL | r=0.982 with reg 61 in run; 0 when the fan stops. Was "EEV Opening" |
| 63 | Compressor Speed | ×0.01 | — (rps or Hz, open) | 0–104.8 | STATISTICAL | log(power) fit with both pressures R²=0.97 (reg 19: 0.61). Fixed 89.9 in step 7.2; 59.9 in steps 1.2, 6.2, 8.1, 8.2. 0 in standby. Was "Energy Counter" |
| 64 | Heat Output | ×1 (signed Int16) | W | −8,870 – +6,745 | CONFIRMED | reg 47 × 4.18 × (reg 44 − reg 40): ratio 1.001, r=0.9994 in run; 0.98 in defrost (2026-10-17). Signed reading confirmed on two installations (13 Apr 2026) |
| 65 | HP Controller State | ×1 | enum | 1–10 | CONFIRMED | Start 10→3→1→2; stop 2→4→10; defrost 2→6→7→8→2 (twice on 2026-10-17). Sub-step in reg 66 |
| 66 | HP State Sub-step | ×1 | index | 0–5 | STATISTICAL | Sub-step within the reg 65 state. Defrost 6.1–6.2, 7.1–7.2, 8.0–8.3. Changes coincide with plant actions (speed steps, fan stop, reversal). Was "Operating Mode" (Off/Heat/DHW/Defrost) — refuted |
| 67 | Sub-step Timer | ×1 | s | 0–37,000+ | STATISTICAL | Counts 1 s/s from 0 in each sub-step (slope 0.99–1.01, 19 blocks). 0 in state 2 and step 7.2. Explains AdamLC zeros. Open: Stu April 2026 counting before a defrost. Was "Runtime Counter (provisional)" |
| 77 | Standby Pump Run Flag | ×1 | enum | 0, 2 | STATISTICAL | 2 in 937 of 961 standby pump-on cycles; 0 in 5,594 of 5,615 pump-off cycles. Pump runs 5 min every 30 min at OAT ≈ 2 °C |
| 91 | Target Flow Temp | ×0.1 | °C | 36.7–65.0 | CONFIRMED | Hub write equals Octopus `fixed_target_flow_temperature` ×10 at all 9 heating changes (2026-10-17). 65.0 °C during DHW |
| 92 | DHW Demand State | ×1 | enum | 1, 2, 4 | CONFIRMED | 1 = idle, 2 = space heating, 4 = DHW. Value 2 confirmed 2026-10-17: 1 → 2 at 05:07:24 UTC, reg 65 10 → 3 three seconds later, heating-circuit flow rises. Value 4 confirmed 2026-06-19 (Octopus Kraken WATER zone) |

## Cross-Validation: COP

```
COP = Heat Output (reg 64) / Electrical Power Input (reg 20)
2026-10-17, 10 min before defrost 1:  5.92 kW / 1.70 kW = 3.49
```

Do not use reg 27 as the denominator: it is compressor power, 0.933 of the
Shelly EM reading, so the COP reads ≈7 % high. Reg 50 is suction pressure,
not a COP.

## Method (2026-10-17)

- Poll cycles built from FC 0x03 responses (regs 19–52, 53–80, 210–211) and
  FC 0x10 hub writes (regs 0–1, 91–97); raw uint16, signed int16 where stated
- R290 saturation: CoolProp `Propane`, pressure = register / 100 + 1.01325 bar
- Shelly EM and primary flow/return sensors: HA step-hold values merged onto
  poll cycles
- Power fit: OLS, log(reg 27) on log(speed), log(Pd_abs), log(Ps_abs), state 2
  with reg 27 > 200 W
- A separate agent re-derived ten claims from the raw files: 8 PASS, 2 PARTLY,
  0 FAIL
- Earlier analysis (2026-04): 24 h of HA history at ~3 s polling; correlations
  against the `cosy_flow_temperature` entity
