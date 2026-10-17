# Cosy 6 — Unknown / Under-Investigation Registers

Registers with no accepted identity, or with a hypothesis that needs a test.
The decoder publishes refuted registers as "Unknown N" / "Unknown Temp N" until
they are identified. Evidence marked "2026-10-17" comes from the cold-weather
capture (see [identified.md](identified.md)).

## Hypotheses — test required

| Register | Decoder name | Hypothesis | Evidence (2026-10-17) | Test |
|----------|--------------|------------|-----------------------|------|
| 58 | Defrost Te Threshold (hypothesis) | Demand-defrost Te threshold, signed ×0.1 °C = −12.0 | Constant raw 65416 for the full capture. Both defrosts started 10 s after reg 57 ≤ −12.0 °C. A Te excursion to −12.0 °C 2.5 min after start (15 s) gave no defrost: a start-up inhibit probably exists. n=2 at OAT 2.3–2.4 °C | Capture defrosts at OAT < 0 °C and at 4–6 °C. Te at trigger constant → fixed threshold. Approach (OAT − Te) constant ≈14.3 K → approach criterion. Check reg 58 on AdamLC and Connor captures |
| 19 | Unknown 19 | Non-negative water-side quantity, clamped at 0 | 0 for several minutes of each defrost while reg 63 = 89.9 and input is 1.0–2.7 kW. The defrost 1 figure suggests the zero window equals the window where reg 64 < 0, not step 7.2 | In states 6–8: agreement of (reg 19 = 0) with (reg 64 < 0). In state 2: correlation with reg 64 and with reg 44 − reg 40 |
| 29 / 30 | Compressor Area Temp 1 / 2 | Compressor body, or inverter module | 48.9 / 43.9 °C after 4.5 h standby at OAT 2.4 °C; no response to defrost; lag reg 45 | Fit reg 29 to compressor power with a first-order lag; compare with the fit to reg 45 |
| 35 | V4 Inverter | Second valve, steps: liquid injection for compressor temperature, or refrigerant cooling of the inverter module | Homes with reg 32. Opens 80–114 (raw) when reg 29 is high: 0 % of cycles < 66 °C, 47 % at 70–72 °C, 97–100 % ≥ 72 °C | Slope of reg 29 when reg 35 opens and closes |
| 25 | DC Link Voltage (candidate) | DC-link voltage, ×0.1 V | 369.8–384.8 V; r=−0.84 with input power (sag under load); never zero | Compare with an inverter diagnostic display, if available |
| 43 | Liquid Line Temp | Defrost termination signal: step 7.2 ends at reg 43 ≥ ≈50 °C | Step 7.2 ended at 56.2 °C and 51.7 °C; step 7.2 is not fixed time (188 s, 175 s) | More defrost events across OAT |

## Identity open — current name refuted

| Register | Decoder name | Old name | Why refuted (2026-10-17) | Next test |
|----------|--------------|----------|--------------------------|-----------|
| 24 | Unknown 24 | Suction Pressure | 6,080 constant in heating and in both defrosts while reg 50 moves 0.04–14.0 bar g. Steps to 4,800–5,170 at start, in standby and at low load | Installer page at a known state |
| 28 | Unknown 28 | Heat Output 2 | Tracks power non-linearly (ratio 0.67 to Shelly EM, r=0.87); current-like | Clamp meter on the compressor lead |
| 37 | Unknown Temp 37 | Indoor Ambient | 36–43 °C during DHW while room sensors read 18.5–21.5 °C. Slow water-side or enclosure temperature | Installer page |
| 39 | Unknown Temp 39 | Outdoor Coil Air Temp | 20.6 °C in standby at OAT 2.4 °C; 7.6–22 °C in heating at OAT 2–2.5 °C; rises when the fan stops | Installer page |
| 54 | Unknown Temp 54 | Outdoor Ambient Raw | 19.5 °C in standby at OAT 2.4 °C; spikes to 44–66 °C at the reversals | Installer page |
| 55 | Unknown Temp 55 | Evaporator Temp | +6.6 °C in heating when Te = −11.9 °C; 20.5 °C in standby; floors at 0. **Do not use as a frost signal — use reg 57** | Installer page |
| 60 | Unknown 60 | Rated Elec Input | Not constant: steps 1100/1200/1300/1400 inversely with Te (r=−0.89). Not condensing pressure | Long capture across OAT |

## Constant or zero (2026-10-17)

- Zero for the full capture: regs 21–23, 31, 33, 42, 46, 49, 68–76, 78–80,
  210, 211.
- Reg 59 = 5,500 constant (Rated Heat Capacity, CONFIG).

## Withdrawn suggestions

Earlier suggestions in this file that the 2026-10-17 data contradicts:

| Register | Earlier suggestion | Now |
|----------|--------------------|-----|
| 26 | Electrical Input Power, possibly L1 phase or filtered | Total input, ratio 1.012 vs Shelly EM (identified.md) |
| 38 | Internal Unit Temp | Stays Suction Line Temp (superheat vs reg 57 consistent) |
| 39 | Outdoor Ambient Temp | Refuted — 20.6 °C in standby at OAT 2.4 °C |
| 50 | Reported COP | Suction pressure, bar g (identified.md) |
| 56 | Heat Output (alt), ×10 W | Saturated condensing temperature (identified.md) |
| 60 | Condensing Pressure | Not a pressure; steps with Te |
| 66 | Operating Sub-State 0–5 / Off-Heat-DHW-Defrost | Sub-step index of reg 65 (identified.md) |
| 24 | Proprietary speed reference | Still open — see table above |
| 37 | WC setpoint or plate HX temperature | Still open — see table above |
