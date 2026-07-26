# Cosy 6 — Identified Modbus Registers

Registers with high-confidence identification based on cross-validation against
independent sensors, known R290 refrigerant properties, and statistical correlation
analysis.

## Confidence Criteria

- **CONFIRMED**: Cross-validated against an independent sensor or physical calculation
  (correlation coefficient and sample size stated)
- **STATISTICAL**: Identified via statistical analysis of HA history data with strong
  supporting evidence (correlation, range, physical plausibility)

## Register Table

| Register | Name | Scale | Unit | Range (scaled) | Confidence | Evidence |
|----------|------|-------|------|----------------|------------|----------|
| 27 | Electrical Power In | ×1 | W | 0–2716 | CONFIRMED | r=0.999 vs Shelly EM (n=1206) |
| 29 | HP LWT | ×0.1 | °C | — | CONFIRMED | r=0.999 vs flow temp sensor (n=1206). HP leaving water temp (condenser outlet) — reads 2–4°C above system flow during steady state |
| 30 | Condenser Temp | ×0.1 | °C | — | REVISED | Condenser plate / refrigerant-side temp — NOT a water circuit temperature. Reads 40°C when HP off (retained heat), drops to 30°C during operation. Inverts vs reg 40 during steady state |
| 36 | T1 External Temp | ×0.1 | °C | — | CONFIRMED | r=1.000 vs Octopus API (n=402) |
| 38 | Internal Unit Temp | ×0.1 | °C | −2.2 – 21.8 | STATISTICAL | Tracks outdoor ambient +5°C offset; r=0.953 vs reg_39 — consistent with enclosure heat from inverter/compressor. AP mode label: "T3 Suction" |
| 39 | Outdoor Ambient Temp | ×0.1 | °C | −5.5 – 17.4 | STATISTICAL | Range consistent with UK ambient over logging period. AP mode label: "Evaporator Temp" |
| 40 | System Return Temp | ×0.1 | °C | 16.9 – 52.8 | CONFIRMED | Tracks Shelly return within −0.4°C during stable flow. Actual system return water temp (EWT). AP mode label: "T5 Return Temp" |
| 45 | Discharge Gas Temp | ×0.1 | °C | 17.3 – 84.1 | CONFIRMED | Only register reaching >60°C; r=0.922 vs flow temp; mean 7.6°C above condensing temp. AP mode label: "T10 Discharge" |
| 47 | Flow Rate | ×0.01 | l/min | — | NAMED | Sika VVX20 flow meter built into unit |
| 50 | Suction | ×0.01 | — | 2.75 – 5.97 | CONFIRMED | Cross-checked with HP installer page labelled: "Suction (bar). Previously labelled "Reported COP" |
| 51 | Compressor Speed | ×1 | % | 0 – 100 | STATISTICAL | Exact 0–100 range; correlates with compressor frequency |
| 52 | Fan Suction | ×1 | Pa | - | CONFIRMED | Cross-check with HP installer page labelled: "Fan suction (Pa)" |
| 53 | Compressor Frequency | ×0.1 | Hz | 0 – 60 | STATISTICAL | Raw 0–600 = 0–60 Hz; r=0.708 vs compressor speed % |
| 64 | Heat Output | ×1 (signed Int16) | W | Observed: −8,870 to +6,745 (AdamLC 24h, 13 Apr 2026). Theoretical Int16: ±32,767 | CONFIRMED | r=0.999 vs flow×ΔT thermal calculation (heating, original identified.md analysis). Signed interpretation confirmed by two installations 13 Apr 2026: AdamLC defrost peaks −8,276 W / −8,870 W (raw 57,260 / 56,666); Stu startup transients −4 to −893 W. R48 phases during defrost validate reverse-cycle physics (3.85 bar equalized at entry, ~16 bar at peak-negative R64). |
| 66 | Operating Mode | ×1 | enum | 0 – 3 | STATISTICAL | Discrete values 0/1/2/3 — likely Off / Heating / DHW / Defrost. Requires operational confirmation |
| 91 | Target Flow Temp | ×0.1 | °C | — | CONFIRMED | Hub → outdoor unit setpoint |
| 92 | Mode Demand | ×1 | enum | 1, 2, 4 | CONFIRMED | 1=Idle, 2=Heating, 4=DHW. 33 transitions over 53.6h; DHW onset matched HW schedule within 3 min (2 events). Used by state machine. |
| 65 | HP Controller State | ×1 | enum | 1–10 | CONFIRMED | Startup: 10→3→1→2 (standby→starting→init→running). Shutdown: 2→4→10. Defrost: 2→6→7→8→2. 77,075 samples. |
| 48 | Discharge Pressure | ×0.01 | bar | 4.5–13.5 | CONFIRMED | Reinstated from 4.4.0 demotion. Two defrost events confirm: ~13 bar normal (R290 sat 35°C = matches condenser inlet), ~5 bar defrost (equalized, R290 sat 5°C = matches evaporator). Second sniffer matched HP installer page label. |
| 67 | Runtime Counter (provisional) | ×1 | s | 0–37,000+ on Stu unit; 0 on AdamLC unit | PROVISIONAL | Stu Cosy 6: seconds since last defrost, 1:1 confirmed (1,648 counts over 1,650 s, Event 2 Apr 2026). AdamLC Cosy 6: register reads 0 throughout 24h capture including both defrost cycles — firmware-dependent. Held provisional pending third installation. |

## Cross-Validation: COP

Independent calculation confirms consistency between power, heat output, and COP registers:

```
COP_implied = Heat Output (reg_64) / Electrical Power In (reg_27)
median COP_implied ≈ 3.94
reg_50 mean COP  = 4.23
Δ = 7% — within expected measurement uncertainty
```

Note: reg_64 (Heat Output) is used here rather than reg_56, which was an earlier
candidate from a different unit's register map.

## Registers Under Review

| Register | Current Name | Issue | Reference |
|----------|-------------|-------|-----------|
| 25 | Heat Output | Raw 3706-3939, never zero — inconsistent with heat output. May be pressure/config. | 2026-04-05 analysis |

## Method

- Analysis based on 24 hours of HA history data at ~3-second polling interval
- Correlations computed against known `cosy_flow_temperature` entity
- R290 saturation properties used to validate pressure/temperature consistency
- Registers confirmed at zero throughout the logging period are excluded
