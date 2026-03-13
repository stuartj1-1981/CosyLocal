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
| 29 | Flow Temp | ×0.1 | °C | — | CONFIRMED | r=0.999 vs flow temp sensor (n=1206) |
| 30 | Return Temp | ×0.1 | °C | — | CONFIRMED | Correlates with return pipe sensor |
| 36 | T1 External Temp | ×0.1 | °C | — | CONFIRMED | r=1.000 vs Octopus API (n=402) |
| 38 | Internal Unit Temp | ×0.1 | °C | −2.2 – 21.8 | STATISTICAL | Tracks outdoor ambient +5°C offset; r=0.953 vs reg_39 — consistent with enclosure heat from inverter/compressor. AP mode label: "T3 Suction" |
| 39 | Outdoor Ambient Temp | ×0.1 | °C | −5.5 – 17.4 | STATISTICAL | Range consistent with UK ambient over logging period. AP mode label: "Evaporator Temp" |
| 40 | Return Water Temp | ×0.1 | °C | 16.9 – 52.8 | STATISTICAL | r=0.950 vs flow temp; mean offset from flow = 3.72°C — consistent with stated system ΔT. AP mode label: "T5 Return Temp" |
| 45 | Discharge Gas Temp | ×0.1 | °C | 17.3 – 84.1 | STATISTICAL | Only register reaching >60°C; r=0.922 vs flow temp; mean 7.6°C above condensing temp. AP mode label: "T10 Discharge" |
| 47 | Flow Rate | ×0.01 | l/min | — | NAMED | Sika VVX20 flow meter built into unit |
| 50 | Reported COP | ×0.01 | — | 2.75 – 5.97 | STATISTICAL | Mean 4.23 consistent with live COP; negative correlation with flow temp confirms higher lift → lower COP. Previously labelled "Suction Pressure" |
| 51 | Compressor Speed | ×1 | % | 0 – 100 | STATISTICAL | Exact 0–100 range; correlates with compressor frequency |
| 53 | Compressor Frequency | ×0.1 | Hz | 0 – 60 | STATISTICAL | Raw 0–600 = 0–60 Hz; r=0.708 vs compressor speed % |
| 64 | Heat Output | ×1 | W | — | CONFIRMED | r=0.999 vs flow×ΔT thermal calculation |
| 66 | Operating Mode | ×1 | enum | 0 – 3 | STATISTICAL | Discrete values 0/1/2/3 — likely Off / Heating / DHW / Defrost. Requires operational confirmation |
| 91 | Target Flow Temp | ×0.1 | °C | — | CONFIRMED | Hub → outdoor unit setpoint |

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

## Method

- Analysis based on 24 hours of HA history data at ~3-second polling interval
- Correlations computed against known `cosy_flow_temperature` entity
- R290 saturation properties used to validate pressure/temperature consistency
- Registers confirmed at zero throughout the logging period are excluded
