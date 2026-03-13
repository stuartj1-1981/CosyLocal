# Cosy 6 — Unknown / Under-Investigation Registers

Registers with medium-confidence identification or observed behaviour that has not
yet been operationally confirmed.

## Medium Confidence

Statistical evidence supports the suggested identification but independent
cross-validation is incomplete.

| Register | Suggested Name | Scale | Unit | Range (scaled) | Notes |
|----------|---------------|-------|------|----------------|-------|
| 26 | Electrical Input Power | ×1 | W | 0–2956 | r=0.996 vs reg_27 — near-identical. May be L1 phase or raw vs filtered reading |
| 56 | Heat Output (alt) | ×10 | W | 0–6290 | Range fits 6 kW unit; median implied COP (reg_56×10 / reg_26) = 3.94 vs reg_50 mean 4.23 — 7% delta |
| 60 | Condensing Pressure | ×1 | kPa | 1100–2000 | 11–20 bar matches R290 saturation at 30–50°C condensing temperature. Currently labelled "Rated Elec Input" in code — conflicts with this identification |
| 65 | Month | ×1 | month | 1–12 | Discrete values observed: 1, 2, 4, 9, 10, 12 — consistent with months of logging history |

### Notes on Medium-Confidence Registers

**reg_26 vs reg_27**: Almost perfectly correlated (r=0.996) with reg_27 confirmed
against Shelly EM. Possible explanations:
- L1 vs L2 phase measurement (single-phase unit — unlikely)
- Raw vs filtered/averaged reading
- Instantaneous vs integrated power

**reg_56**: Scale factor of ×10 is unusual but produces physically plausible heat
output values (0–6290 W) for a 6 kW rated unit. Cross-validates against COP within
7%.

**reg_60**: Currently defined as "Rated Elec Input" (constant, scale ×1, unit W) in
`cosy6_decoder.py`. The statistical analysis suggests it may instead be condensing
pressure (kPa). If values vary with operating conditions, it cannot be a rated
constant — requires further logging to resolve.

**reg_65**: If this is indeed a month register, it implies the outdoor unit maintains
a real-time clock. Other time registers (day, hour, minute) may exist nearby.

## Requires Operational Confirmation

Registers where behaviour is observed but identity cannot be determined from
statistical analysis alone.

| Register | Observation | Suggested Test |
|----------|-------------|----------------|
| reg_66 | Values 0–3, discrete state | Log through DHW cycle and defrost event to map states to Off/Heating/DHW/Defrost |
| reg_37 | Temperature range 17.3–37.8°C, positive correlation with flow | Compare against controller setpoint during weather-compensation curve changes. May be WC setpoint or plate HX temperature |
| reg_43 | Temperature 3.2–36.7°C, anti-correlated with outdoor | Monitor during low ambient operation. Possible suction superheat or liquid line temperature |
| reg_24 | Range 4838–6086, correlates with compressor % (+0.667), units unclear | Compare against compressor Hz at known operating points. Possibly proprietary speed reference |

## Previously Identified Registers That May Need Correction

The statistical analysis suggests the following existing register names may be
incorrect. These should be verified operationally before changing:

| Register | Current Name | Suggested Name | Reason |
|----------|-------------|----------------|--------|
| 38 | T3 Suction | Internal Unit Temp | Tracks outdoor ambient +5°C (enclosure heating), not suction line |
| 39 | Evaporator Temp | Outdoor Ambient Temp | Range matches UK ambient; distinct from confirmed T1 External (reg_36) |
| 50 | Suction Pressure | Reported COP | Range 2.75–5.97 with mean 4.23; negative correlation with flow temp |
| 60 | Rated Elec Input | Condensing Pressure | Range 1100–2000 kPa matches R290 at 30–50°C condensing |
