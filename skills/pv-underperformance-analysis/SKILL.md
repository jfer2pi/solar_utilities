---
name: pv-underperformance-analysis
description: Recommend and break down the analyses to investigate underperformance of a utility-scale solar PV plant, from data review to field and lab work and commercial follow-up.
---

# PV underperformance analysis

Use this skill when someone reports or suspects that a solar PV plant (fixed tilt or single-axis tracker, monofacial or bifacial, central or string inverters) is producing less than budget, P50, the model, or its peers, and wants to know **which analyses to run, in what order, how to break each one down, and what to do with the result**.

The skill is built on Fernando's Underperformance Checklist Template (Model, External Availability, Internal Availability, Meter Data, Sensors, Trackers, DC Health, Inverter/Generator, MV/HV System). It extends that checklist with standards-based methods (IEC 61724-1/-2/-3, IEC TS 63019, IEC TS 62446-3), open-source analytics (RdTools, pvanalytics, pvlib) and 2024-2026 industry evidence (kWh Analytics, Raptor Maps, NREL PV Fleet, Kiwa PVEL, SolarPower Europe O&M BPG v6.0).

Guiding principle from the template: **start with data-based review and move toward more complex and more costly items.** Guiding principle from the research: **treat underperformance as an accounting problem before an engineering one.** Every MWh of the gap goes into a bar of a fixed energy bridge, and the residual is reported, not hidden.

---

## 1. Scope the case first

Before recommending anything, establish (ask only for what is missing and matters):

1. **Symptom and size**: gap vs what (budget P50, re-run model, peers, last year), over which period, % and MWh.
2. **Plant description**: country/market, MWac/MWdc, DC/AC ratio, fixed or tracker (and tracker brand, ATI/linked-row or independent row), module technology (PERC, TOPCon, HJT, CdTe), bifacial or not, inverter type, COD date.
3. **Data available**: SCADA resolution, string/combiner monitoring yes/no, number and class of POA/GHI/rear sensors, soiling station, revenue meter, PPC and TSO setpoint logs, satellite irradiance subscription (e.g. Solcast, Solargis), alarm/ticket logs, last IR scan.
4. **Contract context**: O&M availability or PR guarantee, module/inverter/tracker warranties, PPA or merchant, curtailment compensation regime.
5. **Shape of the gap**: sudden step, gradual drift, seasonal, time of day, single block/inverter or whole plant. The shape alone points to a short list (see section 5).

If the person is not there to answer, take the most reasonable reading, state the assumptions at the top, and proceed.

---

## 2. Use the right metric for the question

Recommend a metric hierarchy instead of raw PR alone:

| Metric | Use it to answer | Source |
|---|---|---|
| Raw PR | Headline only; swings about +/-10% seasonally | IEC 61724-1:2021 |
| NREL weather-corrected PR (Dierauf 2013) | Month-to-month and site-to-site trending; normalizes to the TMY irradiance-weighted cell temperature | NREL/TP-5200-57991; `pvanalytics.metrics.performance_ratio_nrel` |
| Energy Performance Index, EPI = measured / expected energy (agreed model, measured weather) | "Is the plant producing what the model says for the weather it saw?" | IEC TS 61724-3:2016 |
| Power Performance Index, PPI on unconstrained MPPT periods only | Isolates DC capacity/health; excludes clipping, curtailment, trackers not tracking | IEC TS 61724-2 Ed. 2.0 (2025) |
| Energy-based availability | Lost energy, not lost hours | IEC TS 63019 Annex B; SPE O&M BPG v6.0 |

For high DC/AC tracker plants, report PR with and without clipped intervals. Compute cell temperature from measured weather with the same thermal model used for the budget, not from back-of-module sensors.

---

## 3. Build the energy bridge (waterfall)

No standard prescribes the order, so fix one and keep it identical across sites and months (sequential substitution, IEC TS 61724-3 logic):

1. Budget P50
2. **Budget bias** bar (from the four-run model reconciliation, section 4 item A1)
3. Resource variance (measured POA/GHI vs budget; show both satellite and on-site basis if they disagree)
4. Temperature and wind variance → weather-adjusted expected energy (EPI denominator)
5. External unavailability (grid, TSO, force majeure)
6. Internal unavailability (inverter, MV, DC blocks)
7. Curtailment, cause-coded (technical/grid, market/negative price, export limit)
8. Excess clipping vs budget
9. Soiling
10. DC health (strings, combiners, fuses, modules, trackers), PPI-type ratio
11. Degradation vs budget assumption
12. MV/HV and auxiliary losses vs model
13. **Residual** (unexplained)

Interpretation rules:
- A persistent residual above about 2-3% of expected energy usually points at sensors, meter, timestamps or the model, not a physical loss (rule of thumb).
- Put a confidence interval on every bar (IEA PVPS Task 13: 95% per site, 83.4% for cross-site comparison) so nobody acts on noise.
- Context: US fleets ran 8.6% below P50 weather-adjusted over 2015-2023 (kWh Analytics 2025); P50s carry a 3-5% optimistic bias (DNV GL); NREL fleet median performance index is 0.95 even excluding availability. A plant that explains only physical losses will "find" less than the gap.

In R, the bridge is a tidy table `bar, mwh, pct_of_budget, ci_low, ci_high, method, confidence` plotted as a ggplot2 waterfall (`geom_rect` with cumulative sums).

---

## 4. Recommended analyses, tiered by cost

Present recommendations in this order. Items marked **[T]** come from the original template; **[+]** are additions from research. For each item, give: *data needed, method, threshold/check, output, escalation*.

### Tier 0. Data integrity (desk, near zero cost, always first)

- **[+] Timestamp and data QC.** Check time zone, DST, time shifts, stale/flat-lined values, interpolation artifacts, step changes from sensor swaps. In NREL's 19,460-inverter study 22% failed QC for "data shift" and only 41% passed overall. Methods: pvanalytics `quality.time`, `quality.gaps`, `quality.data_shifts`; in R, compare solar-noon timing of measured power vs `pvlib`-equivalent solar position, run-length tests for stale values. Output: QC flag table per channel.
- **[T] Meter data.** Meter vs inverter sum (meter should be slightly lower); integrate power to energy and compare with the cumulative energy register. **[+]** Also check night import/auxiliary consumption at the POI, CT/PT ratio and averaging convention (period-beginning vs period-ending). Tolerance 1-2% (rule of thumb). Confirm meter, CT and PT calibration dates and revenue-grade class.
- **[T] POA/GHI sensors.** Completeness, gap-fill with satellite. **[+]** Track the ratio of on-site POA to satellite POA over time: a drift >2-3% or two sensors diverging >2% flags soiling, drift or misalignment (rule of thumb). Use pvanalytics QCRad limits and clear-sky checks; infer orientation from data to expose a fixed sensor on a tracker plant. Note that cleaning sensors more often than modules biases PR low, and soiled sensors inflate PR and hide module losses. Satellite site adaptation needs 12+ months of ground data (24 optimal; Solargis). Recalibration typically every 2 years (ISO 9847). Field: confirm tilt, azimuth, levelling.
- **[T] Ambient temperature, module temperature, wind.** Plausibility and cross-sensor agreement; module temp above ambient by day and below at night; wind units (m/s) and scaling. **[+]** Use pvanalytics `module_temperature_check` (module temp must correlate with irradiance) to catch dead or swapped sensors.

### Tier 1. Model and accounting (desk)

- **[T] Model review.** **[+] A1. Four-run model reconciliation**: (1) budget meteo + design parameters, (2) measured meteo + design, (3) measured meteo + as-built, (4) as-built + measured losses. Splits estimate bias, as-built changes and operational loss. Audit the assumptions that most often bias P50: satellite dataset and its bias, transposition model, albedo, bifacial factor and rear mismatch, IAM, LID/LeTID, mismatch, ohmic losses, horizon, GCR and backtracking, soiling profile, availability assumption, degradation rate, DC/AC and clipping at sub-hourly resolution.
- **[+] A2. Metric hierarchy and bridge** as in sections 2 and 3.
- **[+] A3. Peer and fleet benchmarking.** Rank inverter specific yield (kWh/kWp) and EPI against peers on the same site and similar sites; deviation >3-5% from peers flags an inverter (rule of thumb). Is the plant within the normal 6-9% fleet gap or in the bottom quartile (>10%)?
- **[+] A4. Uncertainty bands** on every bar (see section 3).

### Tier 2. External and internal availability (desk)

- **[T] Grid availability and curtailments.** Classify events as equipment or grid related and escalate accordingly; repeatable loss method.
- **[+] IEC TS 63019 state tagging.** Tag every interval with one information category (full/partial capability, service set points, out of environmental spec, requested shutdown, out of electrical spec, scheduled maintenance, planned corrective, forced outage, suspended, force majeure, information unavailable) and apply the priority rule for concurrent states. Fix in the contract whether curtailment maps to "service set points" or "requested shutdown". Resolve "information unavailable" (comms gaps) before computing availability: RdTools `AvailabilityAnalysis` uses the meter to show production continued while inverter data was missing.
- **[+] Curtailment cause coding and lost energy.** Estimate with reference inverters scaled by capacity, a model calibrated on recent unconstrained days, or inverter "available power" tags, timed by PPC/TSO setpoint logs. Crude threshold rules can overstate curtailment 6-20x (NREL). Code each event as technical/grid, market/negative price or export limit, and record compensation status by market (Spain: technical-constraint curtailment is uncompensated and reached almost 11% in July 2025; verify Italy, UK, US PPA deemed delivery, Mexico, Colombia, Uruguay per PPA and grid code).
- **[T] Inverter availability and inverter/module availability.** Energy-weighted, not time-based; include partial (MPPT/power-module) outages and module-level losses. Note repeated failures and error codes, warranty tickets, spares strategy, mean time to repair (split into response, diagnostic, logistic, repair per IEC TS 63019). SPE recommends a minimum 98% guaranteed availability; NREL fleet median 0.991.
- **[+] Extreme-weather outage accounting.** Median extreme-weather outage lasts 2-4 days and costs about 1% of annual output (NREL).

### Tier 3. Loss mechanisms from data (desk)

| Item | Method (R first; Python libs as reference via `reticulate` or reimplementation) | Pitfall |
|---|---|---|
| **[T] Soiling** | RdTools `soiling_srr` (stochastic rate and recovery) on daily normalized PI or soiling-station ratio; CODS when soiling biases degradation. Fernando's `soilkit` package. Cleaning interval approx. sqrt(2·C/(rate·daily revenue)) | Positive steps from outage recovery or sensor recalibration get read as cleanings; filter availability events first. Many soiling stations are unreliable |
| **[T] Module degradation** | RdTools YoY on filtered normalized daily series with bootstrap CI; clear-sky workflow as cross-check; IEA T13 recommends an ensemble of methods. Needs ≥2 years (3-5 for tight CI) | Clipping-filter choice alone moved Rd by up to 0.6%/yr. **[+]** Young TOPCon: look for non-linear early UVID dip (Kiwa PVEL 2026 lab median 3.0%, max 14.1%; field 2% median, 8% max) instead of a straight line. PERC/HJT: LID and LeTID. Exclude snow months. Fleet median about -0.75%/yr (NREL) |
| **[+] Clipping and power limits** | Logic-based clipping mask (RdTools `logic_clip_filter`, F-score 85 vs 56 for legacy filter). Check plateau level vs nameplate and vs identical inverters; P-Q scatter to find kVA limit binding under reactive power demand; sub-hourly clipping vs hourly budget | A plateau below nameplate means wrong active-power limit, stuck PPC setpoint or reactive power. Plant export limits look like clipping |
| **[T] Inverter setpoints from PPC / configuration** | Compare PPC P, Q, PF setpoints with inverter response; confirm grid-code mode (Q mode, PF mode). One documented PPC logic error capped an inverter at 10% around midday: about 6% of its yield, invisible in availability | Time sync between PPC and inverter logs |
| **[T] Tracker performance and availability** | Ideal angle from pvlib `tracking.singleaxis` with correct GCR, axis tilt, `cross_axis_tilt` (slope-aware backtracking, NREL TP-5K00-76626); compare SCADA angle per NCU; availability = within manufacturer tolerance (±5° if missing). Check tracker clock and configured GCR. Fernando's Tracker Signal Analyser | Diffuse/smart tracking is not a fault. ATI/linked rows report one angle for several rows that may differ: needs photos or inclinometer audit. **[+]** Tracker twist costs >2%; hourly wind data underestimates wind-stow loss by up to 4%; verify stow on sub-hourly data, and verify hail stow function (hail = 73% of insured loss value) |
| **[T] DC health (CBs, LBDs, strings, modules)** | String/combiner current normalized by string count and divided by the timestamp median of peers; robust outliers (Tukey, Hampel); open string at I_norm <0.1. Fernando's string current exception report and DC health metric | Wrong string-count metadata gives false positives; clipping flattens currents. **[+]** Check fuse sizing (one case: undersized fuses caused 15% plant loss) |
| **[T] Shading and vegetation** | Performance-index residual heatmap on hour × day-of-year or sun azimuth × elevation; pvanalytics `features.shading`; trend by block for seasonal vegetation; cross-check with satellite/street imagery, site photos, cameras, PM reports | Shading is seasonal (worse in fall/winter); morning/evening loss on trackers points at GCR or slope errors |
| **[+] Snow** | pvlib `snow.coverage_nrel`, `dc_loss_nrel`, `loss_townsend`; pvanalytics snow categorization | Keep snow out of degradation and availability |
| **[T] Inverter efficiency** | AC/DC vs normalized AC power; max within 1% of datasheet (typ. 98.5-99%); flag excursions | DC measurement accuracy of inverters is limited |
| **[T] Inverter temperatures** | Power vs internal and ambient temperature against the derating curve. **[+]** Also transformer over-temperature derating; derating often starts at 40-50 °C ambient (rule of thumb) | Sensor fault vs real heat |
| **[+] Thermal model and γ** | Fit Faiman or PVsyst Uc/Uv coefficients to measured module temperature; regress effective temperature coefficient | Use the same thermal model for budget and actual |
| **[+] Bifacial gain and albedo** | Rear POA or albedometer vs view-factor model (IEC 61724-1:2021 adds rear irradiance and albedo) | Bifacial modules degraded about 2% more than monofacial in NREL tests |
| **[+] Spectral residual** | PI vs air mass or precipitable water, mainly CdTe | Unsourced, practitioner method |
| **[T] Inverter to meter losses** | Inverter AC sum vs meter by irradiance band; compare with model MV transformer (typ. 0.8-1.5%) and cable (0.5-1.5%) losses (rules of thumb). Include transformer no-load losses at night | If high, thermal inspection of MV/HV for loose connections or insulation issues |

### Tier 4. Field diagnostics (cost rises; deploy only on flagged blocks)

Escalation logic (IEA PVPS Task 13): IR and EL give the array-wide overview, string IV classifies, module-level flash or lab gives warranty-grade proof.

1. **Remote review of inverter/PPC parameters and logs** (cheap; often finds configuration caps).
2. **Site visit checks**: pyranometer tilt/azimuth/level and cleanliness; ambient sensor shading; wipe test with white cloth for soiling; vegetation state; tracker inclinometer audit (suppliers quote ±2-3°; one audited plant showed ±6° spread and about 3% loss).
3. **Aerial IR thermography (IEC TS 62446-3)**: ≥600 W/m², on-site weather, certified thermographer; about 3 cm/px for module/cell detail, 10-15 cm/px only reliable for string, combiner, inverter faults. At least annual; also after major site works if no string monitoring. Quarterly inspection gave 36% lower loss than annual (Raptor Maps 2026). Vendor loss percentages per anomaly (e.g. 100% open string, 33% bypassed substring) are for ranking, not energy accounting.
4. **Connector and fuse inspection independent of IR**: 79% of high-risk connector failures showed no thermal signature.
5. **String IV curve tracing (IEC 62446-1, IEC 60891 translation)**, typically ≥700 W/m²: steps = mismatch/bypass diodes; low Isc = soiling, shading, degradation; shallow vertical leg = series resistance (corrosion, PID); sloped horizontal leg = shunt/PID. Pre/post-wash traces quantify soiling. Healthy strings at about 90-100% of predicted.
6. **EL / PL / UV fluorescence** on samples: cracks, PID, LeTID, TOPCon checkerboard UVID pattern; UVF dates cracks (installation vs hail) for EPC or insurance disputes. Images find defects, not power: pair with IV or flash.
7. **Insulation resistance and PID tests** (IEC TS 62804-1), MV/HV thermal inspection, power quality and protection trip review.
8. **Lab or mobile flash testing** of a statistically significant module sample, light-soaked first (TOPCon dark-storage loss up to 1%/day reverses in light); soiling microscopy for non-thin-film. Last resort, used for warranty claims.

---

## 5. Shortcut: symptom to first analyses

| Symptom | Look first at |
|---|---|
| Sudden step down, one inverter/block | availability, PPC setpoint, configuration change, fuses/combiners, comms gap |
| Sudden step, whole plant | export limit, curtailment, meter/CT change, timestamp shift, sensor replacement |
| Gradual drift | soiling, sensor drift (POA/satellite ratio), degradation, vegetation |
| Seasonal (winter worse) | shading, snow, tracker backtracking/GCR, low-irradiance behaviour |
| Seasonal (summer worse) | thermal derating, vegetation, soiling (dry season), clipping |
| Midday flat top below nameplate | clipping misconfigured, PPC cap, reactive power, transformer derating |
| Morning/evening only | shading, backtracking, tracker clock/time zone |
| Gap vs P50 but EPI ≈ 1 | budget bias or resource; re-forecast, not an O&M issue |
| Large residual | data QC, meter, sensors, model |
| Young TOPCon plant, early loss | UVID, LID; light-soak before testing |

---

## 6. Route every finding to whoever pays

| Bridge bar | Route to |
|---|---|
| Internal availability, response times | O&M contract (availability guarantee typ. 98-99%, LD caps about 5-20% of contract value or about one year's fee). Many contracts exclude tracker, configuration and DC-health losses, so the owner must track them |
| Inverter faults, repeated codes | Inverter OEM warranty tickets; spares and MTTR plan |
| Module defects | Module warranty claim; needs serial-level IV, EL and flash evidence |
| Tracker angle/twist/stow | Tracker OEM and O&M; recalibration |
| Curtailment | Compensation claim where applicable (cause-coded); otherwise revenue forecast |
| Hail, storm damage | Insurance |
| Estimate bias | Revised P50/P90, budget and valuation, not a claim |

The template notes "a squeaky wheel gets the grease": make sure the party who can fix an issue is told quickly, with data.

---

## 7. Thresholds: sourced vs rule of thumb

Say which numbers are which. Rules of thumb (not standardized; agree them with the team): investigation trigger EPI or weather-corrected PR deviation >2-3% vs trailing 12 months; action bands ≤2% monitor, 2-5% desk attribution, >5% field and contract review; residual >2-3%; POA drift >2-3%; peer deviation >3-5%; soiling ratio alarm <0.97; tracker offset flag 2-5°; DC/AC >1.3 clips materially; meter tolerance 1-2%; inverter, transformer and cable loss ranges above. Sourced: fleet gaps and benchmarks quoted in sections 3-6. Exact IEC numeric requirements (sensor counts per MW, sampling intervals, filter limits) are in the paywalled standards; refer to the purchased text before putting them into contracts.

---

## 8. Output format

Default output is a **recommended analysis plan**:

1. One-paragraph read of the case (symptom, size, likely suspects from section 5, assumptions taken).
2. The analysis plan as a table in the template's format, grouped by the template sections in order (Model, External Availability, Internal Availability, Meter Data, Sensors, Trackers, DC Health, Inverter/Generator, MV/HV System), with added rows marked as new:

   | Section | Review item | Why for this case | Data / method | Check / threshold | Output | Priority (Tier) | Results | Recommendation | Initials/Date |

   Leave Results, Recommendation and Initials/Date blank for the team, unless data was provided and analyzed.
3. The energy bridge structure to fill in, with which bars can be computed from available data.
4. Field escalation steps only if desk analysis can't close the gap, with what each will prove.
5. Commercial routing for anything already identified.

Drop items that don't apply (e.g. trackers on fixed tilt, snow in Colombia) and say so in one line rather than listing them.

If the user asks for the checklist as a file, produce a Word document in the template's layout (title, Purpose paragraph, five-column table: Section | Review Item | Results | Recommendation | Initials/Date), adding the new rows within their sections and a findings page after the table. If the user supplies data, write analysis code in **R** (tidyverse, ggplot2, data.table where volume requires), reusing Fernando's existing packages and scripts where relevant (pvqc, poaQC, soilkit, pvtwin, OEYA PV Engine, string current exception report, DC health metric, Tracker Signal Analyser, BaxEnergy/Canary/Solcast extraction). Call RdTools/pvanalytics/pvlib through `reticulate` only when reimplementing in R is not practical.

## Key references

IEC 61724-1:2021; IEC TS 61724-2 Ed. 2.0 (2025); IEC TS 61724-3:2016; IEC TS 63019:2019; IEC TS 62446-3; IEC 62446-1; IEC 60891; ISO 9847. NREL Dierauf et al. 2013 (weather-corrected PR); NREL PV Fleet Performance FY24; NREL slope-aware backtracking TP-5K00-76626; IEA PVPS T13-22 (performance loss rate) and T13-24 (qualification of PV power plants); SolarPower Europe O&M Best Practice Guidelines v6.0 (2025); kWh Analytics Solar Risk Assessment 2025/2026; Raptor Maps Global Solar Report 2025/2026; Kiwa PVEL Scorecard 2026; RdTools, pvanalytics, pvlib documentation.
