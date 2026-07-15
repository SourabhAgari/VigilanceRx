# RxVigilance — Project Specification

> Real-time medication adherence and refill-gap detection using Apache Flink,
> Kafka (Redpanda) → Kafka, with event-time timers as forward-looking predictions

---

## Overview

RxVigilance is a stateful streaming pipeline that consumes pharmacy fill and
reversal events for chronic/maintenance medications, maintains a rolling
coverage picture per member per therapeutic drug class, and emits early-warning
alerts *before* a member's supply lapses — instead of discovering the gap
retrospectively in a monthly PDC (Proportion of Days Covered) batch report.

The core mechanic: a `KeyedBroadcastProcessFunction` registers a cancellable
**event-time timer** per key at `supplyEndDate - alertLeadDays`. A refill
arriving in time deletes and re-registers the timer; if nothing arrives, the
timer fires and emits a `GapRiskAlert`. A second, harder timer at the actual
exhaustion date emits a `LapsedAlert` if no intervention follows.

**Stack:** Java 17 · Apache Flink 1.18 · Redpanda (Kafka-compatible) · RocksDB
· Avro · Maven · Terraform · Helm · GKE · Prometheus/Grafana · SonarQube Cloud

This is the technical companion to `project_functional_document.md`. That
document defines *what* and *why*; this document defines *how*.

---

## Project coordinates

```
<groupId>com.healthcare</groupId>
<artifactId>rx-vigilance</artifactId>
<version>1.0.0-SNAPSHOT</version>
```

Base package: `com.healthcare.rxvigilance`

---

## Why real-time streaming instead of a daily batch job

*(Canonical source for the five batch-vs-real-time problems. Restated for a
business audience in `project_functional_document.md` §3 and for a
cost/revenue audience in `business_impact_and_cost_analysis.md` §3 — if the
reasoning below changes, update those two as well.)*

The baseline alternative is a nightly batch job (Spark/SQL) that recomputes
each member's coverage status once a day. That approach has five structural
problems this pipeline is designed to solve.

| # | Problem with a daily batch job | How this pipeline solves it |
|---|---|---|
| 1 | **Detection lag is baked in.** A midnight batch run won't notice a threshold crossed at 6am until up to 24h later. | The event-time timer fires at the exact moment the configurable alert-lead-time threshold is crossed, not on the next scheduled run. |
| 2 | **Full recomputation, every run** — even for members with zero new activity, wasteful at ~30M+ active keys. | State is incremental: a member's coverage updates once, when their event arrives, then sits quietly in RocksDB. No rescanning. |
| 3 | **Reversals corrupt state until the next run.** Decisions made mid-day rest on fills already invalidated hours earlier. | Reversal handling reacts to the reversal event itself, recomputing coverage and cancelling stale timers immediately. |
| 4 | **No way to represent "waiting."** Batch can only answer "as of now, at risk?" — not "will run out on Jan 30 unless something changes." | `registerEventTimeTimer` / `deleteEventTimeTimer` *is* an explicit, cancellable, forward-looking prediction per key. |
| 5 | **Flat, undifferentiated alert lists** with no freshness or ordering. | Alerts stream out continuously via side outputs as detected; downstream outreach prioritizes by recency and severity. |

**Where batch remains the right tool.** CMS Star Ratings PDC is a
measurement-year figure — nightly batch reconciliation of the official PDC
number for regulatory submission is reasonable. This pipeline's PDC snapshot
side output *feeds* that reconciliation process; it does not replace it.

---

## Scope

**In scope (v1)**
- Members on CMS Star Ratings-tracked chronic therapy: diabetes, statins
  (cholesterol), RAS antagonists (hypertension).
- Real-time gap-risk detection with a **configurable alert lead time**
  (per drug class × dispensing channel — not a fixed day count).
- Real-time PDC accumulation (coverage-day facts) for downstream reporting.
- Reversal-aware state correction.
- Comorbid members tracked independently per drug class.

**Out of scope (v1)**
- Acute/short-course therapies — filtered upstream, never enter keyed state.
- Specialty/infusion drugs with non-standard day-supply semantics
  (`project_functional_document.md` FR-9).
- Retroactive backfill of historical PDC (separate batch reconciliation job).
- Member-facing notification delivery — this pipeline emits alerts to topics;
  downstream outreach systems own delivery.

---

## Kafka topics

Platform: **Redpanda** — Docker Compose locally, **Redpanda Cloud**
(`SASL_SSL`) for integration/demo. Vendor decision 2026-07-05, see
`IMPLEMENTATION.md` Phase 3.

| Topic | Direction | Description |
| --- | --- | --- |
| `rx-fill-events` | Source | Fill and reversal events from claims adjudication (Avro) |
| `ndc-drug-class-ref` | Source (broadcast) | NDC → chronic drug class + trackability flag; slowly changing |
| `alert-lead-time-ref` | Source (broadcast) | `(drug_class, dispensing_channel)` → `alert_lead_days`; slowly changing |
| `gap-risk-alerts` | Sink | `GapRiskAlert` — supply will lapse in ≤ lead-time days |
| `lapsed-alerts` | Sink | `LapsedAlert` — supply exhausted, no refill arrived |
| `pdc-snapshots` | Sink | Coverage-day facts and running numerator — **not** a finished PDC ratio (FR-6b) |
| `dead-letter` | Sink | Undeserializable / structurally invalid events |

Schema registry: Redpanda built-in (Confluent-API-compatible),
`FULL_TRANSITIVE` compatibility on all subjects (`hld.md` §7).

---

## Domain model

### RxFillEvent *(source)*

One input event = one pharmacy fill or fill-reversal, for one member, for one
NDC (drug). This grain matches the real NCPDP claim standard: one claim = one
NDC — no aggregation or splitting upstream.

```
eventType          EventType   — FILL | REVERSAL
claimId            String      — unique claim identifier
memberId           String      — keyBy field (with drugClass)
ndcCode            String      — resolved to drugClass via broadcast state
fillDate           LocalDate   — event time source
daySupply          int
quantity           BigDecimal
pharmacyId         String
rxNumber           String
refillsAuthorized  int
dispensingChannel  Channel     — RETAIL | MAIL_ORDER | SPECIALTY | UNKNOWN
                                 (default UNKNOWN; alert-lead-time lookup key)
originalClaimId    String      — REVERSAL only; references the claimId undone
```

### GapRiskAlert *(sink)*

```
alertId       String      — generated UUID
memberId      String
drugClass     String
expiresOn     LocalDate   — projected supply-exhaustion date
leadDays      int         — lead time that was applied
emittedAt     Long        — event-time timestamp of the firing timer
```

### LapsedAlert *(sink)*

```
alertId       String
memberId      String
drugClass     String
lapsedOn      LocalDate   — supply-exhaustion date that passed unrefilled
emittedAt     Long
```

Alert semantics (resolved, Phase 1 HLD — `hld.md` §3): alerts are **immutable
point-in-time facts**; the latest alert per `(memberId, drugClass)` supersedes
earlier ones. No retraction event type in v1. If a reversal leaves no
remaining coverage (so no timer would ever fire), the reversal handler emits
the corrective alert immediately in `processElement`. Binding on Phase 8.

### AdherenceState *(keyed state — ValueState)*

```
currentSupplyEndDate     LocalDate        — date supply is exhausted, all known fills
lastFillDate             LocalDate
totalDaysCovered         int              — period-agnostic coverage fact (FR-6b/c);
                                            NOT a finished PDC ratio — measurement-year
                                            slicing happens downstream
activeCoverageIntervals  List<Interval>   — required to correctly unwind reversals
alertLeadDays            int              — resolved from broadcast lookup, persisted
activeTimerTimestamp     Long | null      — defensive check target in onTimer
```

`measurementPeriodStart` (local/advisory only) is retained pending a Phase 2
LLD decision on whether it is needed in keyed state at all — authoritative
period slicing is a downstream reporting-layer concern (FR-6c).

**Startup state — deliberately not seeded.** State starts empty for every key
at go-live; no historical fills are loaded. Accepted consequence: up to ~90
days of degraded gap detection until each member's next fill naturally
populates state (`project_functional_document.md` §6).

### Broadcast state (MapState, two descriptors)

```
NDC_CLASS_DESCRIPTOR        MapState<String, DrugClassRef>
                            ndcCode → { drugClass, trackable (non-specialty,
                            non-infusion), ... }   ← frozen name; savepoints

LEAD_TIME_DESCRIPTOR        MapState<String, Integer>
                            "drugClass|channel" → alertLeadDays   ← frozen name
```

---

## Keying strategy

`keyBy(memberId, drugClass)`

- `drugClass` resolved from `ndcCode` via broadcast state before keying.
- Comorbid members (diabetes + statin) produce multiple independent keys,
  each with its own coverage timeline and timer — comorbidity is the norm
  (see scale estimate for the multiplier).
- **Resolved (Phase 1 HLD, `hld.md` §2)**: per-class keying is final for v1;
  keying is a one-way door for savepoint compatibility. Cross-class alerting,
  if ever required, is a downstream consumer of `gap-risk-alerts` keyed by
  `memberId`, not a re-key of this pipeline.

### Alert lead time is a lookup, not a constant

A fixed 5-day window is wrong in practice; appropriate lead time varies by:

- **Drug class / clinical risk** — insulin warrants a tighter window than a statin.
- **Dispensing channel** — mail-order needs shipping lead time; retail is same-day.
- **Supply cycle length** — 5 days on a 14-day cycle ≠ 5 days on a 90-day fill.
- **Member-level risk/behavior** *(v2 candidate)* — habitual late refillers.

Implemented as the `(drug_class, dispensing_channel) → alert_lead_days`
broadcast reference table. Any 5-day figure in examples is an illustrative
default, never a hardcoded constant.

---

## Pipeline topology

```
Kafka (rx-fill-events)                          Kafka (ndc-drug-class-ref)   Kafka (alert-lead-time-ref)
  └─ RxFillEventKafkaSource                            └─ broadcast ─┐              └─ broadcast ─┐
       watermark: BoundedOutOfOrderness(24h)                         │                            │
       + withIdleness(5min)  ← low-throughput source;                │                            │
         idle-source stalling is the #1 timer risk                   │                            │
       └─ ChronicClassFilterFunction (BroadcastProcessFunction) ◄────┤                            │
            │  discard if ndcCode ∉ tracked classes OR not trackable │                            │
            │  (specialty/infusion exclusion, FR-9)                  │                            │
            └─ keyBy(memberId, drugClass)                            │                            │
                 AdherenceProcessFunction                            │                            │
                 (KeyedBroadcastProcessFunction) ◄───────────────────┴────────────────────────────┘
                      │  maintains AdherenceState in ValueState (RocksDB, 400d TTL)
                      │  FILL: merge interval, delete + re-register event-time timer
                      │  REVERSAL: unwind interval, recompute, re-register or emit
                      │  onTimer: verify → emit GapRiskAlert → register lapsed timer
                      ├─ side output ──► GapRiskAlertKafkaSink   (gap-risk-alerts)
                      ├─ side output ──► LapsedAlertKafkaSink    (lapsed-alerts)
                      ├─ side output ──► PdcSnapshotKafkaSink    (pdc-snapshots)
                      └─ side output ──► DeadLetterKafkaSink     (dead-letter)
```

### Upstream filtering rationale

Acute therapies (e.g., a 7-day antibiotic, 0 refills) must never register a
timer — "no refill" is the *expected, successful* outcome. Filtering on drug
class membership (not `refillsAuthorized == 0`) is deliberate: a chronic drug
can legitimately show 0 refills mid-therapy (authorization expired, needs a
new Rx) and must still be tracked. The same lookup independently confirms
each NDC is a standard, non-specialty, non-infusion formulation — a
diabetes-classified NDC dispensed via a specialty channel still resolves to
"discard" at this stage (FR-9).

---

## Event handling

### `processElement` — FILL

1. Read current `AdherenceState`.
2. Compute the fill's coverage interval: `[fillDate, fillDate + daySupply]`.
3. Merge against `activeCoverageIntervals`: if the new interval starts before
   existing coverage ends, only the non-overlapping portion adds to
   `totalDaysCovered` (prevents double-counting early refills).
4. Update `currentSupplyEndDate`; append to `activeCoverageIntervals`.
5. `ctx.timerService().deleteEventTimeTimer(activeTimerTimestamp)` if present.
6. Resolve `alertLeadDays` from broadcast state, keyed on
   `(drugClass, dispensingChannel)`; persist in state.
7. `registerEventTimeTimer(currentSupplyEndDate - alertLeadDays)`; persist the
   new timestamp in state.

### `onTimer`

1. Defensive check: firing timestamp matches `activeTimerTimestamp` in state.
2. Verify `currentSupplyEndDate` is still in the future relative to the timer.
3. Emit `GapRiskAlert` to the gap-risk side output.
4. Register the second, harder timer at the actual exhaustion date for the
   `LapsedAlert` (committed output — `hld.md` §1.4, §8.2).

**Known limitation — false positives from legitimate discontinuation.** The
alert fires whenever no fill arrives by the deadline, regardless of *why*.
Event data has no "prescriber discontinued this therapy" signal, so a
correctly-stopped member still triggers alerts. Accepted for v1; downstream
mitigation is outreach-side suppression (FR-3).

### `processElement` — REVERSAL

1. Locate the interval referenced by `originalClaimId` in
   `activeCoverageIntervals`; remove it.
2. Recompute `currentSupplyEndDate` and `totalDaysCovered` from the remaining
   intervals — this is why the interval *list* must be kept, not just a
   rolled-up end date.
3. Delete the currently registered timer.
4. If coverage remains, register a new timer against the recomputed end date;
   the recomputed timer emits the superseding alert.
5. If **no** coverage remains (no timer would ever fire), emit the corrective
   alert immediately in `processElement` (`hld.md` §3 — binding on Phase 8).

---

## Flink configuration

### State backend

```
backend:              EmbeddedRocksDB
incremental:          true
state TTL:            400 days idle → clear   (beyond max measurement period;
                                               bounds growth for discontinued therapy)
checkpoint storage:   file:///tmp/rx-vigilance-checkpoints   (local)
                      gs://<bucket>/rx-vigilance-ckpt        (GKE, flink-gs-fs-hadoop)
checkpoint interval:  30 000 ms
min pause between:    10 000 ms
tolerable failures:   3
```

### Watermarks

```
strategy:      BoundedOutOfOrderness(24h)      pharmacy claims arrive in
                                               day-granularity, batchy patterns
idleness:      withIdleness(5 min)             MANDATORY — at ~12–15 events/sec
                                               across partitions, an idle partition
                                               stalls watermarks and silently
                                               freezes every event-time timer
```

### Parallelism

```
default:       2   (local)
default:       4   (GKE)   — throughput-light; sized for state/timer
                             distribution, not events/sec
```

---

## Scale estimate (back-of-envelope, not production figures)

Anchor: OptumRx publicly states it serves 65M+ members.¹

| Metric | Estimate | Basis |
|---|---|---|
| Members on tracked chronic therapy | ~16–20M | ~25–30% of population on Star Ratings-tracked classes |
| Comorbidity multiplier | ~1.5–1.7x | Diabetes/statin/hypertension frequently co-occur |
| Resulting active keys/timers | ~30–34M | Members × comorbidity multiplier |
| Adherence-relevant fill events | ~1–1.2M/day | ~12 fills/member-class/year |
| Average throughput | ~12–15 events/sec | Daily volume / effective hours |
| Peak throughput | ~30–40 events/sec | 2–3x at month-start/holiday spikes |
| Per-key state size | ~200–400 bytes | Few dates, interval list, timer timestamp |
| Total live state | ~6–12 GB | Active keys × per-key size |

¹ OptumRx public FAQ. All other figures are derived estimates for capacity
planning, not disclosed production metrics.

**Design implication**: throughput-light but **timer/state-heavy**. Capacity
planning focuses on RocksDB timer-heap size and task-slot count for even key
distribution — not raw events/sec.

---

## Project structure

```
rx-vigilance/
├── pom.xml
├── Makefile
├── spec.md
├── docker-compose.yml               — local Redpanda broker + schema registry
│
├── src/main/java/com/healthcare/rxvigilance/
│   ├── AdherenceJob.java
│   ├── config/
│   │   ├── JobConfig.java               — ParameterTool + optional --config.file
│   │   └── StateBackendConfig.java
│   ├── domain/
│   │   ├── RxFillEvent.java             — record; zero Flink imports
│   │   ├── GapRiskAlert.java
│   │   ├── LapsedAlert.java
│   │   ├── PdcSnapshot.java
│   │   ├── AdherenceState.java
│   │   ├── CoverageInterval.java
│   │   ├── DrugClassRef.java
│   │   └── EventType.java / Channel.java
│   ├── pipeline/
│   │   ├── source/RxFillEventKafkaSource.java
│   │   ├── source/ReferenceDataSources.java     — both broadcast sources
│   │   ├── sink/AlertKafkaSinks.java
│   │   ├── operators/
│   │   │   ├── ChronicClassFilterFunction.java  — pre-keyBy broadcast filter
│   │   │   └── AdherenceProcessFunction.java    — KeyedBroadcastProcessFunction
│   │   └── coverage/
│   │       └── IntervalMerger.java              — pure functions: merge, unwind,
│   │                                              recompute; no Flink imports
│   ├── serialization/
│   │   ├── RxFillEventAvroDeserializer.java
│   │   └── AlertAvroSerializers.java
│   ├── watermark/
│   │   └── RxFillWatermarkStrategy.java         — bounded OOO + idleness
│   └── metrics/
│       └── AdherenceMetricsReporter.java        — alert counts, filter drop rate
│
├── src/main/resources/
│   ├── application-local.properties
│   ├── application-gke.properties
│   ├── rx-fill-event.avsc
│   ├── gap-risk-alert.avsc
│   └── lapsed-alert.avsc
│
└── src/test/java/com/healthcare/rxvigilance/
    ├── coverage/IntervalMergerTest.java          — pure-logic edge cases first
    ├── operators/AdherenceProcessFunctionTest.java
    ├── operators/AdherenceTimerTest.java         — event-time advancement tests
    └── AdherencePipelineIT.java
```

`IntervalMerger` is deliberately a pure, Flink-free class: the
interval-merge and reversal-unwind logic is where subtle defects are most
likely, and pure functions make exhaustive edge-case testing trivial
(overlapping fills, reversal of a middle interval, out-of-order reversal,
reversal leaving zero coverage).

---

## Key dependencies

```
flink-streaming-java          1.18.x
flink-connector-kafka         3.1.x
flink-statebackend-rocksdb    1.18.x
flink-avro                    1.18.x
flink-avro-confluent-registry 1.18.x     ← schema-registry integration
avro                          1.11.x
slf4j-api                     2.0.x
log4j-slf4j2-impl             2.2x

<!-- test -->
flink-test-utils              1.18.x
junit-jupiter                 5.10.x
assertj-core                  3.25.x
testcontainers-redpanda       1.19.x     ← replaces testcontainers-kafka
```

---

## Local run

```
# 1. Start Redpanda (broker + schema registry)
docker-compose up -d

# 2. Create topics + register schemas (local mirror of the Terraform-managed cloud config)
./scripts/bootstrap-local-topics.sh

# 3. Build
mvn clean package -DskipTests

# 4. Run job locally (local RocksDB, local checkpoints)
mvn exec:java -Dexec.mainClass="com.healthcare.rxvigilance.AdherenceJob" \
  -Dexec.args="--kafka.brokers=localhost:9092 \
               --schema.registry.url=http://localhost:8081 \
               --checkpoint.dir=file:///tmp/rx-vigilance-checkpoints"

# 5. Produce test events
# Fixtures: src/test/resources/test-fill-events.json (fills, early refills,
# reversals, acute-therapy events that must be filtered)
```

---

## Testing strategy

| Layer | Class | Tool |
| --- | --- | --- |
| Pure logic | `IntervalMergerTest` | JUnit — exhaustive merge/unwind edge cases, no Flink |
| Operator unit | `AdherenceProcessFunctionTest` | `KeyedOneInputStreamOperatorTestHarness` + broadcast harness |
| Timer/event-time | `AdherenceTimerTest` | Harness with explicit watermark advancement — timer fires, cancels, defensive-check paths |
| Integration | `AdherencePipelineIT` | `MiniClusterWithClientResource` + RocksDB + Testcontainers Redpanda |

Timer tests are the highest-value tests in this project: they must *advance
event time explicitly* and assert on side-output contents — a test that
passes without watermark advancement is not exercising the design.

---

## Implementation order (for Claude Code)

Build and test one component at a time in this order:

```
1.  domain/            — records, enums, CoverageInterval; no Flink imports
2.  coverage/          — IntervalMerger pure logic + exhaustive tests FIRST
3.  config/            — JobConfig (--config.file support from day one), StateBackendConfig
4.  serialization/     — Avro (de)serializers against .avsc files
5.  watermark/         — bounded OOO + withIdleness
6.  pipeline/source/   — fill-event source + both broadcast reference sources
7.  pipeline/sink/     — four sinks
8.  operators/         — ChronicClassFilterFunction, then AdherenceProcessFunction
                         (FILL path → onTimer → REVERSAL path, tested at each step)
9.  metrics/           — AdherenceMetricsReporter
10. AdherenceJob       — wire everything together, operator UIDs on every operator
11. tests/             — remaining harness tests → MiniCluster IT
```

---

## Notes for Claude Code

- **Local environment is the default for every phase.** Do not require cloud
  connectivity to run tests; Redpanda Cloud + GKE are used only in phases
  explicitly marked as integration/deployment.
- Use **Java 17** features (records for domain objects, switch expressions).
- Domain objects and `IntervalMerger` must have **zero Flink imports**.
- All config via `ParameterTool` — **no hardcoded strings** in pipeline code;
  `alertLeadDays` is *never* a constant anywhere (broadcast lookup only).
- **Operator UIDs on every operator** for savepoint compatibility; broadcast
  `MapStateDescriptor` names are frozen — changing them breaks savepoints.
- RocksDB TTL: **400 days** on `AdherenceState`.
- Timer hygiene: every FILL/REVERSAL path must leave **at most one** registered
  timer per key, and `activeTimerTimestamp` in state must always match it.
- Watermark idleness is mandatory (see Flink configuration) — omitting
  `withIdleness` is a correctness bug here, not a tuning choice.
- Dedupe of redelivered events (open question #3): prefer idempotent
  interval-merge (`claimId` already in `activeCoverageIntervals` → no-op)
  over a separate recently-seen set, unless testing proves otherwise.

---

## Deployment

### Platform

| Component | Technology | Notes |
| --- | --- | --- |
| Cloud | Google Cloud Platform | Consistent with ClaimGuard's K8s learnings; GKE |
| Orchestration | GKE | Small node pool; RocksDB-heavy TaskManagers — mind Capacity vs Allocatable |
| Container registry | GitHub Container Registry (GHCR) | Free tier — `ghcr.io/<owner>/rx-vigilance` |
| Kafka | Redpanda Cloud | External, `SASL_SSL`; free trial window — see environments note |
| Schema registry | Redpanda built-in | Confluent-API-compatible; `FULL_TRANSITIVE` |
| Checkpoint storage | Google Cloud Storage | `flink-gs-fs-hadoop` plugin; survives cluster teardown |
| Metrics | kube-prometheus-stack (Helm) | Prometheus + Grafana + Alertmanager, self-hosted on the same cluster |
| Logs | GCP Cloud Logging | Automatic container stdout/stderr; no Loki in v1 |
| CI/CD | GitHub Actions | PR: build + test + Sonar; main: build + push + deploy |
| Code quality | SonarQube Cloud (free tier) | Quality gate enforced, PR decoration; SonarQube for IDE (IntelliJ, connected mode) |
| Infrastructure as code | Terraform (google + redpanda + helm providers) | Everything cloud-side; nothing hand-created in a console |

### Environments

- **Local (default)**: Docker Compose Redpanda + MiniCluster/test harnesses.
  All pipeline-logic phases develop and test here.
- **Cloud (integration/demo)**: GKE + Redpanda Cloud. Used (a) early, to
  prove SASL_SSL/registry/connector glue before pipeline logic exists;
  (b) periodically as a regression deploy; (c) finally for end-to-end
  validation — checkpoint/restore from GCS, savepoint + redeploy, live
  `FULL_TRANSITIVE` schema-evolution demo.
- **Cost discipline**: GKE bills while the cluster exists — `terraform
  destroy` when idle. GCS checkpoints survive teardown; dev RocksDB state
  is disposable. Redpanda topics/ACLs/schemas are Terraform-managed, so the
  Kafka layer is recreatable with a single `apply` after the trial ends.

### Architecture

```
git push to main
  └─► GitHub Actions
        ├─► mvn verify + SonarQube Cloud analysis (quality gate)
        ├─► docker build + push ─► ghcr.io/<owner>/rx-vigilance:<git-sha>
        └─► kubectl patch FlinkDeployment ─► GKE (rx-vigilance-gke)
                                               └─► Namespace: rx-vigilance
                                                     ├─► FlinkDeployment: rx-vigilance
                                                     │     JobManager  (1 replica)
                                                     │     TaskManager (RocksDB-sized memory)
                                                     ├─► PodMonitor ──► Prometheus ──► Grafana
                                                     └─► checkpoints ──► GCS bucket
                                   external ─────► Redpanda Cloud (SASL_SSL)
                                                     rx-fill-events        (source)
                                                     ndc-drug-class-ref    (broadcast)
                                                     alert-lead-time-ref   (broadcast)
                                                     gap-risk-alerts       (sink)
                                                     lapsed-alerts         (sink)
                                                     pdc-snapshots         (sink)
                                                     dead-letter           (sink)
```

### GCP resources (Terraform-managed)

| Resource | Name | Notes |
| --- | --- | --- |
| GKE cluster | `rx-vigilance-gke` | Small node pool; zonal to control cost |
| GCS bucket | `<project>-rx-vigilance-ckpt` | Checkpoints/savepoints |
| Service account + Workload Identity | `rx-vigilance-sa` | GCS access from Flink pods |

Terraform also manages, via the **Redpanda Cloud provider**: all seven topics,
users/ACLs, and schema-registry subjects/compatibility — and, via the **helm
provider**: cert-manager, the Flink Kubernetes Operator, and
kube-prometheus-stack releases.

Terraform state: GCS backend (dedicated bucket, created once by
`scripts/bootstrap-tf-state.sh`) — never in git.

### Observability (dashboard minimum panel set)

- **Watermark lag per source** — the critical panel: at this throughput,
  idle-source watermark stalling is the most likely *silent* failure mode
  for event-time timers.
- Checkpoint duration and size.
- RocksDB memory (block cache / memtables) and estimated state size.
- Records in/out per operator, including the pre-keyBy filter drop rate.
- Alert side-output emission counts (`GapRiskAlert`, `LapsedAlert`).

Alertmanager rules (minimum): checkpoint failure; watermark stall.

### Deployment file structure

```
rx-vigilance/
├── Dockerfile                        — multi-stage: maven builder → flink:1.18 runtime,
│                                       JAR in /opt/flink/usrlib/
├── infra/terraform/
│   ├── providers.tf                  — google + redpanda + helm providers, GCS state backend
│   ├── variables.tf
│   ├── gke.tf                        — cluster, node pool, workload identity
│   ├── gcs.tf                        — checkpoint bucket
│   ├── redpanda.tf                   — topics, users/ACLs, schema registry config
│   ├── helm.tf                       — cert-manager, Flink operator, kube-prometheus-stack
│   └── outputs.tf
├── k8s/
│   ├── namespace.yaml
│   └── flink/
│       ├── flink-serviceaccount.yaml
│       └── flink-deployment.yaml     — FlinkDeployment CR; image tag injected by deploy workflow
├── observability/
│   ├── podmonitor.yaml
│   ├── grafana-dashboard-adherence.json
│   └── alertmanager-rules.yaml
├── scripts/
│   ├── bootstrap-tf-state.sh         — one-time: GCS bucket for Terraform state
│   └── bootstrap-local-topics.sh     — local mirror of Terraform-managed topics/schemas
└── .github/workflows/
    ├── ci.yml                        — PR: mvn verify + Sonar quality gate + docker build (no push)
    └── deploy.yml                    — main: package → push GHCR → patch FlinkDeployment
```

---

## Resolved decisions & open questions

1. ~~Retraction semantics~~ — **Resolved (Phase 1 HLD, `hld.md` §3).**
   Supersede-by-key; alerts are immutable point-in-time facts; the pipeline
   guarantees a superseding alert whenever a reversal invalidates an emitted
   one (see Event handling — REVERSAL, step 5).
2. ~~Keying strategy revisit~~ — **Resolved (Phase 1 HLD, `hld.md` §2).**
   `keyBy(memberId, drugClass)` final for v1; one-way door for savepoints.
3. **Open**: duplicate/redelivered events — dedupe by `claimId` via a bounded
   recently-seen set, or rely on idempotent interval-merge? (Leaning
   idempotent merge — see Notes for Claude Code.)
4. ~~Measurement-period boundary handling~~ — **Resolved.** Reporting-layer
   concern; RxVigilance tracks period-agnostic coverage facts only
   (`project_functional_document.md` FR-6).
