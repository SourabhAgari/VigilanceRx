# RxVigilance — IMPLEMENTATION.md

> Phase-gated implementation plan. Companion to `spec.md` (the blueprint —
> read-only source of truth) and `CLAUDE.md` (operating rules). This file is
> the **ledger**: Claude Code updates task checkboxes and phase status here as
> work completes, and records decisions in the Decision Log at the bottom.

**Rules of engagement**

- Work one phase at a time, in order. A phase is not started until the
  previous phase's **exit criteria** all pass.
- Every phase: plan → get approval → apply (plan-before-apply is enforced).
- Local environment is the default. Only phases marked **[CLOUD]** touch
  GKE / Redpanda Cloud.
- Never mark a task done without its verification step passing.
- New design decisions (anything not already resolved in `spec.md`/`hld.md`)
  go in the Decision Log with date and rationale.

**Timing constraint**: Redpanda Cloud trial has a limited window (~20 days
from 2026-07-15). Phases 1–2 are deliberately front-loaded to bank all
cloud-specific glue early; if the trial lapses mid-project, only Phase 12
requires restoring it (one `terraform apply`).

---

## Status

| Phase | Name | Env | Status |
|---|---|---|---|
| 0 | Repo scaffolding & local environment | local | ✅ done 2026-07-16 |
| 1 | Infrastructure bootstrap (Terraform) | cloud | ☐ not started |
| 2 | Cloud connectivity smoke test | cloud | ☐ not started |
| 3 | Domain model & interval logic | local | ☐ not started |
| 4 | Config, serialization, watermarks | local | ☐ not started |
| 5 | Sources & sinks | local | ☐ not started |
| 6 | Upstream broadcast filter | local | ☐ not started |
| 7 | Adherence core — FILL path & timers | local | ☐ not started |
| 8 | onTimer, LapsedAlert & REVERSAL path | local | ☐ not started |
| 9 | Metrics, job wiring & integration test | local | ☐ not started |
| 10 | Containerization & CI/CD deploy path | cloud | ☐ not started |
| 11 | Observability | cloud | ☐ not started |
| 12 | End-to-end cloud validation & docs | cloud | ☐ not started |

Status values: ☐ not started · ◐ in progress · ✅ done · ⏸ blocked (note why)

---

## Phase 0 — Repo scaffolding & local environment

**Goal**: a repo where `docker-compose up` gives a working local Redpanda and
`mvn verify` runs (empty) tests, with CI and Sonar wired from commit one.

- [x] Initialize repo: `pom.xml` (coordinates per spec, Java 17, dependency
      versions per spec "Key dependencies"), `.gitignore`, `Makefile`
      — done 2026-07-16: issue #2 / PR #8 merged; `make build` → BUILD SUCCESS
      (0 tests, surefire+failsafe wired); `target/`+`.idea/` untracked verified.
      Avro pinned 1.11.4 (CVE-2024-47561 fix, within spec 1.11.x).
- [x] `docker-compose.yml`: Redpanda broker + schema registry (single node)
      — done 2026-07-16: issue #3; container healthy, `rpk cluster info` shows
      broker at localhost:9092, registry `/subjects` → `[]` (v24.1.7 pinned)
- [x] `scripts/bootstrap-local-topics.sh`: create all 7 topics, register
      `.avsc` schemas with `FULL_TRANSITIVE` compatibility
      — done 2026-07-16: issue #4; idempotent (double-run verified), health-gated;
      `rpk topic list` → 7 topics (rx-fill-events p=3), 3 subjects FULL_TRANSITIVE
- [x] Avro schemas: `rx-fill-event.avsc`, `gap-risk-alert.avsc`,
      `lapsed-alert.avsc` (fields per spec Domain model)
      — done 2026-07-16: issue #4; all fields per spec, registry accepted all 3;
      Channel enum default UNKNOWN, EventType no default (dead-letter by design)
- [x] `.github/workflows/ci.yml`: `mvn verify` + docker build (no push)
      — done 2026-07-16: issue #5 / PR #11; green on PR (42s) and main push (31s),
      Maven cache keyed on pom hash; docker step gated on Dockerfile existence
      (activates in Phase 2)
- [x] SonarQube Cloud: import repo, enable PR decoration, add analysis step
      to `ci.yml`; install SonarQube for IDE (IntelliJ, connected mode)
      — done 2026-07-16: issue #6 / PR #12; CI-based analysis (Automatic
      Analysis off), `mvn sonar:sonar` via SONAR_TOKEN secret, checkout
      fetch-depth 0; jacoco 0.8.12 wired for coverage; SonarCloud Code
      Analysis check green + PR decoration on #12; IntelliJ connected mode
      bound to SourabhAgari_VigilanceRx, verified via planted-finding
      round trip (S1135)
- [x] `CLAUDE.md`: operating rules (plan-before-apply, local-default,
      invariants from spec "Notes for Claude Code", doc pointers)
      — done 2026-07-16: committed in ec57caa with the spec docs; reviewed
      against task scope — covers doc hierarchy, plan-before-apply +
      explain-first workflow, local-default environments, all spec
      invariants (§4), testing/git/security standards

**Exit criteria**
- [x] `docker-compose up -d && ./scripts/bootstrap-local-topics.sh` succeeds;
  `rpk topic list` shows all 7 topics; schema registry lists 3 subjects
  — confirmed 2026-07-16 (re-verified at phase close; evidence in epic #1)
- [x] CI green on a trivial PR; Sonar quality gate reports on the PR
  — confirmed 2026-07-16: PR #12 build + SonarCloud Code Analysis checks
  green, quality gate decoration on the PR

Phase closed 2026-07-16 — epic #1 closed with all children (#2–#7) done.

---

## Phase 1 — Infrastructure bootstrap (Terraform) **[CLOUD]**

**Goal**: all cloud infrastructure exists via `terraform apply` and nothing
else. Burn trial time on config, not clicks.

- [ ] `scripts/bootstrap-tf-state.sh`: one-time GCS bucket for Terraform state
- [ ] `infra/terraform/providers.tf`: google + redpanda + helm providers,
      GCS state backend
- [ ] `gke.tf`: zonal cluster `rx-vigilance-gke`, small node pool,
      Workload Identity; `gcs.tf`: checkpoint bucket + SA binding
- [ ] `redpanda.tf`: all 7 topics, service user + ACLs, schema-registry
      subjects with `FULL_TRANSITIVE` (mirror of Phase 0 local bootstrap)
- [ ] `helm.tf`: cert-manager, Flink Kubernetes Operator,
      kube-prometheus-stack releases
- [ ] `k8s/namespace.yaml`, `k8s/flink/flink-serviceaccount.yaml`
- [ ] Kafka credentials as Kubernetes Secret (manual creation documented;
      never committed)
- [ ] Document `terraform destroy` / re-`apply` idle-cost workflow in README

**Exit criteria**
- Fresh `terraform apply` from empty state completes without manual steps
  (except documented secret creation)
- `kubectl get pods -n flink-system` shows operator healthy;
  Prometheus/Grafana pods running
- `rpk` (cloud profile) lists the 7 topics; registry shows subjects
- `terraform destroy` + re-`apply` round-trip verified once

---

## Phase 2 — Cloud connectivity smoke test **[CLOUD]**

**Goal**: prove the entire glue path — SASL_SSL, schema registry, GCS
checkpoints, operator deployment — before any pipeline logic exists.

- [ ] Minimal `SmokeJob`: Kafka source (`rx-fill-events`, Avro via registry)
      → log/print sink, checkpointing to GCS
- [ ] Minimal Dockerfile (multi-stage per spec) + manual image push to GHCR
- [ ] `k8s/flink/flink-deployment.yaml`: FlinkDeployment CR running SmokeJob
- [ ] Produce a hand-crafted Avro event to cloud `rx-fill-events`; observe it
      logged by the job on GKE
- [ ] Verify a checkpoint object appears in the GCS bucket
- [ ] Capture all connection config into `application-gke.properties` +
      README notes (registry URLs, truststore approach, operator quirks)

**Exit criteria**
- SmokeJob runs on GKE, consumes a cloud event end-to-end, checkpoints to GCS
- Everything needed to reproduce this is in git (minus secrets)
- `terraform destroy` cluster afterward (cost discipline) — recorded here

---

## Phase 3 — Domain model & interval logic

**Goal**: the riskiest pure logic, exhaustively tested before any Flink code.

- [ ] `domain/`: `RxFillEvent`, `GapRiskAlert`, `LapsedAlert`, `PdcSnapshot`,
      `AdherenceState`, `CoverageInterval`, `DrugClassRef`, enums —
      records, zero Flink imports
- [ ] `coverage/IntervalMerger`: pure functions — `merge(fill)`,
      `unwind(originalClaimId)`, `recompute()` returning
      `(currentSupplyEndDate, totalDaysCovered)`
- [ ] `IntervalMergerTest` — edge cases (each a named test):
  - [ ] non-overlapping fill appends cleanly
  - [ ] early refill: only non-overlapping days add to `totalDaysCovered`
  - [ ] fill fully inside existing coverage adds zero days
  - [ ] reversal of the latest fill shrinks end date
  - [ ] reversal of a *middle* interval recomputes correctly
  - [ ] reversal referencing unknown `claimId` is a safe no-op (logged)
  - [ ] reversal leaving zero coverage returns empty state signal
  - [ ] duplicate `claimId` fill is an idempotent no-op (Decision D-open-3)
  - [ ] out-of-order fill (older `fillDate` after newer) merges correctly

**Exit criteria**
- 100% branch coverage on `IntervalMerger` (verify via jacoco); Sonar gate green

---

## Phase 4 — Config, serialization, watermarks

- [ ] `config/JobConfig`: `ParameterTool` + optional `--config.file` merge
      (file < CLI precedence), `StateBackendConfig` (RocksDB incremental,
      400-day TTL constant defined once)
- [ ] `serialization/`: Avro (de)serializers against registry
      (`flink-avro-confluent-registry`); dead-letter path for
      undeserializable events
- [ ] `watermark/RxFillWatermarkStrategy`: BoundedOutOfOrderness(24h)
      **+ withIdleness(5min)** — spec marks idleness mandatory
- [ ] Unit tests: config precedence; serializer round-trip; deserializer
      failure → dead-letter signal

**Exit criteria**: tests green; no hardcoded config strings (Sonar rule spot-check)

---

## Phase 5 — Sources & sinks

- [ ] `RxFillEventKafkaSource` (watermark strategy applied at source)
- [ ] `ReferenceDataSources`: both broadcast sources
      (`ndc-drug-class-ref`, `alert-lead-time-ref`)
- [ ] `AlertKafkaSinks`: 4 sinks, exactly-once, operator UIDs
- [ ] Testcontainers-Redpanda test: produce → source → collect; sink →
      consume round-trip

**Exit criteria**: container tests green locally

---

## Phase 6 — Upstream broadcast filter

- [ ] `ChronicClassFilterFunction` (`BroadcastProcessFunction`): discard if
      NDC not in tracked classes **or** not trackable
      (specialty/infusion, FR-9); forward enriched event (with resolved
      `drugClass`) otherwise
- [ ] Buffering decision for events arriving before first reference broadcast
      → record in Decision Log
- [ ] Harness tests: acute drug discarded; chronic + 0-refills kept;
      diabetes-classed specialty NDC discarded; drop-rate metric increments

**Exit criteria**: harness tests green; filter drop counter exposed as metric

---

## Phase 7 — Adherence core — FILL path & timers

**Goal**: `AdherenceProcessFunction` FILL handling exactly per spec
"Event handling — FILL" (7 steps).

- [ ] `KeyedBroadcastProcessFunction` skeleton, keyed
      `(memberId, drugClass)`; `AdherenceState` ValueState + TTL
- [ ] FILL path: IntervalMerger delegation, delete-then-register timer,
      `alertLeadDays` broadcast lookup persisted, `activeTimerTimestamp`
      persisted
- [ ] Missing lead-time lookup entry → default + warn metric (Decision Log)
- [ ] `AdherenceTimerTest` (event-time advancement, explicit watermarks):
  - [ ] single fill registers timer at `endDate - leadDays`
  - [ ] refill before threshold cancels & re-registers (exactly one timer)
  - [ ] lead time resolved per `(class, channel)`, not a constant
  - [ ] PDC snapshot emitted on fill

**Exit criteria**: timer invariant holds in every test — at most one
registered timer per key, state timestamp matches it

---

## Phase 8 — onTimer, LapsedAlert & REVERSAL path

**Goal**: the alert contract, including the **binding correction guarantee**
(`hld.md` §3 / spec "Event handling — REVERSAL" step 5).

- [ ] `onTimer`: defensive timestamp check → `GapRiskAlert` side output →
      register lapsed timer at exhaustion date
- [ ] Lapsed timer fires → `LapsedAlert`
- [ ] REVERSAL path: unwind via IntervalMerger, recompute, delete timer,
      re-register if coverage remains; if **no** coverage remains, emit
      corrective alert immediately in `processElement`
- [ ] Harness tests:
  - [ ] no refill → GapRiskAlert then LapsedAlert, in event-time order
  - [ ] stale timer (timestamp mismatch) fires as no-op
  - [ ] reversal shrinking coverage → superseding alert from recomputed timer
  - [ ] reversal to zero coverage → immediate corrective alert, no timer left
  - [ ] reversal after GapRiskAlert already emitted → supersede semantics hold

**Exit criteria**: every test asserts side-output *contents*, not just
counts; correction guarantee covered explicitly

---

## Phase 9 — Metrics, job wiring & integration test

- [ ] `AdherenceMetricsReporter`: alert emission counters, filter drop rate,
      lead-time-default-used counter
- [ ] `AdherenceJob`: full topology wiring, **operator UIDs on every
      operator**, side outputs → sinks
- [ ] `AdherencePipelineIT` (MiniCluster + RocksDB + Testcontainers
      Redpanda): fixture stream covering fill → early refill → reversal →
      gap → lapse; asserts on all four output topics
- [ ] Local run instructions verified exactly as written in spec "Local run"

**Exit criteria**: IT green; job runs locally end-to-end from
`docker-compose up` through alerts visible in `gap-risk-alerts`

---

## Phase 10 — Containerization & CI/CD deploy path **[CLOUD]**

- [ ] Finalize Dockerfile (dependency-cached multi-stage per spec)
- [ ] `.github/workflows/deploy.yml`: main-branch push → package → GHCR push
      → patch FlinkDeployment image tag
- [ ] Re-`apply` Terraform if cluster was destroyed; deploy real job via CI
- [ ] Verify job healthy on GKE against cloud Redpanda (reuse Phase 2 config)

**Exit criteria**: a merged PR reaches GKE with no manual steps beyond
approval; job stable through one checkpoint cycle

---

## Phase 11 — Observability **[CLOUD]**

- [ ] Flink Prometheus reporter enabled in FlinkDeployment;
      `observability/podmonitor.yaml` scraping JM + TMs
- [ ] `grafana-dashboard-adherence.json` — spec minimum panel set:
      watermark lag per source (the critical panel), checkpoint
      duration/size, RocksDB memory/state size, records in/out + filter
      drop rate, alert emission counts
- [ ] `alertmanager-rules.yaml`: checkpoint failure; watermark stall
- [ ] Induce a watermark stall (pause producer) and confirm the panel and
      alert both catch it

**Exit criteria**: dashboard renders live data; the induced-stall drill
fires the Alertmanager rule

---

## Phase 12 — End-to-end cloud validation & docs **[CLOUD]**

- [ ] Sustained soak: replay a multi-day synthetic event file (time-compressed)
      through cloud topics; verify alert ordering and PDC snapshots
- [ ] Kill a TaskManager mid-run → checkpoint restore verified, no lost or
      duplicated alerts (exactly-once claim exercised)
- [ ] Savepoint → redeploy new image → resume from savepoint
- [ ] Schema evolution demo: add optional field to `rx-fill-event.avsc`,
      redeploy producer, confirm `FULL_TRANSITIVE` acceptance and old-path
      compatibility
- [ ] README: architecture diagram, run instructions, dashboard screenshots,
      trial-restore instructions (`terraform apply` recreates Kafka layer)
- [ ] Final `terraform destroy`; confirm GCS checkpoints/savepoints survive

**Exit criteria**: all validation evidence (logs/screenshots) linked from
README; repo reproducible from clean clone + documented secrets

---

## Decision Log

| ID | Date | Decision | Rationale |
|---|---|---|---|
| D1 | 2026-07-05 | Redpanda Cloud as Kafka platform | Kafka-compatible, built-in registry, free trial (pre-existing, from spec) |
| D2 | 2026-07-15 | Phases 1–2 front-loaded before pipeline logic | Bank all cloud glue inside the ~20-day trial window |
| D3 | 2026-07-15 | `IntervalMerger` as pure Flink-free class, built & tested first | Highest-defect-risk logic; exhaustive plain-JUnit edge cases |
| D-open-3 | — | Dedupe strategy: leaning idempotent interval-merge over recently-seen set | Confirm or reverse during Phase 3 testing (spec open question #3) |
| D4 | 2026-07-16 | `quantity` Avro type: `decimal(precision=10, scale=2)` | NCPDP billing quantities are 2-decimal; precision/scale frozen once registered under FULL_TRANSITIVE |
| D5 | 2026-07-16 | Avro namespace `com.healthcare.rxvigilance.avro` (separate from domain package) | Avoid collision between Avro-generated classes and hand-written Flink-free domain records (Phase 3) |
| D6 | 2026-07-16 | Local partitions: `rx-fill-events`=3, all other topics=1, r=1 | Multi-partition source reproduces idle-partition watermark behavior locally (§4 idleness invariant testable) |
