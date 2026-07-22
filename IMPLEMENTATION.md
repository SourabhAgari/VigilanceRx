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
| 1 | Infrastructure bootstrap (Terraform) | cloud | ✅ done 2026-07-19 |
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

Structure per D8: two stacks split by lifecycle — `infra/terraform/platform/`
(Redpanda + GCS; cheap, persistent, never destroyed) and
`infra/terraform/runtime/` (GKE + Helm; disposable, the one-click target).
Epic #17; child issues #18–#23.

- [x] #18: `scripts/bootstrap-tf-state.sh` (one-time GCS state bucket) +
      `platform/` & `runtime/` providers.tf/variables.tf — google + redpanda
      + helm providers, GCS backend with prefix-separated state per stack
      — done 2026-07-17: PR #24 merged; bootstrap script idempotent
      (double-run verified); `terraform init`+`validate` green in both stacks
      vs GCS backend (google v6.50.0, redpanda v1.9.0, helm ~>3.0 pinned via
      committed lock files); project vigilancerx-502702, us-central1
- [x] #19: `runtime/gke.tf`: zonal cluster `vigilance-rx-gke` (D10), single-node
      e2-standard-4 spot pool (D7), Workload Identity; `platform/gcs.tf`:
      checkpoint bucket + SA binding; GCP budget alert (D7/D9)
      — done 2026-07-18: both stacks applied & verified — cluster RUNNING
      (us-central1-a, 1× e2-standard-4 spot, 1.35.5-gke.1241004), bucket
      `vigilancerx-502702-rx-vigilance-ckpt` exists, 5+2 resources in state
      (`terraform state list`). Gotchas hit: billing-budget API needs
      `user_project_override`+`billing_project` in the google provider (ADC
      quota project); WI pool `<project>.svc.id.goog` is created lazily by
      the FIRST WI-enabled cluster → first-ever apply order is runtime
      cluster before platform WI binding (one-time per project; document in
      #23 README)
- [x] #20: `platform/redpanda.tf`: all 7 topics, service user + ACLs,
      schema-registry subjects with `FULL_TRANSITIVE` (mirror of Phase 0
      local bootstrap)
      — done 2026-07-18: serverless cluster `rx-vigilance` (us-east-1,
      id d9dhs6gi8skvgsajf9n0, D11) + resource group, 7 topics (ref topics
      compacted, D12), user `rx-vigilance-flink` (scram-sha-256, password
      write-only — not in state), 8 least-privilege ACLs, 3 subjects
      FULL_TRANSITIVE; registry verified via curl as the flink user
      (3 subjects listed). Provider bumped ~>1.0 → ~>2.1 (D13: v1.9.0
      redpanda_schema broken vs serverless, provider issue #338); schemas
      use cloud Bearer auth; deprecated `cluster_api_url` attr retained
      knowingly (warnings accepted). rpk cloud-profile topic listing
      deferred to phase exit-criteria check
- [x] #21: `runtime/helm.tf`: cert-manager, Flink Kubernetes Operator,
      kube-prometheus-stack releases (depends_on chain per CLAUDE.md §10)
      — done 2026-07-19: helm provider wired to GKE via google_client_config
      token (no kubeconfig/static creds); pinned cert-manager v1.21.0 →
      flink-kubernetes-operator 1.15.0 (depends_on cert-manager, ns
      flink-system) + kube-prometheus-stack 87.17.0 (ns monitoring, no
      fake dep). Verified: all pods Running — cert-manager 3/3 pods,
      operator 2/2 (webhook container up = cert chain works), full
      monitoring set incl. prometheus-0 and grafana
- [x] #22: `k8s/namespace.yaml`, `k8s/flink/flink-serviceaccount.yaml`;
      Kafka credentials as Kubernetes Secret created from env vars by the
      infra-up script (never committed, never in Terraform state)
      — done 2026-07-19: namespace `rx-vigilance` Active; KSA `flink` with
      WI annotation verified against live cluster (jsonpath readback =
      `rx-vigilance-sa@...` — closes the [rx-vigilance/flink] loop from
      #19; end-to-end token-exchange proof deferred to Phase 2 SmokeJob).
      Secret `kafka-credentials` (keys `sasl-username`/`sasl-password` —
      name-frozen for Phase 2) created manually from env vars, verified
      via `kubectl describe` (keys+sizes only); procedure documented in
      `k8s/README.md` with placeholders; automation lands in #23 infra-up.
      GitOps/Argo CD proposal filed as #30 + D-open-10 (Phase 10 decision)
- [x] #23: `make infra-up` / `make infra-down` one-click wrappers (D8);
      document destroy / re-`apply` idle-cost workflow in README;
      round-trip verification
      — done 2026-07-19: Makefile targets (infra-up/-down/-verify; verify =
      kubectl wait Ready ×3 ns + WI annotation + secret keys) + sharp-edge
      docs in infra/terraform/README.md. Round-trip verified: destroy 5
      resources → cluster list empty, bucket + 3 subjects survived →
      `make infra-up` CLEAN SINGLE PASS (helm-provider §10 edge did NOT
      fire on helm provider 3.x; targeted-apply fallback documented anyway)
      → all pods Ready, secret recreated. rpk SASL topic list: 7 topics,
      rx-fill-events p=3 — first Kafka-protocol auth test of flink user

**Exit criteria — all verified 2026-07-19:**
- ✅ Fresh apply from empty runtime state, no manual steps beyond documented
  secret creation: `make infra-up` single pass (2026-07-19 round-trip)
- ✅ Operator healthy + Prometheus/Grafana running: `kubectl wait` Ready in
  flink-system (2/2) and monitoring via `make infra-verify`
- ✅ rpk lists 7 topics (SASL as flink user); registry lists 3 subjects
  (curl, basic auth)
- ✅ destroy + re-apply round-trip verified once (this session; platform
  stack untouched throughout)

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

Epic #37; child issues #38–#42 (produce-and-observe + checkpoint
verification tasks below are combined in #41).

- [x] #38 Minimal `SmokeJob`: Kafka source (`rx-fill-events`, Avro via registry)
      → log/print sink, checkpointing to GCS
      — done 2026-07-20: KafkaSource (GenericRecord, ConfluentRegistryAvroDeserializationSchema,
      earliest offsets, noWatermarks — job has no event-time logic) → LoggingSink
      (redacted INFO, full record DEBUG per §7/§9). Verified locally against
      docker-compose Redpanda: hand-produced Avro event consumed and logged
      (`type=FILL, ndc=00093-7424-56, fillDate=20654`, no memberId at INFO);
      checkpoints completing continuously (chk-1..chk-8 across runs,
      `_metadata` present). pom: added `flink-connector-base` (provided) —
      non-transitive dependency gap, invisible until a local run. CLAUDE.md
      §6 corrected: `mvn exec:java` does not work for local Flink runs
      (classloader mismatch between the exec plugin and Flink's mini-cluster
      threads) — replaced with IDE run instructions + the two argument
      gotchas (`--key value` not `--key=value`; provided-scope classpath
      checkbox)
- [x] #39 Minimal Dockerfile (multi-stage per spec) + manual image push to GHCR
      — done 2026-07-21: maven-shade-plugin added (fat jar — bundles
      compile-scope Kafka connector/Avro/registry client; provided-scope
      Flink core correctly excluded by shade's default artifact set,
      verified via `jar tf` grep before/after). Dockerfile: maven builder
      stage → `flink:1.18` runtime, jar renamed to fixed
      `/opt/flink/usrlib/rx-vigilance.jar` (version-independent for #40's
      manifest). Verified: local build, `flink --version` clean in the
      built image. Pushed `ghcr.io/sourabhagari/rx-vigilance:smoke`
      manually — package private, linked to this repo (unlocks
      GITHUB_TOKEN push/pull for Phase 10, no PAT secret needed). Fixed a
      pre-existing bug in `ci.yml`'s docker-build step (missing build
      context arg — never exercised before this task); Dockerfile added
      to `sonar.sources` (deferred from #33), no new findings. Follow-up
      for #40: private package needs a k8s imagePullSecret
- [x] #40 `k8s/flink/flink-deployment.yaml`: FlinkDeployment CR running SmokeJob
      — done 2026-07-21: verified `kubectl get flinkdeployment` → JOB STATUS
      RUNNING, LIFECYCLE STATE STABLE; JM + TM pods 1/1 Running; checkpoints
      1–7 completing every ~30s to the real GCS bucket (`Successfully
      repaired gs://vigilancerx-502702-rx-vigilance-ckpt/...` in JM log) —
      Workload Identity + GCS plugin proven end-to-end. Also added
      `k8s/flink/flink-rbac.yaml` (Role+RoleBinding — not originally scoped
      in #22, discovered here: Flink's native K8s execution mode needs its
      own K8s API RBAC, separate from Workload Identity's GCP-only scope).
      Chain of fixes along the way, each isolated and verified: SASL
      properties added to SmokeJob's KafkaSource (env-var gated, #38 had no
      cloud auth path); GHCR image was arm64-only (Apple Silicon build host
      vs GKE's amd64 nodes) → `--platform linux/amd64`; `flink:1.18` base
      image is JDK 11, our bytecode is Java 17 → switched to
      `flink:1.18-java17`; `kafka-clients` version conflict
      (3.4.0 vs Confluent's transitive 7.2.2-ccs) excluded via pom; two RBAC
      gaps (core `pods`/`configmaps`/`services`/`endpoints`, then
      `apps/deployments` for owner-reference lookups); shared top-level
      `podTemplate` corrupted the TaskManager's `kind` field on the
      operator's merge path — moved to per-role `podTemplate` under
      `jobManager`/`taskManager` (YAML anchor to avoid duplication). All
      gotchas written into `k8s/README.md`. Deployment left running for
      #41 (produce-and-observe reuses it; GCS checkpoint verification is
      effectively already evidenced above)
- [x] #41 Produce a hand-crafted Avro event to cloud `rx-fill-events`; observe it
      logged by the job on GKE
      — done 2026-07-21: two events produced via `rpk` as the new
      `rx-vigilance-test-producer` identity, both logged
      (`type=FILL, ndc=00093-7424-56, fillDate=20654`, no memberId at INFO).
      Discovered a real gap along the way: `ConfluentRegistryAvroDeserializationSchema`
      needs its own HTTP basic-auth config for the schema registry —
      completely separate from the KafkaSource's SASL properties (two
      different protocols/services, same underlying credentials). Fixed in
      `SmokeJob` via a `registryConfigs` map
      (`basic.auth.credentials.source=USER_INFO` +
      `schema.registry.basic.auth.user.info`), same env-var-gated pattern as
      the Kafka SASL branch. Also: produce attempt with the job's own
      `rx-vigilance-flink` identity correctly failed
      `TOPIC_AUTHORIZATION_FAILED` (least-privilege ACLs from #20 working
      as designed — that identity has no WRITE on its own source topic) →
      added `rx-vigilance-test-producer` (Terraform: new `redpanda_user` +
      3 WRITE ACLs on rx-fill-events/ndc-drug-class-ref/alert-lead-time-ref)
      as a dedicated test-injection identity, never used by the job itself.
      Applying that Terraform change also surfaced and cleanly resolved
      long-pending drift: the #33 Sonar fix (bucket IAM objectAdmin →
      objectUser) had been code-approved but never actually applied —
      applied now, live job unaffected (checkpoints continued normally
      through the change)
- [x] #41 Verify a checkpoint object appears in the GCS bucket
      — done 2026-07-21: already evidenced continuously since #40 and
      reconfirmed here — checkpoints 4–5 completed cleanly post-restart
      (`gs://vigilancerx-502702-rx-vigilance-ckpt/rx-vigilance-ckpt/...`),
      confirming the objectUser role change didn't break GCS access
- [x] #42 Capture all connection config into `application-gke.properties` +
      README notes (registry URLs, truststore approach, operator quirks)
      — done 2026-07-21: `src/main/resources/application-gke.properties`
      captures non-secret connection facts (brokers, SASL mechanism,
      registry URL + basic-auth mode, checkpoint dir) — credentials
      deliberately excluded, sourced from the K8s Secret at runtime instead
      (§9). Not yet consumed by code (`config/JobConfig.java` is Phase 4);
      this is the verified-working reference Phase 4/10 will load via
      `--config.file`. `k8s/README.md` "FlinkDeployment gotchas" section
      consolidates every discovery from #40/#41 (arch/JDK mismatch, K8s
      RBAC vs Workload Identity, shared-podTemplate merge bug, Kafka vs
      registry auth split, no truststore needed, test-producer identity) —
      the #40 version of this section had been drafted but never actually
      committed; folded in here so nothing was lost.

**Exit criteria — all verified 2026-07-21:**
- ✅ SmokeJob runs on GKE, consumes a cloud event end-to-end, checkpoints
  to GCS — #40/#41 (FlinkDeployment RUNNING/STABLE, two events logged,
  checkpoints continuous)
- ✅ Everything needed to reproduce this is in git (minus secrets) —
  Dockerfile, k8s manifests, Terraform, application-gke.properties,
  README docs; only the SASL/registry passwords live outside git (§9)
- [ ] `terraform destroy` cluster afterward (cost discipline) — pending,
      run once this issue's PR merges and the epic closes

---

## Phase 3 — Domain model & interval logic

**Goal**: the riskiest pure logic, exhaustively tested before any Flink code.

Epic #52; child issues #53–#55.

- [x] #53 `domain/`: `RxFillEvent`, `GapRiskAlert`, `LapsedAlert`, `PdcSnapshot`,
      `AdherenceState`, `CoverageInterval`, `DrugClassRef`, enums —
      records, zero Flink imports
      — done 2026-07-22: all 8 types as records/enums; `mvn clean compile`
      green; grep-verified zero `org.apache.flink.*` imports under
      `domain/`. Two invariant guards added at construction:
      `CoverageInterval` rejects `start.isAfter(end)`; `AdherenceState`
      defensively copies `activeCoverageIntervals` via `List.copyOf` (record
      immutability only protects the field reference, not a mutable list
      handed in). `PdcSnapshot` was spec-underspecified (topic description
      only, no field list) — resolved per D16 below.
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
| D7 | 2026-07-17 | GKE runtime: single-node zonal pool, e2-standard-4 **spot**, + GCP budget alert | GCP free-trial budget (₹28,016 / 50 days at start of Phase 1); ~13.3 GB Allocatable holds the full stack (~9.8 GB requests incl. TM RocksDB budget); spot ≈70% cheaper; preemption acceptable on a self-healing demo cluster |
| D8 | 2026-07-17 | Terraform split by lifecycle: `platform/` (Redpanda topics/ACLs/subjects + GCS, persistent) vs `runtime/` (GKE + Helm, disposable); one-click `make infra-up`/`infra-down` on runtime only; Kafka Secret created from env vars by infra-up script | `terraform destroy` must never delete topics or checkpoints (spec: checkpoints survive teardown); split makes destroy-when-idle cost discipline mechanical, not careful |
| D9 | 2026-07-18 | Budget alert amount ₹25,000 (not the full ₹28,016 trial credit) + extra 20% threshold rule (D7 amendment) | Deliberate safety margin below the trial credit; 20%/50%/80%/100% thresholds give an earlier warning ladder |
| D10 | 2026-07-18 | GKE cluster named `vigilance-rx-gke` (spec says `rx-vigilance-gke`) — accepted deviation; single-node pool kept fixed at 1 (no autoscaling to 2) | Name immutable post-create and user chose to keep it; spec left as-is, this row is the record. Autoscaling max=2 considered and rejected: a hard single node makes Capacity-vs-Allocatable sizing mistakes fail loudly (Pending pod) instead of silently doubling spend |
| D11 | 2026-07-18 | Redpanda serverless cluster itself Terraform-managed in `platform/` (`rx-vigilance`, AWS `us-east-1`) — created, not clicked | GCP-backed serverless is beta-gated; cross-cloud latency irrelevant at 12–15 ev/s; cluster in platform stack = survives runtime destroys (D8), `allow_deletion=false` |
| D12 | 2026-07-18 | `cleanup.policy=compact` on `ndc-drug-class-ref` + `alert-lead-time-ref` (cloud); backport to local bootstrap as separate issue | Broadcast state is rebuilt from the full topic on every job start; with delete-policy retention, ref records would age out and the chronic-class filter would silently discard events |
| D13 | 2026-07-18 | Redpanda provider `~> 2.1` (from `~> 1.0`); `redpanda_schema` uses cloud Bearer auth (no username/password); deprecated `cluster_api_url` attribute kept in use | v1.9.0 `redpanda_schema` can't read serverless clusters (provider issue #338, fixed v2.0.0); `password_wo` unusable at refresh time per provider warning; deprecated attr still present in 2.1.x — accepted with warnings |
| D-open-10 | — | **Proposed** (2026-07-19): Phase 10 deploy path via Argo CD GitOps (CI commits manifest, Argo reconciles) instead of spec's direct `deploy.yml` patching | User wants enterprise-pattern learning; decide at Phase 10 epic creation — capacity (D7 single node) and slim-install vs Flux to be resolved in that plan. Issue filed |
| D14 | 2026-07-20 | Sonar `sonar.coverage.exclusions` for entry-point/wiring job classes (`SmokeJob.java` now; add `AdherenceJob.java` explicitly in Phase 9 — no glob) | No unit-testable logic (builder-chain wiring only); real verification is the manual/integration run (§5). Domain/coverage/operator logic (Phase 3+) gets no such exclusion — §5's exhaustive-testing rule is unchanged there |
| D15 | 2026-07-21 | New Redpanda identity `rx-vigilance-test-producer` (WRITE-only on rx-fill-events + the two broadcast ref topics), dedicated to manual/test event injection, never used by the deployed job | `rx-vigilance-flink`'s READ-only ACL on its own source topic (#20) is correct least-privilege and must stay that way; smoke-testing needs *something* to act as the upstream producer without widening the job's own identity |
| D16 | 2026-07-22 | `PdcSnapshot` shape resolved: `memberId`, `drugClass`, `totalDaysCovered`, `currentSupplyEndDate`, `emittedAt` (long) | Spec named the sink and its purpose ("coverage-day facts and running numerator") but never gave a field list; shape derived from the matching language used for `AdherenceState.totalDaysCovered` + the other two alerts' `emittedAt` pattern; confirmed with user before writing |
