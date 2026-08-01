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
| 3 | Domain model & interval logic | local | ✅ done 2026-07-23 |
| 4 | Config, serialization, watermarks | local | ✅ done 2026-07-25 |
| 5 | Sources & sinks | local | ✅ done 2026-07-29 |
| 6 | Upstream broadcast filter | local | ✅ done 2026-08-01 |
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
      only, no field list) — resolved per D16 below. `CoverageIntervalTest`
      + `AdherenceStateTest` added to cover the two invariant guards (100%
      instructions/branches on both classes per JaCoCo); Sonar coverage gate
      extended per D17 for the 7 zero-logic records. Sonar quality gate
      green at 100% coverage on new code.
- [x] #54 `coverage/IntervalMerger`: pure functions — `merge(fill)`,
      `unwind(originalClaimId)`, `recompute()` returning
      `(currentSupplyEndDate, totalDaysCovered)`
      — done 2026-07-23: `recompute()` is the shared algorithm both
      `merge()`/`unwind()` delegate to (single source of truth for the
      overlap/gap merge math, so FILL and REVERSAL paths can never quietly
      disagree). `merge()` includes a duplicate-`claimId` idempotency guard
      (D-open-3's resolution: dedup check lives inside `merge()` against
      `activeCoverageIntervals`, not a separate recently-seen set).
      `alertLeadDays`/`activeTimerTimestamp` are always passed through
      unchanged — timer/lookup logic is Phase 7's `AdherenceProcessFunction`,
      not this class's concern. Two real bugs caught and fixed during
      implementation/testing (not just style): `recompute()`'s gap-handling
      branch was originally nested inside the overlap check, silently
      skipping genuine gaps entirely (caught by the first merge test);
      the duplicate-`claimId` guard logged but didn't actually `return`,
      so it never really no-opped (caught by the dedicated idempotency
      test) — both are exactly why §5 treats the edge-case suite as
      the gate, not a formality.
- [x] #55 `IntervalMergerTest` — edge cases (each a named test):
  - [x] non-overlapping fill appends cleanly
  - [x] early refill: only non-overlapping days add to `totalDaysCovered`
  - [x] fill fully inside existing coverage adds zero days
  - [x] reversal of the latest fill shrinks end date
  - [x] reversal of a *middle* interval recomputes correctly
  - [x] reversal referencing unknown `claimId` is a safe no-op (logged)
  - [x] reversal leaving zero coverage returns empty state signal
  - [x] duplicate `claimId` fill is an idempotent no-op (Decision D-open-3)
  - [x] out-of-order fill (older `fillDate` after newer) merges correctly
      — done 2026-07-23: all 9 named cases green. Found and closed one more
      coverage gap beyond the 9: the out-of-order test's starting state had
      `lastFillDate=null`, which accidentally avoided exercising the
      "keep existing lastFillDate" branch of `merge()`'s compound
      null-or-not-later condition — fixed by giving that test a realistic
      non-null starting `lastFillDate` plus an assertion it doesn't move.

**Exit criteria — verified 2026-07-23:**
- ✅ 100% branch coverage on `IntervalMerger` (jacoco: `recompute`, `unwind`,
  both lambdas already 100%; `merge` reached 100% after the lastFillDate
  test fix); Sonar gate green

---

## Phase 4 — Config, serialization, watermarks

Epic #59; child issues #60–#62.

- [x] #60 `config/JobConfig`: `ParameterTool` + optional `--config.file` merge
      (file < CLI precedence), `StateBackendConfig` (RocksDB incremental,
      400-day TTL constant defined once)
      — done 2026-07-23: modular composition (D18) — `JobConfig` composes
      three typed sub-config records (`KafkaConnectionConfig`,
      `CheckpointConfig`, `StateBackEndConfig`) rather than one flat class
      of unrelated getters; each validates its own invariants in a compact
      constructor (construction fails fast, no separate `validate()` to
      remember). Three-tier precedence: classpath `application-{profile}
      .properties` < `--config.file` < CLI args. State TTL made
      configurable with a 400-day default rather than a frozen constant
      (D19 — reversed mid-implementation on user's correctly-raised
      concern that a hardcoded value forces a redeploy to tune). Two real
      test gaps caught by JaCoCo the same way as #53/#54: compound
      null-vs-blank conditions in `KafkaConnectionConfig`'s guard needed
      separate tests per branch; `StateBackEndConfig.toStateTtlConfig()`/
      `configureRocksDbBackEnd()` and `JobConfig.getStateBackEndConfig()`
      had never actually been called by any test despite the class
      compiling and other tests passing. 100% branch coverage on all
      four classes; Sonar gate green.
- [x] #61 `serialization/`: Avro (de)serializers against registry
      (`flink-avro-confluent-registry`); dead-letter path for
      undeserializable events
      — done 2026-07-24: `RxFillEventAvroDeserializer` wraps Confluent's
      `KafkaAvroDeserializer` directly (not Flink's registry wrapper, which
      can't accept an injected test client) — production uses a real
      `CachedSchemaRegistryClient`, tests use `MockSchemaRegistryClient`,
      same class, zero network calls in tests. `DeserializationResult`
      wraps success/failure so the deserializer never throws for bad
      input — malformed magic byte routes to failure, not a crash.
      GenericRecord → RxFillEvent mapping uses Avro's own tested
      `Conversions.DecimalConversion` for the decimal field rather than
      hand-rolled byte decoding. Exception handling narrowed to the
      specific types that represent genuinely bad external data
      (`SerializationException`/`ClassCastException`/
      `IllegalArgumentException`/`NullPointerException`), not a blanket
      `catch (Exception)` that would also mask our own bugs as dead-letter
      events. Round-trip tests cover both FILL (`originalClaimId` null)
      and REVERSAL (`originalClaimId` populated) cases; 100% branch
      coverage; Sonar gate green. D20 records two design detours
      (generic Strategy-pattern engine, then a lightweight interface)
      that were built, discussed, and deliberately reverted — kept for
      the reasoning, not the code.
- [x] #62 `watermark/RxFillWatermarkStrategy`: BoundedOutOfOrderness(24h)
      **+ withIdleness(5min)** — spec marks idleness mandatory
      — done 2026-07-25: durations sourced from a new `WatermarkConfig`
      record (`config/` package, same compact-constructor-validation +
      `fromParams` pattern as the other three sub-configs), not literals
      on the strategy class — caught mid-implementation on user's "nothing
      must be hardcoded" instruction (D21). Default idleness constant was
      initially miscoded as 5 *hours* (`Duration.ofHours(5)`); caught by
      writing the defaults test before fixing the constant, per the
      test-first pattern established in this project — corrected to
      `Duration.ofMinutes(5)` per the CLAUDE.md §4 invariant. Watermark
      generator/timestamp-assigner behavior verified by extracting and
      driving `TimestampAssigner`/`WatermarkGenerator` directly (Flink's
      strategy object has no observable behavior until its two products
      are driven by hand) against a hand-written `RecordingWatermarkOutput`
      test double: UTC-midnight timestamp mapping, bounded-out-of-orderness
      trailing the max-seen timestamp (not last-seen) by exactly
      `outOfOrderness` (confirmed via bytecode: Flink's
      `BoundedOutOfOrdernessWatermarks` emits `maxTimestamp -
      outOfOrderness - 1`, not a round value), and idleness marking
      (confirmed via bytecode that `WatermarksWithIdleness`'s inactivity
      clock starts on the *first* `checkIfIdle()` call, not at generator
      creation — test needs two `onPeriodicEmit` calls with a sleep
      between them, not a sleep before a single call). 100% branch
      coverage on `WatermarkConfig` and `RxFillWatermarkStrategy`;
      `mvn clean verify` green (21 classes analyzed). Sonar gate to be
      confirmed on PR CI (§8).
- [x] Unit tests: config precedence; serializer round-trip; deserializer
      failure → dead-letter signal
      — done 2026-07-25: config precedence covered by `JobConfigTest`
      (#60: CLI-over-file override, classpath profile load, unknown-profile
      fallback); serializer round-trip + dead-letter covered by
      `RxFillEventAvroDeserializerTest` (#61); watermark strategy covered
      by `RxFillWatermarkStrategyTest` (#62, above) — this line closes out
      across all three Phase 4 issues.

**Exit criteria**: tests green (confirmed — `mvn clean verify` BUILD
SUCCESS); no hardcoded config strings — all Phase 4 thresholds
(Kafka/checkpoint/state-TTL/watermark) now sourced via `ParameterTool`
through typed config records, none left as literals in pipeline code;
Sonar rule spot-check to be confirmed on PR CI, not blocking this ledger
update per established practice (#60/#61 verification gate is
`mvn clean verify`, Sonar is the PR-merge gate per §8)

---

## Phase 5 — Sources & sinks

- [x] #68 `RxFillEventKafkaSource` (watermark strategy applied at source)
      — done 2026-07-26, reworked 2026-07-26 (see #69 below): originally built
      as its own concrete `KafkaSource<DeserializationResult>` +
      package-private `DeadLetterSplitFunction` + hand-written
      `RxFillEventKryoSerializer`/`DeserializationResultKryoSerializer`.
      While building #69, that shape was reopened and unified onto the same
      reusable `KafkaTypedSourceBuilder<T>` all three sources now use (D23) —
      `RxFillEventSource.build()` is now a ~15-line configuration of the
      shared builder (topic, `RxFillEventAvroMapper`, dead-letter tag), not a
      bespoke pipeline. The original watermark-ordering reasoning still
      holds and is unchanged: the builder's raw source output
      (`KafkaSourceResult<T>`) can't carry a `WatermarkStrategy<RxFillEvent>`
      and dead-letter records have no `fillDate` to watermark on, so
      `RxFillEventSource` calls `.assignTimestampsAndWatermarks(...)` itself,
      once, immediately after the shared builder returns — no shuffle/keyBy
      happens in between, so this remains operationally identical to
      applying watermarks directly at the source. `KafkaConnectionConfig`
      (#60) extended with `securityProtocol`/`saslMechanism` fields that
      `application-gke.properties` already defined but nothing previously
      read.
      This task is also where the Kryo/records incompatibility was first
      discovered: Flink 1.18's bundled Kryo (2.24.0) cannot serialize Java
      records at all — `Unsafe.objectFieldOffset()` throws on any record
      field, and `TypeExtractor` doesn't recognize records as POJOs. First
      resolved via D22 (hand-written per-type serializers); superseded by
      D24 (generic reflection-based serializer) once #69 needed the same
      treatment for two more record types. Also fixed mid-task: Sonar's own
      JRE auto-provisioning network flakiness (unrelated to this task's
      code); Surefire's `--add-opens=java.base/java.util` had to be wired
      via JaCoCo's late-bound `@{jacocoArgLine}` property, not `${argLine}`.
- [x] #69 `ReferenceDataSources`: both broadcast sources
      (`ndc-drug-class-ref`, `alert-lead-time-ref`)
      — done 2026-07-26: built as a from-scratch SOLID/OCP redesign (D23)
      spanning all three Kafka sources in this phase, not just the two
      reference topics — user explicitly asked for an enterprise-grade,
      closed-for-modification architecture partway through, which reopened
      #68's already-merged code (see above). Final shape, three layers:
      domain (`DrugClassRef`, `DrugClassRefUpdate`, `AlertLeadTimeUpdate` —
      untouched by any of this); generic infrastructure, closed for
      modification (`AvroValueMapper<T>` Strategy interface,
      `AvroKeyValueDeSerializer<T>`, `TypedKafkaRecordDeserialisationSchema<T>`,
      `KafkaSourceResult<T>`, `DeadLetterSplitFunction<T>`,
      `RecordKryoSerializer`, `KafkaTypedSourceBuilder<T>`); per-topic
      definitions, the only layer that grows (`RxFillEventAvroMapper`,
      `DrugClassRefMapper`, `AlertLeadTimeMapper`, and the three thin
      `*Source`/`*KafkaSource` classes). Adding a future topic needs two new
      files and zero changes to the shared layer — verified concretely by
      walking through a hypothetical 4th topic during design.
      `ndc-drug-class-ref`/`alert-lead-time-ref` use Avro (not JSON — user's
      explicit call, consistent with `rx-fill-events`), with the Kafka
      message *key* carrying the lookup key (`ndcCode`, or the composite
      `"drugClass|channel"` string matching `LEAD_TIME_DESCRIPTOR`'s MapState
      key format exactly) and only the looked-up value Avro-encoded — this
      matters for `cleanup.policy=compact` (D12): compaction dedupes by
      Kafka key, so the key must be the real lookup key, not embedded in the
      Avro value. New `.avsc` schemas (`drug-class-ref.avsc`,
      `alert-lead-time-ref.avsc`) registered in `bootstrap-local-topics.sh`.
      Two real bugs caught and fixed along the way: `KafkaAvroDeserializer
      .deserialize(key, bytes)` was passing the Kafka message key into the
      *topic* parameter slot (should be `null` — schema ID is
      self-describing, no topic needed); `OutputTag<>("id")` without the
      anonymous-subclass braces silently loses its generic type info
      (`getClass()` can't recover `T` without the synthetic subclass),
      breaking side-output routing the same way `.process()`'s output type
      needs `.returns(...)` once `DeadLetterSplitFunction` became generic.
      D24 records the Kryo generalization. 100% branch coverage across all
      three layers; `mvn clean verify` green (40 classes analyzed).
- [x] #70 `AlertKafkaSinks`: 4 sinks, exactly-once, operator UIDs
      — done 2026-07-27: same layered, closed-for-modification approach as
      #69's source rebuild (D23), applied to the write side. Shared/generic
      layer: `AvroValueSerializer<T>` (Strategy interface, the write-side
      mirror of `AvroValueMapper<T>`), `AvroRecordSerializer<T>` (owns
      `KafkaAvroSerializer`, delegates to the strategy), `TypedAvroSerializationSchema<T>`
      (Flink `SerializationSchema<T>` adapter), `KafkaTypedSinkBuilder<T>`
      (Builder wrapping `KafkaSink.<T>builder()` + Flink's own
      `KafkaRecordSerializationSchema.builder()`, `DeliveryGuarantee
      .EXACTLY_ONCE` + `"rx-vigilance-" + topic` transactional ID, defensive
      `RecordKryoSerializer` registration for `T`). Per-topic layer:
      `GapRiskAlertAvroSerializer`/`LapsedAlertAvroSerializer`/
      `PdcSnapshotAvroSerializer` (one per alert type) plus `AlertKafkaSinks`
      itself — one class, three thin factory methods over the shared
      builder, matching spec.md's explicit `sink/AlertKafkaSinks.java`
      naming (not four separate classes the way sources were split — spec
      names this one file, unlike the source side where per-topic splitting
      was this project's own OCP proposal, not spec-mandated).
      `dead-letter` is genuinely structurally different, not just a fourth
      `T`: on failure, `KafkaSourceResult<T>.value()` is always null
      regardless of `T`, so a new non-generic `DeadLetterRecord(byte[]
      rawBytes, String errorMessage)` bridges all three sources' distinct
      failure types into one shape. Re-encoding already-undecodable bytes
      as Avro would be counterproductive, so this sink bypasses
      `KafkaTypedSinkBuilder` entirely — raw bytes as the Kafka value
      (`DeadLetterRecord::rawBytes`), `errorMessage` as a Kafka record
      header via `HeaderProvider`, using Flink's own
      `KafkaRecordSerializationSchema.builder()` directly.
      New `pdc-snapshot.avsc` created (didn't exist yet — `GapRiskAlert`/
      `LapsedAlert` had schemas, `PdcSnapshot`'s D16 resolution never got
      one), registered in `bootstrap-local-topics.sh`.
      Deliberate asymmetry from the read side, confirmed by tracing the
      actual exception type through a live probe: `AvroKeyValueDeSerializer`
      catches decode failures (bad *data*, dead-letter is correct)
      but `AvroRecordSerializer`/`TypedAvroSerializationSchema` catch
      nothing on encode — a `SerializationException` there almost always
      means the registry itself is unreachable (an infrastructure problem,
      not bad data), so it propagates and lets Flink's normal checkpoint/
      restart fault-tolerance handle it, rather than silently dropping a
      valid alert.
      D25 records the Sonar coverage-exclusion decision for the three
      per-alert `loadSchema()` methods' `catch (IOException)` blocks —
      confirmed via direct probing that Avro's `Schema.Parser` converts
      both a missing resource and malformed content into its own unchecked
      `SchemaParseException` internally, never a raw `IOException`, so the
      catch exists only to satisfy the checked-exception rules of a static
      field initializer and is not reachable through any realistic failure.
      100% branch coverage elsewhere; `mvn clean verify` green
      (48 classes analyzed).
- [x] #71 Testcontainers-Redpanda test: produce → source → collect; sink →
      consume round-trip
      — done 2026-07-29: `KafkaSourceSinkRoundTripIT` (`MiniClusterWithClientResource`
      + Testcontainers `RedpandaContainer`, per CLAUDE.md §5), six round-trips —
      all three Phase 5 sources (`RxFillEventSource` #68, `DrugClassRefKafkaSource`
      + `AlertLeadTimeKafkaSource` #69) via a hand-built `KafkaProducer` +
      `KafkaAvroSerializer` standing in for the real upstream producer, and all
      four `AlertKafkaSinks` sinks #70 (`GapRiskAlert`/`LapsedAlert`/`PdcSnapshot`
      via a shared `consumeOne()` raw-consumer helper; `DeadLetter` separately,
      since it carries no Avro/schema-registry at all — raw bytes + a Kafka
      header, verified with a plain `ByteArrayDeserializer` instead).
      This phase was the first time any of this project's Kafka code ran
      against a real broker/registry rather than a mock or harness, and it
      found real defects no unit/harness test could have — see D28/D29.
      Also fixed mid-task, none design decisions in their own right:
      Maven Failsafe's `argLine` had no `--add-opens=java.base/java.util`/
      `java.time` at all (Surefire got this fix back in #68; Failsafe is a
      separate plugin block and never inherited it — same Kryo-vs-JPMS
      disease as before, just a second untreated spot); `commons-lang3`
      explicitly pinned to 3.17.0 (Testcontainers 2.x's `docker-java` needs
      `ArrayFill`, added in 3.14.0, but Flink's `provided`-scope pull of
      3.12.0 was winning Maven's nearest-wins mediation); `KafkaTypedSinkBuilder`
      now sets `transaction.timeout.ms` (default 60000, `ParameterTool`-driven
      per CLAUDE.md §4) explicitly rather than inheriting Flink's built-in
      1-hour default, which some brokers reject outright — not the root
      cause of the hang actually hit here (D29 was), but a real latent risk
      independently worth closing; `AlertKafkaSinks.deadLetterSink()` does
      **not** yet have this same fix — it builds its producer properties
      independently of `KafkaTypedSinkBuilder` and was never touched, flagged
      for whoever next revisits that sink.
      `mvn verify -Dit.test=KafkaSourceSinkRoundTripIT` green, all six methods.

**Exit criteria**: container tests green locally — met, evidence above

---

## Phase 6 — Upstream broadcast filter

- [x] `ChronicClassFilterFunction` (`BroadcastProcessFunction`): discard if
      NDC not in tracked classes **or** not trackable
      (specialty/infusion, FR-9); forward enriched event (with resolved
      `drugClass`) otherwise
      — done 2026-08-01: issue #78. `NDC_CLASS_DESCRIPTOR` (`MapState<String,
      DrugClassRef>`) registered with `RecordKryoSerializer` (D24's pattern)
      rather than left on Flink's default Kryo path — `DrugClassRef` is a
      record, and Flink's type extraction can't recognize records as POJOs
      (no JavaBean getters/setters), so it falls back to vanilla Kryo's
      `FieldSerializer`, which can't handle records at all under Java 17
      (D22's original finding, hit again here). New domain record
      `EnrichedFillEvent(RxFillEvent event, String drugClass)` (D31 — wraps
      rather than flattens) needed the same registration, plus `RxFillEvent`
      separately, since Kryo registration doesn't propagate into nested
      record fields (same lesson as `DrugClassRefUpdate`/`DrugClassRef`).
      Operator UID intentionally not set yet — that happens at stream-wiring
      time (`.process(...).uid(...)`), and no job-wiring class exists until
      Phase 9; harness tests bypass `StreamExecutionEnvironment` entirely so
      there's nothing to set it on right now. Intended UID:
      `"chronic-class-filter"`, to be applied when Phase 9 wires this in.
- [x] Buffering decision for events arriving before first reference broadcast
      → record in Decision Log
      — resolved as D30: buffer in operator list state (via
      `CheckpointedFunction`/`OperatorStateStore`, since this operator runs
      before `keyBy` — Flink's keyed-state API isn't valid here), flushed
      once `NDC_CLASS_DESCRIPTOR` receives its first update. "Is broadcast
      state empty" is checked live against the actual state on every
      `processElement` call rather than tracked via a separate boolean flag
      — a flag would desync from the real, checkpointed broadcast state
      across a restart that happens mid-buffering.
- [x] Harness tests: acute drug discarded; chronic + 0-refills kept;
      diabetes-classed specialty NDC discarded; drop-rate metric increments
      — done 2026-08-01: `ChronicClassFilterFunctionTest`, non-keyed
      `BroadcastOperatorTestHarness` + `CoBroadcastWithNonKeyedOperator`
      (distinct from the keyed variant Phase 7's `AdherenceProcessFunction`
      will need). Four tests: acute discard; chronic + 0-refills kept
      (proves `refillsAuthorized` is never the filter signal); diabetes-
      classed but `trackable=false` NDC discarded (proves FR-9's specialty
      exclusion is a property of the NDC/reference data, not the fill
      event's own `dispensingChanel` field); and the D30 cold-start
      buffer-then-replay path explicitly exercised (event arrives before
      any broadcast update, proven buffered not dropped, then correctly
      emitted once the update lands). Drop-counter assertions folded into
      the two discard tests rather than a separate test, via a
      package-private `droppedCount()` accessor — Flink's metrics API is
      write-only, no built-in way to read a `Counter` back without one.
      `mvn clean verify` green (50 classes analyzed, includes the full
      Phase 5 integration suite).

**Exit criteria**: harness tests green; filter drop counter exposed as metric — met, evidence above

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
| D17 | 2026-07-22 | Sonar `sonar.coverage.exclusions` extended to the 7 zero-logic domain records/enums (`RxFillEvent`, `GapRiskAlert`, `LapsedAlert`, `PdcSnapshot`, `DrugClassRef`, `EventType`, `Channel`) — `CoverageInterval` and `AdherenceState` deliberately excluded from this exclusion, since they carry real logic (compact-constructor guards) with real tests | Same reasoning as D14: plain records with no hand-written branches (JaCoCo-confirmed complexity of 1) have nothing meaningful to test; §5's exhaustive-testing rule stays fully in force for anything with actual logic |
| D18 | 2026-07-23 | `JobConfig` composed of three typed sub-config records (`KafkaConnectionConfig`, `CheckpointConfig`, `StateBackEndConfig`) instead of one flat class with 20+ unrelated getters (the ClaimGuard-project pattern reviewed as a reference) | Cohesion: each record groups exactly the settings a caller actually needs together (e.g., "give me the Kafka config" not five loose strings); fail-fast validation lives in each record's own compact constructor instead of a separate `validate()` someone has to remember to call |
| D19 | 2026-07-23 | RocksDB state TTL is a *configurable* value with a 400-day default (`state.ttl.days`), not a frozen unconfigurable constant as first proposed | Reversed mid-implementation: CLAUDE.md's "defined once" was initially read as "immutable," but the DRY guarantee only requires the default to exist in one place — forcing a code change + redeploy to retune an operational number is worse practice than making it a default-with-override, same as every other setting in this project |
| D20 | 2026-07-24 | `serialization/` deserializer kept as a single concrete class (`RxFillEventAvroDeserializer`), not a generic Strategy-pattern engine or a shared interface — both were built, discussed, and reverted | Only one Avro-backed deserializer is currently spec'd (the two broadcast topics have no registered schema); the generic engine and the interface each cost real clarity in a learning-first codebase for a reuse case that doesn't exist yet — revisit if/when a second concrete Avro deserializer is actually needed |
| D21 | 2026-07-25 | Watermark thresholds (`forBoundedOutOfOrderness` duration, `withIdleness` duration) sourced from a new `WatermarkConfig` record via `ParameterTool`, not literals on `RxFillWatermarkStrategy` — same `fromParams`/compact-constructor pattern as `KafkaConnectionConfig`/`CheckpointConfig`/`StateBackEndConfig` (D18) | User: "nothing must be hardcoded, everything must come from the environment variable" — an explicit, general instruction, not scoped to one class; applies the same reasoning already established by D19 (state TTL) to the watermark durations, closing the one remaining hardcoded-threshold gap in Phase 4 |
| D22 | 2026-07-26 | Domain records that cross a Flink serialization boundary get a small hand-written `com.esotericsoftware.kryo.Serializer<T>` per type, registered via `ExecutionConfig.registerTypeWithKryoSerializer(...)` | Flink 1.18's bundled Kryo (2.24.0, via `chill-java`) cannot serialize records at all — `FieldSerializer`'s `Unsafe.objectFieldOffset()` throws on any record field, and `TypeExtractor` doesn't recognize records as POJOs. **Superseded by D24** the same day, once #69 needed the identical treatment for two more record types and a hand-written-per-type approach started costing real duplication — kept here for the original diagnosis, which D24 still relies on |
| D23 | 2026-07-26 | All three Kafka sources in Phase 5 (`rx-fill-events`, `ndc-drug-class-ref`, `alert-lead-time-ref`) rebuilt on one shared, closed-for-modification architecture: `AvroValueMapper<T>` (Strategy interface, per-topic mapping logic) + `AvroKeyValueDeSerializer<T>` + `TypedKafkaRecordDeserialisationSchema<T>` + `KafkaSourceResult<T>` + `DeadLetterSplitFunction<T>` + `KafkaTypedSourceBuilder<T>` (Builder pattern) — all generic, none topic-specific. Per-topic code is only ever a new `AvroValueMapper<T>` implementation + a thin `*Source` class configuring the shared builder. This reopened #68's already-merged `RxFillEventKafkaSource` to unify it onto the same pattern rather than leaving it on a bespoke one-off shape | User: "let's start from scratch... use the enterprise reusable patterns followed by top orgs which follows OOP and SOLID principle... a class created must be not open for modifications" — explicit instruction, given after the original #69 design (separate concrete `DrugClassRefAvroDeserializer`/`AlertLeadTimeAvroDeserializer` classes, mirroring #68's shape) was flagged as producing near-duplicate classes and near-duplicate tests for every new topic. Verified concretely: a hypothetical 4th topic needs two new files and zero changes to any shared class |
| D24 | 2026-07-26 | The per-type hand-written Kryo serializers from D22 are replaced by one generic, reflection-based `RecordKryoSerializer` (uses `java.lang.reflect.RecordComponent` + the record's canonical constructor, works for *any* record type via `kryo.writeClassAndObject`/`readClassAndObject` on each component) | Building #69's two reference-data record types the D22 way would have meant two more hand-written serializers, each ~20 lines of near-identical boilerplate — exactly the "revisit if/when needed again" signal. A single generic serializer, registered per-type with a one-line `registerTypeWithKryoSerializer(X.class, RecordKryoSerializer.class)` call, eliminates the boilerplate permanently for every current and future record, including nested ones (`DrugClassRef` nested inside `DrugClassRefUpdate` needed its own registration too — nesting doesn't get inherited automatically) |
| D25 | 2026-07-27 | `sonar.coverage.exclusions` extended to `GapRiskAlertAvroSerializer.java`/`LapsedAlertAvroSerializer.java`/`PdcSnapshotAvroSerializer.java` — specifically for their `loadSchema()` methods' `catch (IOException)` blocks, unlike D14/D17 which excluded genuinely zero-logic files entirely | Confirmed via a direct runtime probe (not assumed): Avro's `Schema.Parser.parse(InputStream)` converts *both* a missing classpath resource and malformed schema content into its own unchecked `SchemaParseException` internally — it never lets a raw `IOException` escape. The catch block only exists because `loadSchema()` is called from a `private static final Schema SCHEMA = loadSchema();` field initializer, which can't declare `throws IOException` (checked exceptions can't escape a static initializer) — so it's required boilerplate for a path that doesn't fire under any realistic failure. Unlike D14/D17, this trades away real coverage credit for `serialize()` (which *is* tested) since Sonar's exclusion mechanism has no finer granularity than whole-file |
| D26 | 2026-07-28 | `AvroValueMapper<T>`/`AvroValueSerializer<T>` (the per-topic GenericRecord↔domain-object Strategy pair from D23) renamed to `AvroRecordDecoder<T>` (`T decode(String key, GenericRecord rec)`) / `AvroRecordEncoder<T>` (`GenericRecord encode(T value)`) | User flagged the original names as not meaningful — `AvroValueMapper` vs `AvroValueSerializer` don't read as an inverse pair and don't convey direction. Root cause: both interfaces operate purely at the `GenericRecord`↔domain-object boundary, never touching bytes — reusing "Serializer" there collides in spirit with `AvroKeyValueSerializer`/`AvroKeyValueDeSerializer`, which already own that word one layer down for the byte-level Confluent wrapping. Encoder/Decoder was chosen over a Reader/Writer or explicit Mapper-pair alternative (both presented) as the standard, unambiguous-direction pair for structured-representation conversion, reserving Serializer/Deserializer exclusively for the byte-level classes |
| D27 | 2026-07-28 | Top-level package `serde` renamed to `serialization` and restructured: `serde/mapper` → `serialization/codec` (holds `AvroRecordDecoder`/`AvroRecordEncoder` from D26); `serde/deserialization` → `serialization/decode` (+ `decode/decoders` for the per-topic `RxFillEventAvroMapper`/`DrugClassRefMapper`/`AlertLeadTimeMapper`); `serde/serialization` → `serialization/encode` (+ `encode/encoders` for the per-topic `*AvroSerializer` classes); `serde/kryo` → `serialization/kryo` (unchanged contents); `DeadLetterRecord`/`DeadLetterSplitFunction` moved out of the old `serde/util` grab-bag into a new `serialization/deadletter`. `KafkaSourceResult`/`KafkaSourceUtil` remain in `serialization/util` for now — not yet resolved where they belong (candidates: fold into `deadletter` since `KafkaSourceResult` is what `DeadLetterRecord`/`DeadLetterSplitFunction` consume, or move `KafkaSourceUtil` beside `config.KafkaConnectionConfig` since it only builds `OffsetsInitializer`/security `Properties` from Kafka connection config and has nothing to do with serialization at all) | User flagged the `serde` layout as not enterprise-grade. Root cause was twofold: (1) `serde` itself was never spec-aligned — `spec.md`'s "Project structure" section (line 400) names this package `serialization`, and the divergence was never recorded as a Decision Log entry, it just accumulated across D22–D26; (2) `serde/util` was a classic grab-bag (no cohesive responsibility) and `serde/mapper` was a stale name left over from before D26 renamed the types it holds from Mapper/Serializer to Decoder/Encoder. Verified: `mvn clean verify` green with zero remaining `rxvigilance.serde` references anywhere in `src/`, PR merged |
| D28 | 2026-07-29 | Testcontainers bumped `1.19.8` → `2.0.5` (`testcontainers-redpanda`/`testcontainers-junit-jupiter` artifact IDs, per 2.x's module rename convention — `org.testcontainers.redpanda.RedpandaContainer`'s package is unchanged, so no source changes needed beyond `pom.xml`) | `#71`'s first `KafkaSourceSinkRoundTripIT` run failed with every Testcontainers Docker-detection strategy rejected by a `400 Bad Request`, confirmed via `-Dlog4j2.configurationFile` DEBUG logging to be `docker-java` (bundled in Testcontainers 1.x) hardcoding a request for Docker API version 1.32, which this machine's Docker Engine 29 (Docker Desktop 4.56) refuses outright. Documented, widely-hit break (`testcontainers-java` issues #11232/#11235): 1.20.x/1.21.x never fixed it: 2.0.2+ ships a `docker-java` that negotiates the API version with the daemon instead of assuming a fixed old one |
| D29 | 2026-07-29 | `AvroKeyValueSerializer` (write side) now constructs `KafkaAvroSerializer` via the 2-arg `(SchemaRegistryClient, Map<String,?>)` constructor with `auto.register.schemas=true` and `schema.registry.url` explicitly set, instead of the 1-arg `(SchemaRegistryClient)` constructor it used before | `gapRiskAlertSinkRoundTrip` hung in an infinite Flink task-restart loop (196+ attempts observed before being killed) once checkpointing was enabled (checkpointing implicitly turns on Flink's retry-on-failure, unlike the source tests which never hit this). Decompiled `KafkaAvroSerializer`'s bytecode to confirm: the 1-arg constructor never calls `configure(...)`, so `autoRegisterSchema` — documented by Confluent as defaulting to `true` — was silently sitting at Java's untouched-`boolean` default of `false` instead, with no way to override it since no config map was ever passed at all. Every write to a topic without an already-registered schema failed with `404 Subject not found`, and retrying doesn't fix a permanently-misconfigured serializer, hence the infinite loop. This was invisible before #71 because `AlertKafkaSinksTest`/`KafkaTypedSinkBuilderTest` stub the schema registry client (never exercising this runtime path), and even real local runs are masked by `bootstrap-local-topics.sh` pre-registering schemas for the three known production topics (`gap-risk-alerts`/`lapsed-alerts`/`pdc-snapshots` — confirmed in Phase 0's own ledger note, `FULL_TRANSITIVE` on 3 subjects) — a topic whose schema isn't pre-registered would have hit this in production too, indistinguishable from a hang, with no error pointing at the cause |
| D30 | 2026-07-31 | `ChronicClassFilterFunction` buffers fill events arriving before `NDC_CLASS_DESCRIPTOR`'s broadcast state has received its first update (operator list state; flushed through the filter once the first broadcast update lands), rather than discarding them | The race only exists on a genuinely fresh first-ever deploy — Flink restores all operator state, broadcast state included, before resuming record processing on every ordinary restart, so this window never recurs after the first successful run. Discarding during that window would silently drop real member fills, an actual correctness bug, not a cosmetic one. Buffering also resolves a second question for free: once the first broadcast update lands, "NDC not present in state" unambiguously means "not tracked" forever after — no need to keep distinguishing "not tracked" from "not warm yet" on an ongoing basis |
| D31 | 2026-07-31 | New domain record `EnrichedFillEvent(RxFillEvent event, String drugClass)` wraps the original event rather than flattening its fields into a new duplicate record | Wrapping means zero risk of the two field sets drifting apart as `RxFillEvent` evolves — a flattened alternative would require updating two records in lockstep for every future `RxFillEvent` field change. The one extra `.event()` indirection downstream is a small, one-time cost against that ongoing duplication risk |
| D32 | 2026-08-01 | A missing `LEAD_TIME_DESCRIPTOR` entry for `(drugClass, dispensingChannel)` falls back to a static, `ParameterTool`-driven default (`alert.lead.days.default`, defaulting to 7) rather than a hardcoded literal or a dynamic max-of-known-values scan, and increments a warn metric on every occurrence — deliberately not a buffer-and-wait like D30 | The default can never be `0`: `GapRiskAlert` exists specifically to warn *before* exhaustion, so a 0-day fallback would silently turn every unmapped combo into a same-day/lapsed-style notification, defeating the alert's purpose. A dynamic "largest currently-known value" alternative was considered and rejected as unneeded complexity for what should be a rare, data-quality-driven gap — caught and fixed via the warn metric at the source, not optimized around at runtime. Doesn't need D30's buffer treatment: by the time a fill reaches `AdherenceProcessFunction` it's already confirmed trackable (survived Phase 6), and the interval-merge/`PdcSnapshot` work (spec's steps 2–4) doesn't depend on `alertLeadDays` at all — only the timer's exact firing date does, so holding up the entire state update over one lookup gap would be disproportionate |
