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
| 7 | Adherence core — FILL path & timers | local | ✅ done 2026-08-01 |
| 8 | onTimer, LapsedAlert & REVERSAL path | local | ✅ done 2026-08-02 |
| 9 | Metrics, job wiring & integration test | local | ✅ done 2026-08-03 (IT hardened + topology test 2026-08-05 — D41–D45; metric counters tested 2026-08-06 — D46) |
| 10 | Containerization & CI/CD deploy path | cloud | ✅ done 2026-08-10 — all 14 child issues closed, both exit criteria met with evidence (D49–D65). **GKE cluster is still up and billing — destroy unless starting Phase 11** |
| 11 | Logging & Observability | cloud | ◐ in progress — started 2026-08-10. Epic #145 + child issues #146–#153 created; scope expanded beyond spec minimum (D66). #146–#150 done (all the local work); #151–#153 remain and all need the cluster |
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

- [x] `KeyedBroadcastProcessFunction` skeleton, keyed
      `(memberId, drugClass)`; `AdherenceState` ValueState + TTL
      — done 2026-08-01: issue #81. Keyed on `Tuple2<String, String>`
      (`memberId`, `drugClass`) rather than a new domain record — purely
      Flink plumbing, not a domain concept worth naming, and `Tuple2` has
      Flink's own built-in `TupleSerializer` (zero Kryo registration
      needed), unlike every domain record touched so far. Main `OUT` type
      is `Void`: per spec.md's pipeline topology diagram, all four of this
      operator's eventual outputs (`GapRiskAlert`, `LapsedAlert`,
      `PdcSnapshot`, dead-letter) are side outputs, not a shared main-output
      type — there's no honest common supertype across four genuinely
      different domain shapes. Only `PDC_SNAPSHOT_OUTPUT_TAG` declared this
      phase; the other three belong to Phase 8, where what feeds them
      actually exists. `AdherenceState`'s `ValueStateDescriptor` is a
      `private transient` instance field built in `open()` (not
      `static final` like `LEAD_TIME_DESCRIPTOR`) since `enableTimeToLive`
      mutates the descriptor in place using constructor-injected
      `StateBackEndConfig` (reused from Phase 4, not redeclared) — nothing
      external needs the same instance, unlike the broadcast descriptor.
- [x] FILL path: IntervalMerger delegation, delete-then-register timer,
      `alertLeadDays` broadcast lookup persisted, `activeTimerTimestamp`
      persisted
      — done 2026-08-01: `IntervalMerger.merge()` (Phase 3, pure/tested)
      handles the interval math entirely; this operator only orchestrates
      the Flink-specific parts around it. Real bug caught while reading
      `merge()`'s actual source before writing the caller:
      `IntervalMerger.merge()` requires a non-null `state` argument (calls
      `state.activeCoverageIntervals()` immediately, no null-guard) — a
      brand-new key's `null` `ValueState` read is converted to an empty
      `AdherenceState` before the call, not passed through. Also:
      `merge()` returns the *same object reference* (not an equal copy) on
      a duplicate `claimId` — detected via `==`, not `.equals()`, to skip
      the timer/lookup work entirely on redelivery rather than needlessly
      re-registering an identical timer or re-running the lead-time lookup
      against potentially-changed reference data. Timer timestamps and the
      watermark strategy's own timestamp assignment (Phase 4's
      `RxFillWatermarkStrategy`) both derive from `LocalDate` via
      `atStartOfDay(ZoneOffset.UTC)` — confirmed identical before writing,
      since a mismatched time base would silently misalign timers against
      the watermarks driving them. `PdcSnapshot.emittedAt` uses
      `ctx.timestamp()` (the fill event's own assigned event-time
      timestamp), not `ctx.timerService().currentWatermark()` — the
      watermark lags real event time by design and can be uninitialized
      early in a stream.
- [x] Missing lead-time lookup entry → default + warn metric (Decision Log)
      — resolved as D32: static `ParameterTool`-driven default
      (`alert.lead.days.default`, default 7, never 0 — see D32's full
      rationale for why 0 would defeat `GapRiskAlert`'s purpose), plus a
      warn metric on every occurrence. Deliberately not buffered like D30 —
      by the time a fill reaches this operator it's already confirmed
      trackable (survived Phase 6), and the interval-merge/`PdcSnapshot`
      work doesn't depend on `alertLeadDays` at all, only the timer's exact
      firing date does.
- [x] `AdherenceTimerTest` (event-time advancement, explicit watermarks):
  - [x] single fill registers timer at `endDate - leadDays`
  - [x] refill before threshold cancels & re-registers (exactly one timer)
  - [x] lead time resolved per `(class, channel)`, not a constant
  - [x] PDC snapshot emitted on fill
      — done 2026-08-01: `AdherenceProcessFunctionTest`,
      `KeyedBroadcastOperatorTestHarness` + `CoBroadcastWithKeyedOperator`.
      `onTimer` isn't implemented until Phase 8, so there's no side-output
      alert content to assert on when a timer fires yet — resolved by
      asserting on real persisted-state content (`activeTimerTimestamp`,
      `alertLeadDays`, via a package-private `currentAdherenceState()`
      accessor, same pattern as Phase 6's `droppedCount()`) plus bracketing
      an exact watermark advance with `numEventTimeTimers()` before/after,
      rather than weakening CLAUDE.md §5's "assert on content, not counts"
      rule — advancing to the precise expected timestamp and observing the
      timer actually fire there is still advancing time explicitly and
      observing a real effect, not a vacuous count check. Broadcast-side
      watermark advanced to `Long.MAX_VALUE` once in `setUp()`, since the
      operator's combined watermark is the minimum across both connected
      inputs and the reference stream was otherwise silently holding back
      every timer from ever firing. Five Kryo registrations needed (not
      two) — every record this operator's data actually touches:
      `EnrichedFillEvent` + nested `RxFillEvent` (input), `AdherenceState` +
      nested `CoverageInterval` (keyed state), `PdcSnapshot` (side output).
      A fifth test (`missingLeadTimeLookupFallsBackToDefaultAndWarns`)
      covers D32's fallback path, beyond the four ledger-listed cases.
      `mvn clean verify` green (52 classes analyzed, includes the full
      Phase 5 integration suite).

**Exit criteria**: timer invariant holds in every test — at most one
registered timer per key, state timestamp matches it — met, evidence above

---

## Phase 8 — onTimer, LapsedAlert & REVERSAL path

**Goal**: the alert contract, including the **binding correction guarantee**
(`hld.md` §3 / spec "Event handling — REVERSAL" step 5).

- [x] `onTimer`: defensive timestamp check → `GapRiskAlert` side output →
      register lapsed timer at exhaustion date
      — done 2026-08-02: issue #84. Two independent defensive checks before
      emitting, not one: the firing timestamp must match `activeTimerTimestamp`
      (Phase 7's hygiene invariant paying off directly), *and*
      `currentSupplyEndDate` must still be ahead of the firing timestamp
      (spec step 2) — belt and suspenders, since a false `GapRiskAlert` has
      real member-facing cost. The second check only applies to the
      `GAP_RISK` branch, not `LAPSED` — firing exactly at
      `currentSupplyEndDate` *is* the correct, expected behavior for the
      lapsed stage, not something to defend against. `emittedAt` uses the
      `onTimer(long timestamp, ...)` parameter itself, not `ctx.timestamp()`
      — the latter is for the current *input element*, and there isn't one
      when a timer fires; easy to reach for the wrong one since they sound
      alike. `ctx.getCurrentKey()` (a `Tuple2<String,String>`) is the source
      of `memberId`/`drugClass` for the alert, since there's no incoming
      event to read them from either.
- [x] Lapsed timer fires → `LapsedAlert`
      — done 2026-08-02: same `onTimer` override, `LAPSED` branch. Clears
      `activeTimerTimestamp`/`activeTimerStage` back to `null`/`null` on
      firing — the correct resting state until the next real fill for this
      key restarts the cycle from `GAP_RISK`.
- [x] REVERSAL path: unwind via IntervalMerger, recompute, delete timer,
      re-register if coverage remains; if **no** coverage remains, emit
      corrective alert immediately in `processElement`
      — done 2026-08-02: added the `eventType` branch `processElement` was
      missing since Phase 7 (every event, FILL or REVERSAL, was silently
      merged as a FILL until now — latent since #81, never in scope until
      this issue). `IntervalMerger.unwind()` (Phase 3) does the actual
      interval math; `handleReversal()` orchestrates the Flink-specific
      parts, reusing the exact same reference-equality no-op signal
      `merge()` uses for duplicate `claimId`s (`unwound == current`) — which
      also elegantly covers redelivery of the same reversal for free, since
      a second delivery finds nothing left to unwind. Coverage-remains case
      re-arms as `GAP_RISK` (subtracts `alertLeadDays` again), not a bare
      "no lead time" timer — spec's own wording ("the recomputed timer
      *emits the superseding alert*") ties this to the same
      latest-alert-wins machinery a fresh fill uses, not a shortcut to the
      confirmed-lapse stage. Timer deletion never checks
      `activeTimerStage` before deleting — Flink's `deleteEventTimeTimer`
      is purely timestamp-based, no concept of stage at all, so the same
      one line correctly cleans up a pending `GAP_RISK` or `LAPSED` timer
      without needing to know which.
- [x] Harness tests:
  - [x] no refill → GapRiskAlert then LapsedAlert, in event-time order
  - [x] stale timer (timestamp mismatch) fires as no-op
  - [x] reversal shrinking coverage → superseding alert from recomputed timer
  - [x] reversal to zero coverage → immediate corrective alert, no timer left
  - [x] reversal after GapRiskAlert already emitted → supersede semantics hold
      — done 2026-08-02: `AdherenceProcessFunctionTest`, extended. The
      stale-timer case is structurally unreachable through the normal,
      correct FILL/REVERSAL paths by design (both always delete-then-
      register) — genuinely testable only by deliberately desyncing state
      from the real registered timer, so a new package-private
      `forceAdherenceStateForTest()` accessor was added alongside the
      existing `currentadherenceState()`, simulating the realistic cause
      (a restored savepoint older than the live timer schedule). The
      "event-time order" assertion compares the two alerts' own `emittedAt`
      fields against each other, not which `assertThat` call happens to run
      first in the test method — the latter would prove nothing about the
      pipeline's actual behavior. Two more Kryo registrations needed in
      `setUp()` (`GapRiskAlert`, `LapsedAlert`) — same disease as every
      prior phase, hit again the moment these types first flowed through a
      side output. One pre-existing Phase-7-era test
      (`singleFillRegistersTimerAtEndDateMinusLeadDays`) had a now-stale
      assertion (`numEventTimeTimers()` expected `0` after firing, written
      for `onTimer`'s old no-op stub) — updated to assert `1` (the cascaded
      lapsed timer) plus real `GapRiskAlert` content, rather than weakened
      or deleted, per this repo's own rule on failing tests.
      `mvn clean verify` green (55 classes analyzed, includes the full
      Phase 5 integration suite).

**Exit criteria**: every test asserts side-output *contents*, not just
counts; correction guarantee covered explicitly — met, evidence above

---

## Phase 9 — Metrics, job wiring & integration test

- [x] `AdherenceMetricsReporter`: alert emission counters, filter drop rate,
      lead-time-default-used counter
      — done 2026-08-02: `chronicFilterDropped`, `missingLeadTimeLookup`,
      `gapRiskAlertsEmitted`, `lapsedAlertsEmitted` counters implemented and
      wired into `ChronicClassFilterFunction`/`AdherenceProcessFunction`
      — gap found 2026-08-05, closed 2026-08-06 (#92): the evidence above
      covered *wiring* only — no test asserted the registered metric names or
      that the counters increment. Correction to the 2026-08-05 note: this
      was 3 of 4 counters, not all 4. `chronicFilterDropped` was already
      covered by four assertions in `ChronicClassFilterFunctionTest` via the
      existing `ChronicClassFilterFunction.droppedCount()` accessor; the
      initial review missed it because the test asserts through that accessor
      rather than the counter name. Now closed for all four — see D46.
      `mvn clean verify` green: 118 unit tests (from 117), 10 IT
- [x] `AdherenceJob`: full topology wiring, **operator UIDs on every
      operator**, side outputs → sinks
      — done 2026-08-02: full topology wired (D35 naming convention); see
      D36–D38 for three correctness bugs found and fixed while verifying
      this wiring end-to-end against a live local job
- [x] `AdherencePipelineIT` (MiniCluster + RocksDB + Testcontainers
      Redpanda): fixture stream covering fill → early refill → reversal →
      gap → lapse; asserts on all four output topics
      — done 2026-08-03: `fillProducesGapRiskAlertAndPdcSnapshot` green
      (`Tests run: 1, Failures: 0`, 16.94s) — see D39 for the topic-existence
      and watermark-starvation issues found and fixed getting this first
      test green. `reversalWithNoRemainingCoverageProducesLapsedAlert` also
      green same session, confirming D34's binding correction guarantee
      end-to-end (`LapsedAlert` fires synchronously from `processElement`,
      no timer/watermark dependency, unlike the gap-risk path).
      `malformedRecordsOnAllThreeSourcesRouteToSharedDeadLetterTopic` green
      after fixing a real production bug — see D40 — confirming all three
      sources' malformed records converge on one dead-letter topic. All four
      output topics (`gap-risk-alerts`, `pdc-snapshots`, `lapsed-alerts`,
      `dead-letter`) now covered by this test class. D40's `build()`
      signature change broke 3 other existing tests
      (`DrugClassRefKafkaSourceTest`, `AlertLeadTimeKafkaSourceTest`,
      `KafkaSourceSinkRoundTripIT`) still calling the old
      `DataStream<T>`-returning signature — fixed to use `.events()` on the
      new result record. `mvn clean verify` green full-suite, confirming no
      other regressions from the D40 signature change
      — **amended 2026-08-05**: the 2026-08-03 sign-off above was verified
      per-method, not full-class. Running the whole class failed
      deterministically (3/3 attempts) on
      `fillProducesGapRiskAlertAndPdcSnapshot`, while solo and both pairwise
      runs passed. Root cause is a watermark-idleness ordering hazard, not a
      build or Kafka issue — see D41. Fixed and re-verified 2026-08-05:
      `AdherencePipelineIT` 3/3 green on 3 consecutive full-class runs
      (~23s each), `mvn clean verify` green. Two further hardening changes in
      the same pass: every test now overrides **all five** sink topics
      (previously each test left 2–3 on their default names, so the three
      jobs shared `transactional.id`s within one Redpanda container), and
      `malformedRecords…`'s assertion gained `.doesNotContainNull()` — it
      previously asserted only `hasSize(3)` and would have passed with three
      null error-messages, contrary to §5's assert-on-contents rule
- [x] `AdherenceJobTopologyTest`: builds the StreamGraph without executing it
      and asserts the §4 savepoint invariants
      — done 2026-08-05: asserts the 8 operator UIDs wired in `AdherenceJob`
      are present, and that `NDC_CLASS_DESCRIPTOR`/`LEAD_TIME_DESCRIPTOR`
      still read `ndc-class-state`/`lead-time-state`. `Tests run: 2,
      Failures: 0`, 0.8s. Written to cover `AdherenceJob` with a fast test
      rather than excluding it from coverage as D14 had planned — see D42.
      — extended 2026-08-05: the UID enumeration was run and found 3 of 22
      operators with no UID — a real §4 violation. Fixed and the test now
      also asserts *no* operator has a null UID, so the invariant is
      self-enforcing rather than a list someone has to remember to extend
      — see D43. `mvn clean verify` green: 117 unit tests (from 114), 10 IT
- [x] Local run instructions verified exactly as written in spec "Local run"
      — done 2026-08-02: `docker-compose up` → IntelliJ run → hand-produced
      `DrugClassRef` + `RxFillEvent` for `TEST-MEMBER-A`/`DIABETES` →
      `GapRiskAlert` observed on `gap-risk-alerts` (`leadDays=10`,
      `expiresOn=2026-08-07`), matching `alert.lead.days.default` fallback
      (D32); 6 alerts total confirmed across every test member produced
      during this verification session

**Exit criteria**: IT green; job runs locally end-to-end from
`docker-compose up` through alerts visible in `gap-risk-alerts`

---

## Phase 10 — Containerization & CI/CD deploy path **[CLOUD]**

Epic #97. Tasks expanded 2026-08-07 when the epic was created — the four
bullets originally here are now #99, #103, #104 and #105.

*Local — no cluster, no cost:*
- [x] #98 Configure `maven-shade-plugin` (`ServicesResourceTransformer`)
      — done 2026-08-07: 5 service files were being silently truncated in the
      uber-jar; all now fully merged. See D49
- [x] #99 Finalize Dockerfile (dependency-cached multi-stage per spec)
      — done 2026-08-07: build context 396 MB → 18.92 kB, jar name no longer
      hardcoded, Flink base image pinned to 1.18.1. See D50
- [x] #100 Point FlinkDeployment at `AdherenceJob` — `upgradeMode: savepoint`,
      RocksDB-aware memory, and collapse the duplicated broker/registry
      config to one source (D47 showed why the duplication is a hazard)
      — done 2026-08-07: entry class, savepoint mode + savepoint dir, and args
      collapsed to `--profile gke`. Memory sizing moved to #101, which
      measures the node. See D51
- [x] #109 `AdherenceJob` does not authenticate to the schema registry — only
      `SmokeJob` passes registry credentials, so Phase 2 being green proves
      nothing. Found while scoping #100. **Blocks #105**
      — done 2026-08-08: registry credentials threaded through both the encode
      and decode paths; `AUTO_REGISTER_SCHEMAS` turned off and the three missing
      subjects added to `redpanda.tf` (applied — cloud now has all six).
      122 unit + 10 integration green, run twice to rule out the race
      described in D52. See D52

*Infrastructure:*
- [x] #101 Scale GKE node pool, revise D7 (credits expire ~2026-09-05).
      Captures `kubectl describe node` — the measurement that settles #30
      — done 2026-08-08: node pool 1 → 2 applied (5 to add, 0 to change,
      0 to destroy; two-phase apply per §10). Measured 3920m CPU and
      ~12.96 GiB Allocatable per node, ~3.1 CPU and ~11.7 GiB free after the
      platform stack. TaskManager set to 4096m from that measurement.
      D7 superseded, revert trigger recorded. See D53.
      **Cluster is now up and billing — destroy after #105**
- [x] #102 ~~Workload Identity Federation for GitHub Actions → GCP~~
      — **closed 2026-08-08 as dead work, not done**. Under GitOps CI pushes
      to GHCR and commits to git, both native GitHub permissions, and never
      reaches GCP. Reopen only if CI must verify cluster health directly.
      See D54

*Pipeline:*
- [x] #30 Install Argo CD: `helm_release` in `runtime/helm.tf`, an `AppProject`
      scoping repo/namespace/kinds, and `k8s/flink/kustomization.yaml`.
      **Application deliberately not created yet** — see the ordering note
      below. D54
      — done 2026-08-08: chart 10.3.0 (Argo CD v3.5.0) applied, `dex` and
      `notifications` disabled, resource requests set on the five remaining
      components. AppProject applied by hand. `kubectl kustomize k8s/flink`
      renders all three resources with the image transform. See D55
- [x] #103 `.github/workflows/deploy.yml`: main-branch push → package → GHCR
      push (commit-SHA tag) → `kustomize edit set image` → commit.
      Triggers with `paths-ignore: k8s/**` so the bot commit cannot loop.
      Health is Argo CD's sync/health status, not a CI poll. D54
      — done 2026-08-08: merge to `main` builds and pushes the SHA-tagged
      image, then opens a deploy PR rather than pushing to protected `main`.
      Verified end to end via PR #123: `newTag` on `main` is the commit SHA
      and **no run was triggered by the merge**. `yq` replaced
      `kustomize edit`; the `production` environment gate was dropped in
      favour of the PR. Argo CD sync/health still unproven — that is #114.
      See D57

*Cloud — billing starts here:*
- [x] #104 Re-`apply` runtime Terraform; recreate the hand-made
      `kafka-credentials` and `ghcr-pull` Secrets (§9, D8 — never in
      Terraform state)
      — done 2026-08-08: namespace applied, both Secrets created
      (`sasl-password` 10 bytes, `sasl-username` 18 — non-empty, verified).
      `make infra-up` already automated this and now reflects Argo CD's
      ownership of `k8s/flink/`; `make infra-verify` green across
      cert-manager, flink-system, monitoring and argocd. Secrets-storage
      decision recorded and deferred to #118. See D56
- [x] #114 Create the Argo CD `Application` (`path: k8s/flink`, automated
      prune + selfHeal). After first sync, confirm both Secrets still exist —
      the check that proves D54's prune reasoning rather than assuming it
      — done 2026-08-09: Synced at `437b9ce` (the deploy bot's own commit),
      image `:24b8a458…` matching `newTag` on `main`. Both Secrets survived
      the first prune (ages 4h30m, older than the Application). Namespace
      carries no `argocd.argoproj.io/instance`. selfHeal proven by patching
      `flink-role` verbs to `["get"]` — all seven restored before the next
      command ran. Argo CD's `Healthy` reported green three times while the
      job was broken; `kubectl get flinkdeployment` is the real signal.
      Also added the AppProject + Application to `make infra-up`: both were
      hand-applied in #30 and died with the cluster. See D58
- [x] #130 Grant `TRANSACTIONAL_ID` ACL to `rx-vigilance-flink` — EXACTLY_ONCE
      sinks failed with `TransactionalIdAuthorizationException`. Found while
      verifying #114
      — done 2026-08-09: one PREFIXED WRITE on `rx-vigilance-` in
      `redpanda.tf`; platform apply read 1 to add, 0 to change, 0 to destroy.
      See D59
- [x] Sink transaction timeout — `deadLetterSink` never set
      `transaction.timeout.ms`, so Kafka's 1h default exceeded Redpanda's
      15m maximum. Shipped in PR #131 alongside #130 without its own issue
      — done 2026-08-09: `KafkaSourceUtil.producerProperties` now the single
      construction point for every sink's producer config; 3 new tests
      (125 unit + 10 IT green). See D59
- [x] #105 Verify job healthy on GKE against cloud Redpanda through one
      completed checkpoint cycle
      — infrastructure half done 2026-08-09: checkpoints 50→81 completed,
      every 30s, ~33.5 kB, ~1s duration; JM and TM pods 0 restarts;
      `RUNNING`/`STABLE`; seven job directories and one savepoint visible
      under `gs://…-ckpt/`. Proves the transactional sink commits, Workload
      Identity reaches the bucket, the GCS plugin loads, and
      `state.checkpoints.dir` is correct. Full loop exercised: merge →
      Actions build → GHCR → App-token commit → Argo CD sync → operator
      savepoint upgrade → running job
      — **end-to-end half done 2026-08-09**, after the entry above was
      wrongly ticked on the infrastructure evidence alone. Every checkpoint
      being exactly 33544 bytes was the tell: state was not growing because
      no data had ever flowed. `CloudEventPublisher` published reference
      data plus two fills as `rx-vigilance-test-producer`; one
      `GapRiskAlert` for the M001 fill appeared on `gap-risk-alerts`,
      `dead-letter` stayed empty. **Observed delay: 5 minutes** — matches
      D41's predicted idleness wait exactly; recorded on #94. See D60
- [x] #132 Single-source the checkpoint directory — the `gs://` path is written
      in both `application-gke.properties` and `flink-deployment.yaml` and
      nothing forces them to agree (D47's hazard). Nothing is broken today
      — done 2026-08-10: `checkpoint.dir` is now optional; on GKE the manifest
      is the only source, locally `application-local.properties` is. Proven on
      the cluster: the job logged that it was relying on the cluster config,
      and Flink logged `FileSystemCheckpointStorage` — which rules out the
      JobManager-memory fallback directly rather than by inference. New job
      directory in GCS, checkpoints 2401-2403 at 37031 bytes, so the savepoint
      restore carried state across the upgrade. See D63
- [x] #133 Revisit `transaction.timeout.ms` — 60000 was kept because the alert
      sinks had already proven it against this broker, not chosen. A restart
      lasting over a minute expires in-flight transactions. Needs the broker's
      actual `transaction.max.timeout.ms`
      — done 2026-08-10: raised to 900000, the broker's maximum, confirmed by
      probing the cluster (1h rejected, 15min accepted). **A measured restart
      took 64s** — the old value was *below* real restart time, not merely
      close to it. Deployed job logs `transaction.timeout.ms = 900000` on all
      three producers. See D64
- [x] #135 `deploy.yml` rebuilt and redeployed on documentation-only changes —
      `paths-ignore` covered `k8s/**` only, so a Markdown edit restarted a
      healthy job for ~64s and pushed another 600 MB image
      — done 2026-08-10: `'**.md'` added to `paths-ignore`. **Verification is
      the merge of this ledger entry itself** — a commit touching only
      `IMPLEMENTATION.md`, which must produce no Deploy run. See D65
- [x] #118 Replace the hand-created Secrets with External Secrets Operator +
      Google Secret Manager. **After #105** — a new secrets mechanism before
      the job is proven makes failures ambiguous (same reasoning as #30).
      Chosen because Workload Identity means **zero stored credentials**;
      ESO is the abstraction, so the store stays swappable
      — done 2026-08-09: two Secret Manager entries (containers in Terraform,
      values added out of band), a dedicated GSA with per-secret
      `secretAccessor`, ESO chart 2.9.0, a namespaced `SecretStore` reporting
      `Valid`/`READY True`, and `ExternalSecret`s producing `kafka-credentials`
      and `ghcr-pull` under their original names. Migrated via temporary names
      and hash comparison; a deleted TaskManager came back `1/1` on the
      operator-managed Secrets, and checkpoints continued (840, 841).
      `make infra-up` no longer needs any credential — `check-env` deleted.
      See D62
- [x] #115 Set resource requests on cert-manager, the Flink operator and
      kube-prometheus-stack — all ten pods are `BestEffort`. **After #105**
      on purpose: don't move the cluster underneath the job being verified.
      Resilience, not capacity — D53 stands. See D55
      — done 2026-08-09: 11 resource blocks across the three releases; every
      pod in `cert-manager`, `flink-system` and `monitoring` now reports
      `Burstable` and `grep BestEffort` across the whole cluster returns
      nothing. Job stayed `RUNNING`/`STABLE` through the operator restart.
      ~460m CPU and 2 GiB now reserved. See D61

**Decision gate — RESOLVED 2026-08-08: Argo CD adopted** (D54). #102 is closed
as dead work; #103's last mile becomes a git commit rather than a cluster call.

**Ordering trap** (D54): the FlinkDeployment references the `ghcr-pull` and
`kafka-credentials` Secrets and the image tag `:main`, none of which exist
until #104 and #103. Creating the `Application` before those lands syncs
straight into `ImagePullBackOff`. Install Argo CD now, create the Application
after #103.

**Exit criteria**: a merged PR reaches GKE with no manual steps beyond
approval; job stable through one checkpoint cycle

- ✅ 2026-08-09: PR #131 merged → Actions built and pushed
  `ghcr.io/sourabhagari/rx-vigilance:24b8a458…` → the GitHub App wrote
  `newTag` to `main` (`437b9ce`) → Argo CD synced that revision → the
  operator ran a savepoint upgrade → job `RUNNING`/`STABLE`. Merging the PR
  was the only human action.
- ✅ 2026-08-09: checkpoints 50→81, 30s apart, ~1s duration, 0 pod restarts.

**Phase closed ✅ 2026-08-10.** All 14 child issues closed: #30, #98, #99,
#100, #101, #102 (dead work), #103, #104, #105, #109, #114, #115, #118, #130,
#132, #133, #135. Epic #97 closed with both criteria evidenced above.

Repeated since, so the exit criteria are not a one-off: five further merges
reached the cluster the same way, the last being `cb0d8b0` for #133. The
deploy path also survived being changed underneath itself — D57's PR flow
became D58's GitHub App, and D54's Argo CD Application took over from
`kubectl apply`, without a manual deployment in between.

What this phase did **not** produce, deliberately: dashboards, alert rules,
or any measurement of the job under sustained load. Those are Phases 11 and
12. The only observability today is `kubectl logs` and the fact that
checkpoints keep completing — which was enough to close #105 and is not
enough to run anything.

---

## Phase 11 — Logging & Observability **[CLOUD]**

Epic #145; child issues #146–#153. Scope expanded beyond `spec.md`'s four
bullets at phase start — see **D66**. The spec minimum is #146, one dashboard
of #151, and two rules of #152; everything else is the expansion.

**Goal**: make the job supportable by someone who has never read the code.
Every item is judged against one path — alert → component → dashboard → pod →
log → correlation ID → Kafka message → processing step → error → root cause.

**Starting position (measured 2026-08-10)**: kube-prometheus-stack has been
running since Phase 1, but nothing scrapes Flink — no reporter, no PodMonitor,
so `AdherenceMetricsReporter`'s four counters go nowhere. `observability/`
does not exist. Three classes log at all (`AdherenceJob`, `SmokeJob`,
`IntervalMerger`); `AdherenceProcessFunction`, all three sources, all four
sinks and `DeadLetterSplitFunction` log nothing. Operators have `.uid()` but
never `.name()`, and Flink metric labels use the name. `DeadLetterRecord` is
`(rawBytes, errorMessage)` — a poison message cannot be traced back to Kafka.

**Local — verifiable without the cluster**
- [x] #146 Flink Prometheus reporter in FlinkDeployment +
      `k8s/flink/podmonitor.yaml` scraping JM + TMs. The unblocker:
      nothing else in the phase works until metrics leave the job
      — done 2026-08-11: config only, no image change. The reporter jar
      already ships at `/opt/flink/plugins/metrics-prometheus/` (the image
      pre-stages all seven reporters as plugin folders), so
      `ENABLE_BUILT_IN_PLUGINS` was **not** touched — that variable is for
      loose jars in `/opt/flink/opt/`, where no prometheus jar exists.
      Four files: `appproject.yaml` (+PodMonitor kind, hand-applied — the
      Makefile owns it, not Argo CD), `flink-deployment.yaml`
      (`metrics.reporter.prom.factory.class` + `.port "9249"`, and a named
      container port `metrics` on the shared `&podTemplate` anchor so JM and
      TM both get it), `podmonitor.yaml` (new), `kustomization.yaml`.
      Verified as a four-rung ladder, each rung isolating one half:
      (1) no distinctive startup log line — moot, (2) superseded it:
      `curl localhost:9249/metrics` inside both pods returned 1897 lines /
      298 distinct metric names, (3) Prometheus `/api/v1/targets` shows both
      pods `up` on `podMonitor/rx-vigilance/rx-vigilance-flink/0` with the
      `component` label present, proving `podTargetLabels` worked,
      (4) queries resolve: `flink_jobmanager_job_uptime`=133081,
      `numberOfCompletedCheckpoints`=2, and all four `adherence` counters.
      **Metric-name contract for #151/#152**, confirmed against the live
      endpoint rather than assumed:
      `flink_taskmanager_job_task_operator_adherence_{chronicFilterDropped,
      gapRiskAlertsEmitted, lapsedAlertsEmitted, missingLeadTimeLookup}`,
      `flink_taskmanager_job_task_operator_currentOutputWatermark`,
      `flink_jobmanager_job_uptime`,
      `flink_jobmanager_job_numberOfCompletedCheckpoints`.
      **Finding that re-orders the phase**: `operator_name` labels come back
      as `Co_Process_Broadcast`, `Co_Process_Broadcast_Keyed` and three
      indistinguishable `Sink:_Writer`s. Both alert counters sit on
      `Co_Process_Broadcast_Keyed`. This is #148's premise confirmed by
      evidence rather than reasoning, so **#148 must land before #151** —
      dashboards and alert annotations built on these labels would be
      unreadable. See D68
- [x] #147 JSON structured logging (`log4j-layout-template-json`) with the
      standard field set. Applied via `spec.logConfiguration` in the
      FlinkDeployment, not the in-JAR `log4j2.properties` (which the cluster
      never reads). Drops the `/tmp/adherence-job.log` file appender
      — done 2026-08-12: console output is JSON carrying `timestamp`,
      `severity`, `message`, `logger`, `thread`, `service`, `jobName`,
      `imageTag`, `exception` (className/message/stackTrace) and MDC flattened
      to top level. Verified in Cloud Logging, not just `kubectl logs`: a
      `jsonPayload.service="rx-vigilance"` query returns parsed entries, and
      GCP promoted `severity` to the entry level, so severity filters work.
      `imageTag` is baked from `GIT_SHA` at build time (`ARG`/`ENV` in the
      Dockerfile, `--build-arg` in both workflows) and confirmed carrying a
      real commit SHA — nothing at runtime knows which commit built the image.
      The rolling file appender deliberately stays `PatternLayout`: it is what
      the Flink Web UI's Logs tab reads, JSON is unreadable there, and it never
      leaves the pod so it costs nothing in Cloud Logging.
      **Four traps, each of which cost a deploy cycle and none of which are
      visible locally — see D69.** (1) The cluster reads
      `/opt/flink/conf/log4j-console.properties`, not the in-JAR config.
      (2) `logConfiguration` is a **sibling** of `flinkConfiguration`; nested
      inside it the operator records `spec.logConfiguration=null` and silently
      applies Flink's default, with no error anywhere. (3) The operator mounts
      a ConfigMap over `/opt/flink/conf`, shadowing anything the image writes
      there — the template had to move to `/opt/flink/usrlib`. (4) Log4j
      resolves **parent-first**, so Flink's bundled 2.17.1 wins over ours;
      `log4j-layout-template-json:2.23.1` died with `NoSuchMethodError` on
      `Strings.toRootLowerCase`. New property `${flink.log4j.version}` = 2.17.1
      pins log4j add-ons to the oldest log4j in play, so they work on the
      cluster and locally.
      `JsonLogLayoutTest` (4 tests) guards the template — nothing compiles a
      JSON resource, and it now runs at the version production runs, which it
      did not before trap 4. 132 tests green.
      **`log4j-jul` added** so the GCS connector's `java.util.logging` output
      goes through log4j too, activated by
      `env.java.opts.all=-Djava.util.logging.manager=...` (a JVM flag, because
      JUL reads it when its LogManager first initialises — too early for any
      config file), plus `logger.gcs.level = WARN` to silence its chatty
      latency INFO. `docker-entrypoint.sh` output stays plain text and always
      will: it precedes the JVM. **#153's runbook must say so** — a query
      filtering on `jsonPayload.service` silently misses those lines.
      **Two findings for later issues.** GKE's logging agent already attaches
      `labels."k8s-pod/component"`, `k8s-pod/app`, `resource.labels.pod_name`
      and `container_name` to every entry, so **#148 must not put `component`
      in MDC** — that field is free, and duplicating it creates two values that
      can disagree. And §9's "verify masking against real logs rather than
      trust it" is now **verified**: Confluent's `AbstractConfig` dump shows
      `basic.auth.user.info = [hidden]` and `bearer.auth.token = [hidden]`.
      #153 can record that as evidenced
      — **correction 2026-08-12 (see #172, D72)**: the verification recorded
      above was true of the log *format* and false of the job. The same deploy
      that produced those JSON logs also set
      `flinkConfiguration.env.java.opts.all` to the JUL property alone, which
      **replaces** Flink 1.18's default value — the JDK-17 list of 8
      `--add-exports` and 12 `--add-opens`. Without
      `--add-opens=java.base/java.util=ALL-UNNAMED`, Kryo cannot reflect into
      JDK collections, so `chronic-class-filter`'s operator `ListState` failed
      to deserialize on every restore. The job crash-looped for ~13 hours —
      470 restarts, 26,288 `JobException`s, zero records processed — while
      printing exactly the JSON this task was verifying. #148 and #149 both
      merged into the already-broken cluster and were not implicated. Fixed in
      PR #171 by moving the property to `JAVA_TOOL_OPTIONS`, which the JVM
      applies additively. The lesson is recorded as D72 and is the reason
      "verified in Cloud Logging" is no longer accepted as evidence that a job
      runs
- [x] #148 `.name()` on every operator (savepoint-safe; `.uid()` is what
      drives state) + real logging in operators, sources and sinks
      — done 2026-08-12: 142 unit + 11 integration tests green
      (`mvn clean verify`). **The naming half was already complete** and was
      verified rather than redone: every `.uid()` in `AdherenceJob` and both
      watermark operators has a matching `.name()`, and the three Kafka
      sources take their operator name from the third argument of
      `env.fromSource(source, strategy, sourceName + "-source")`, which is
      why they looked unnamed in a `.name()` grep. Only `SmokeJob` still has a
      bare `.uid()`; it is not deployed and was left alone.
      **The logging half** covers six places. `AdherenceJob` logs one INFO
      config summary on the JobManager at submission — named fields only,
      never the `ParameterTool`, which carries `KAFKA_SASL_PASSWORD`, and
      `saslConfigured` as a boolean so a cloud auth failure is one line to
      diagnose. Both broadcast operators log a running entry count at INFO
      (first entry, then every 500th): an empty broadcast is the nastiest
      silent failure in this job — `ChronicClassFilterFunction` buffers every
      event forever, `AdherenceProcessFunction` silently gives every key the
      default lead time, and neither moves a counter.
      **The real payload is that every silent `return` now states its
      reason.** There were five: a fill that changed no coverage
      (`duplicate-or-fully-covered`), a reversal with no state, a reversal
      matching no interval, a timer firing at or after projected exhaustion
      (spec step 2), and a stale timer — the last split into `no-state`,
      `no-active-timer` and `timer-superseded`, because only the first two are
      suspicious and the third is the normal refill case. "We sent claim X and
      got no alert" is answerable from the log for the first time.
      Timer lines carry both `timerTs` (epoch millis, matching the Flink UI's
      watermark display) and `timerAt` (an `Instant`), since comparing a timer
      to a watermark is the first move in any stalled-timer investigation
      (§10). Alert emissions carry `alertId`, which required hoisting the
      inline `UUID.randomUUID()` into a local — the id in the log is now the
      id on the Kafka message, so a log line and an alert can be joined.
      `DeadLetterSplitFunction` finally spends #149: a WARN with the Kafka
      coordinates and `rawBytesLength`, never the payload (§9), sampled at
      first-then-every-100th because the dead-letter topic holds the complete
      record and the log is only a pointer to it. Both Kafka builders log
      their resolved topic at INFO — sinks with `transactionalIdPrefix`,
      sources with `groupId` and starting offsets.
      **PHI discipline held throughout**: `memberId` appears at DEBUG only,
      never INFO or above, and `rawBytes` appears nowhere at any level.
      New test helper `LogCapture` (src/test/java) attaches an appender and
      raises only the `com.healthcare.rxvigilance` logger for the duration of
      one test, so a DEBUG assertion does not leave the suite at DEBUG or
      drown the build in Flink's own output. Eight new tests assert the
      lines; two of them earn their keep beyond logging — the stale-timer test
      is the only one that drives the orphaned-timer branch, and the lapsed
      test is the only one that proves the LAPSED stage clears its timer (§4).
      See D71 for the two bullets from the issue that were deliberately not
      implemented
- [x] #149 Kafka topic/partition/offset carried through `KafkaSourceResult`
      and `DeadLetterRecord` to dead-letter **headers** — additive, no Avro
      schema, no registry one-way door (D67)
      — done 2026-08-12: 134 unit + 11 integration tests green
      (`mvn clean verify`). New record `KafkaCoordinates`
      (topic/partition/offset/timestamp) is captured in
      `TypedAvroDeserialisationSchema.deserialize` — the last point where the
      `ConsumerRecord` still exists — and attached to **every** result, not
      only failures, because #148's per-record DEBUG logs want it too. It
      travels on `KafkaSourceResult` via `withCoordinates(...)`, deliberately
      kept out of the `success`/`failure` factories so the Avro decoder never
      learns where on the broker its bytes came from.
      Four headers (`source-topic`, `source-partition`, `source-offset`,
      `source-timestamp`) are written next to `error-message`, and **skipped
      entirely when coordinates are null** — a `DeadLetterRecord` built
      outside the source path has no position, and an absent header is
      distinguishable from the string `"null"`. `error-message` is now
      null-guarded too: the previous serializer called
      `errorMessage().getBytes()` directly, so a null message would have
      thrown inside the sink — a failure while reporting a failure.
      `toString` prints `rawBytesLength` and coordinates but never the
      payload (§9), asserted with `doesNotContain`. `equals`/`hashCode`
      include coordinates, so two dead letters from different offsets no
      longer compare equal.
      Verified end to end by
      `deadLetterCarriesTheSourceCoordinatesOfTheMessageThatFailed`, which
      produces a poison message and asserts the side output's coordinates
      match the `RecordMetadata` the producer returned — the only test that
      exercises the capture path rather than hand-building the record.
      **Trap, cost the build: Java records are `GenericTypeInfo` in Flink
      1.18, so Kryo cannot serialize a newly added nested record unless
      `RecordKryoSerializer` is registered for it — see D70.**
- [x] #150 New application metrics (`deadLetterRecords`,
      `duplicateClaimIdDropped`, `reversalWithoutOriginal`,
      `timersRegistered`/`timersFired`, `activeKeys`,
      `broadcastEntriesLoaded`, `pdcSnapshotsEmitted`) + three RocksDB
      metrics. `IntervalMerger` keeps zero Flink imports, so its two counters
      are incremented caller-side
      — done 2026-08-14: 150 unit + 11 integration tests green
      (`mvn clean verify`). Six counters and one gauge added, `activeKeys`
      dropped — see **D73** for all four design decisions.
      **`IntervalMerger` is now a pure function.** It signalled "nothing
      happened" by returning the *same object* it was given, and the caller
      checked `merged == currentState`. That protocol was invisible, one bit
      wide, and broke in the dangerous direction: a defensive copy anywhere in
      the merger would have made every duplicate claim register a timer and
      double-count coverage, with no test failing — the two tests guarding it
      asserted `isSameAs`, pinning the pointer trick rather than the contract.
      `merge`/`unwind` now return `MergeResult`/`UnwindResult` carrying an
      outcome enum, kept as two enums rather than one so `merge` *cannot*
      report a reversal reason. Both `LOG.warn`s moved to the caller, which
      has the drug class the merger never had, and the merger lost SLF4J
      entirely. `IntervalMergerTest`'s helpers assert `APPLIED` on every
      arithmetic test, so all seven now cover the outcome for free.
      **Metric registration became lazy.** `register()` created all four
      counters and each caller took the one or three it wanted, so
      chronic-class-filter published three permanent zeros. At eleven metrics
      across five operators that would have been ~40 junk series for #151 to
      filter. Counters are now memoised on first request; the map also makes a
      repeated accessor call idempotent, which matters because Flink's
      `MetricGroup` rejects a second registration of the same name and returns
      a counter nothing reports. `count(name)`/`gaugeValue(name)` throw on an
      unknown name — a typo in a test assertion would otherwise read as "the
      counter never moved" and pass green. This replaced what would have been
      five more `…Count()` accessors, holding the line D46 drew.
      **`DeadLetterSplitFunction`'s log-sampling field is now the metric**:
      same semantics (per subtask, reset by `open()` on restart), one number
      instead of two. `deadLetterWarningsAreSampledAfterTheFirst` asserts 100
      dead letters against 2 log lines and a counter of 100 — coupling those
      would have under-reported bad data 50x on the dashboard.
      **Metric-name contract, read off the live `/metrics` endpoint
      2026-08-14** (TaskManager `rx-vigilance-taskmanager-1-1`), not assumed.
      All eleven application metrics carry the prefix
      `flink_taskmanager_job_task_operator_adherence_` and are separated by
      the `operator_name` label:
      `adherence_process` — `missingLeadTimeLookup`, `gapRiskAlertsEmitted`,
      `lapsedAlertsEmitted`, `duplicateClaimIdDropped`,
      `reversalWithoutOriginal`, `timersRegistered`, `timersFired`,
      `pdcSnapshotsEmitted`, `broadcastEntriesLoaded`;
      `chronic_class_filter` — `chronicFilterDropped`,
      `broadcastEntriesLoaded`;
      `{rx_fill_events,ndc_drug_class_ref,alert_lead_time_ref}_dead_letter_split`
      — `deadLetterRecords`, three series, one per source, exactly as intended.
      **Lazy registration is confirmed working**: `chronic_class_filter`
      publishes only its two metrics, and the alert counters no longer appear
      on it as permanent zeros.
      **The RocksDB names are not what the config keys suggest**, and this is
      #146's lesson repeating — `column_family` appears both as a scope segment
      *in the name* and as a label:
      `flink_taskmanager_job_task_operator_column_family_rocksdb_{estimate_num_keys,
      estimate_live_data_size,estimate_pending_compaction_bytes}` with
      `column_family="adherence_state"`, `"_timer_state_event_user_timers"` or
      `"_timer_state_processing_user_timers"`. A panel written as
      `..._rocksdb_estimate_num_keys` returns nothing. Only the keyed operator
      has column families; `chronic_class_filter`'s buffer is operator state,
      so it has none.
      **State of the job at capture time, which #151 must account for**: every
      watermark is `-9.223372036854776E18`, `sourceIdleTime` ≈ 2.3M ms,
      `records_consumed_total` is 0 and `estimate_num_keys` is 0. The offsets
      separate two different facts that are easy to conflate: both reference
      topics show `committedOffset = 1`, so the drug-class and lead-time data
      **was** consumed and is held in state, while `rx-fill-events` shows
      `committedOffset = -1` on all three partitions — nothing has ever been
      produced to it. The job is correctly configured and idle, waiting for
      fills. #151's "dashboards render live data" exit criterion therefore
      needs a producer for `rx-fill-events`, not for the reference topics.
      **`broadcastEntriesLoaded` also reads 0, and that does not mean the
      broadcast is empty — the gauge is defective. Filed as #175.** It counts
      entries *arriving* via `processBroadcastElement` since `open()`, and a
      restore repopulates broadcast state without any such call, so it reads 0
      on a healthy restart exactly as it would on a genuinely empty broadcast.
      That is the one distinction #150 added it to make. An earlier version of
      this note read the 0 as "the reference data never loaded"; that was wrong
      and is corrected here. #152 must not build an alert on this gauge until
      #175 is resolved, and any #151 panel using it must be labelled an arrival
      rate rather than a fill level

**Cloud — billing starts here**
- [ ] #151 Four Grafana dashboards (support, developer, platform, business),
      provisioned as ConfigMaps so they survive a Grafana pod restart
- [ ] #152 `alertmanager-rules.yaml`: seven critical + eight warning rules,
      with `for:` durations, inhibition (JobDown suppresses its symptoms),
      spot preemption at warning only, and full annotations
      (summary/description/impact/dashboard/logQuery/runbook/firstStep).
      Includes the induced watermark-stall drill
- [ ] #153 `observability/RUNBOOK.md` covering all 40 failure scenarios +
      PHI log-hygiene enforcement (`LogFields`, `LogSafe`, hashed `memberRef`,
      a test asserting no PHI at INFO or above)

**Exit criteria**:
1. Flink metrics scraped by Prometheus; all four existing counters visible
2. All logs JSON in Cloud Logging with the full standard field set
3. Every operator has both `.uid()` and `.name()`; metric labels readable
4. A dead-letter message traceable to its exact source topic/partition/offset
5. All four dashboards render live data and survive a Grafana pod restart
6. All critical alert rules exist with complete annotations and pass
   `promtool test rules` in CI
7. Induced-stall drill passes — panel shows it, `WatermarkStalled` fires
8. `RUNBOOK.md` covers all 40 failure scenarios
9. Log-hygiene test green: no PHI at INFO or above
10. Every failure scenario maps to ≥1 metric, ≥1 panel, ≥1 runbook section
11. `mvn clean verify` green; Sonar gate green

**Deliberately out of scope**: #94 (broadcast streams gate the operator
watermark) — this phase makes it *visible*; the fix is pipeline logic and
stays a separate issue. Also out: distributed tracing, Loki, GCP Managed
Prometheus, automated remediation, SLO/error-budget dashboards.

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
| D7 | 2026-07-17 | GKE runtime: single-node zonal pool, e2-standard-4 **spot**, + GCP budget alert | GCP free-trial budget (₹28,016 / 50 days at start of Phase 1); ~13.3 GB Allocatable holds the full stack (~9.8 GB requests incl. TM RocksDB budget); spot ≈70% cheaper; preemption acceptable on a self-healing demo cluster. **Superseded by D53 (2026-08-08): node count 1 → 2.** The cost reasoning was correct while credits were the scarce resource; #101 revisited it once ~₹27,000 of credits were due to expire 2026-09-05, at which point unspent credit, not spend, became the waste. Machine type, spot and zonal choices all still stand |
| D8 | 2026-07-17 | Terraform split by lifecycle: `platform/` (Redpanda topics/ACLs/subjects + GCS, persistent) vs `runtime/` (GKE + Helm, disposable); one-click `make infra-up`/`infra-down` on runtime only; Kafka Secret created from env vars by infra-up script | `terraform destroy` must never delete topics or checkpoints (spec: checkpoints survive teardown); split makes destroy-when-idle cost discipline mechanical, not careful |
| D9 | 2026-07-18 | Budget alert amount ₹25,000 (not the full ₹28,016 trial credit) + extra 20% threshold rule (D7 amendment) | Deliberate safety margin below the trial credit; 20%/50%/80%/100% thresholds give an earlier warning ladder |
| D10 | 2026-07-18 | GKE cluster named `vigilance-rx-gke` (spec says `rx-vigilance-gke`) — accepted deviation; single-node pool kept fixed at 1 (no autoscaling to 2) | Name immutable post-create and user chose to keep it; spec left as-is, this row is the record. Autoscaling max=2 considered and rejected: a hard single node makes Capacity-vs-Allocatable sizing mistakes fail loudly (Pending pod) instead of silently doubling spend |
| D11 | 2026-07-18 | Redpanda serverless cluster itself Terraform-managed in `platform/` (`rx-vigilance`, AWS `us-east-1`) — created, not clicked | GCP-backed serverless is beta-gated; cross-cloud latency irrelevant at 12–15 ev/s; cluster in platform stack = survives runtime destroys (D8), `allow_deletion=false` |
| D12 | 2026-07-18 | `cleanup.policy=compact` on `ndc-drug-class-ref` + `alert-lead-time-ref` (cloud); backport to local bootstrap as separate issue | Broadcast state is rebuilt from the full topic on every job start; with delete-policy retention, ref records would age out and the chronic-class filter would silently discard events |
| D13 | 2026-07-18 | Redpanda provider `~> 2.1` (from `~> 1.0`); `redpanda_schema` uses cloud Bearer auth (no username/password); deprecated `cluster_api_url` attribute kept in use | v1.9.0 `redpanda_schema` can't read serverless clusters (provider issue #338, fixed v2.0.0); `password_wo` unusable at refresh time per provider warning; deprecated attr still present in 2.1.x — accepted with warnings |
| D-open-10 | — | **Proposed** (2026-07-19): Phase 10 deploy path via Argo CD GitOps (CI commits manifest, Argo reconciles) instead of spec's direct `deploy.yml` patching | User wants enterprise-pattern learning; decide at Phase 10 epic creation — capacity (D7 single node) and slim-install vs Flux to be resolved in that plan. Issue filed. **RESOLVED 2026-08-08 — adopted, see D54** |
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
| D33 | 2026-08-02 | `AdherenceState` gains a new field, `TimerStage activeTimerStage` (new enum with values `GAP_RISK` and `LAPSED`), so `onTimer` can tell which of the two cascaded timers just fired, rather than inferring it by comparing the firing timestamp against `currentSupplyEndDate` | The comparison-based alternative is silently fragile: nothing in the code prevents a real (non-default) `LEAD_TIME_DESCRIPTOR` entry from being configured with `alertLeadDays = 0`, which would make the gap-risk and lapsed timers' timestamps coincide and impossible to distinguish. An explicit field is correct regardless of any `alertLeadDays` value ever configured. This is a state-schema change (`CLAUDE.md` §11 stop-and-ask trigger) touching `AdherenceState` construction sites in already-merged code — `IntervalMerger.merge()`/`.unwind()` (Phase 3) and `AdherenceProcessFunction` (Phase 7) — but the real-world cost is low right now: this state has never been part of a live savepoint, since no phase has deployed or wired the full job yet (Phase 9). The cost of this exact change rises sharply once that's no longer true |
| D34 | 2026-08-02 | A reversal that leaves zero coverage (the binding correction guarantee) emits `LapsedAlert`, not `GapRiskAlert` | `LapsedAlert` matches the member's actual post-reversal state — zero coverage, as of now, not a future risk — and fits the schema cleanly: `lapsedOn` gets the reversal event's own date (the date the correction became known), a genuinely meaningful value. `GapRiskAlert` would need artificial values for both `expiresOn` (no real future exhaustion date exists to project — coverage isn't "about to" run out, it already has) and `leadDays` |
| D35 | 2026-08-02 | `AdherenceJob`'s operator UIDs extend Phase 5's existing `KafkaTypedSourceBuilder` convention (`"{topic-name}-source"`/`"{topic-name}-dead-letter-split"`) rather than a new scheme: `chronic-class-filter`, `adherence-process` (both class-name-shortening, matching how `chronic-class-filter` itself was already derived), and `gap-risk-alerts-sink`/`lapsed-alerts-sink`/`pdc-snapshots-sink`/`dead-letter-sink` (topic-name + `-sink`, mirroring sources' `-source` suffix) | Consistency with an already-established, already-shipped convention beats inventing a second naming scheme. These are frozen the same way `MapStateDescriptor` names are (`CLAUDE.md` §4) — once a real job runs with them, changing any one breaks savepoint restore — recorded here for the same reason the descriptor names get called out explicitly |
| D36 | 2026-08-02 | `DrugClassRefKafkaSource`/`AlertLeadTimeKafkaSource` gain an explicit `WatermarkStrategy.noWatermarks().withIdleness(watermarkConfig.idleness())` (reusing the existing `watermark.idleness.ms` knob from D21, not a new config key), applied the same way `RxFillEventSource` already applies `RxFillWatermarkStrategy` | Found during first live end-to-end run: both broadcast sources went through `KafkaTypedSourceBuilder`, which defaults every source to `WatermarkStrategy.noWatermarks()` with no idleness. A two-input operator's (`ChronicClassFilterFunction`, `AdherenceProcessFunction`) output watermark is the min across all inputs; a channel under bare `noWatermarks()` never emits a watermark and is never marked idle, so it sits at `Long.MIN_VALUE` forever — permanently blocking every event-time timer downstream, indistinguishable at first from a hung job. This is the same class of bug CLAUDE.md §4/§10 already calls out for the main source ("the #1 silent failure"), just missed on the two reference sources because `RxFillEventSource` alone applied the fix explicitly |
| D37 | 2026-08-02 | `ChronicClassFilterFunction.processElement` buffers on a per-NDC check (`broadcastState.get(event.ndcCode()) == null`) instead of D30's whole-map `isEmpty()` check; `processBroadcastElement` only flushes/removes buffered events matching the NDC just updated, re-buffering the rest, instead of clearing the whole buffer unconditionally | **Corrects D30.** D30's rationale ("this window never recurs after the first successful run") assumed the only race was cold-start (map totally empty). Found during this session's live verification that it's broader: once *any* NDC has ever landed in the broadcast map, `isEmpty()` is false forever, so a fill event for a *different*, newly-introduced NDC arriving even microseconds before its own broadcast update is evaluated immediately, found missing, and silently dropped forever (no dead-letter, no exception, no buffering) — a real operational scenario (e.g. a formulary update landing while the job is already running), not just a first-deploy edge case. The old `processBroadcastElement` compounded this: it cleared the *entire* buffer on any broadcast update, so even a correctly-buffered event could be discarded if an unrelated NDC's update arrived first |
| D38 | 2026-08-02 | `RecordKryoSerializer.write()` copies any `List`-typed record component into a plain `ArrayList` before calling `kryo.writeClassAndObject`, rather than writing whatever list instance the record holds | Found via a genuine crash-loop during this session's live verification: `AdherenceState`'s compact constructor (correctly, defensively) does `activeCoverageIntervals = List.copyOf(activeCoverageIntervals)`, so every persisted `AdherenceState` holds an immutable list. Kryo 2.24.0's `CollectionSerializer` (D22's bundled-Kryo constraint) deserializes by instantiating the same class it wrote and calling `.add()` on it — impossible for an immutable list, throwing `UnsupportedOperationException` on *any* second read of a key's state (i.e. almost every real key, not an edge case). The fix stays in the generic serializer, not in `AdherenceState`: weakening the record's own immutability guarantee to work around a 2.x-era Kryo limitation would trade a correct domain invariant for a serialization-library workaround, and any future record with a `List` field would silently reintroduce the same crash |
| D39 | 2026-08-03 | `AdherencePipelineIT`'s fixture produces a matching `AlertLeadTimeUpdate` before `startJob` (real lookup path, not D32's default-fallback path), and produces a second, later-dated `RxFillEvent` for an unrelated member/claim after `startJob` purely to advance the fill-events watermark | Two distinct issues found getting the first IT green: (1) the lead-time-ref Kafka topic didn't exist until a message was produced to it, and `KafkaSource`'s partition discovery runs once at startup — an IT that never produces to a topic before `startJob` crash-loops the whole job on `UnknownTopicOrPartitionException`, unrelated to D32's application-level default; (2) even after fixing that, `GapRiskAlert` (unlike `PdcSnapshot`) is only ever emitted from `onTimer`, gated on the two-input operator's combined watermark (`AdherenceProcessFunction` connects keyed fill-events to the lead-time broadcast). A test that sends exactly one fill event and nothing else lets *both* inputs go idle before the real watermark ever advances past the registered timer's threshold, since the fill-events channel's real `forBoundedOutOfOrderness` watermark never gets a second tick to build on. This is a test-fixture-only fix — production traffic keeps the fill-events watermark alive continuously, so D36's idleness fix on the broadcast sources is unaffected — but is exactly the scenario CLAUDE.md §5 already requires: timer tests must advance event time explicitly via watermarks, not rely on wall-clock waiting |
| D40 | 2026-08-03 | `DrugClassRefKafkaSource.build()`/`AlertLeadTimeKafkaSource.build()` now return a small result record (`DrugClassRefSourceResult`/`AlertLeadRefSourceResult`, mirroring `RxFillEventSource.RxFillEventSourceResult`) carrying both the watermarked events stream and the dead-letter side-output stream, captured via `events.getSideOutput(DEAD_LETTER_TAG)` **before** `.assignTimestampsAndWatermarks(...)` runs. `AdherenceJob` updated to consume `.deadLetters()` from these records instead of calling `.getSideOutput(...)` on the post-watermark stream | Found via `malformedRecordsOnAllThreeSourcesRouteToSharedDeadLetterTopic`: only 1 of 3 malformed records reached the dead-letter topic. Root cause: `.assignTimestampsAndWatermarks(...)` creates a new downstream operator in the graph; a side-output tag attached to the upstream `KafkaTypedSourceBuilder` operator is not visible via `.getSideOutput(...)` called on that new downstream operator — Flink doesn't throw for this mismatch, it just yields an always-empty stream, so the job ran fine and silently dropped every malformed record from the two broadcast reference sources instead of dead-lettering them. Only `RxFillEventSource` had captured the side output correctly (on `events`, pre-watermark), which is why fill-events' dead-letter path worked and the other two didn't. This is a genuine production defect (silent swallowing of malformed reference data), not a test artifact — directly violates CLAUDE.md §7 ("never swallow") |
| D41 | 2026-08-05 | `AdherencePipelineIT` produces its watermark-advancing `RxFillEvent` **after** `consumeOne(pdcTopic)` has returned, not before `startJob` and not immediately after it. Refines D39, which established *why* a second fill is needed but not *when* it must be sent | D39's placement (immediately after `startJob`) is a race, and the race is lost deterministically once the class runs all three tests in one JVM. Both broadcast sources use `WatermarkStrategy.noWatermarks().withIdleness(...)`, so their channels sit at `Long.MIN_VALUE` and hold `AdherenceProcessFunction`'s min-watermark down until they are marked IDLE and excluded. But Flink does not advance a watermark while *every* input channel is idle — so the fill channel must still be active at the moment the broadcasts idle. With only 2 fills the fill channel idles at nearly the same time, and which side idles first is decided by machine load. Evidence: solo run and both pairwise runs green, full-class run red 3/3; a probe confirmed `pdcRecords=2` (both fills read and processed, both timers registered), and extending the poll from 60s to 200s still produced nothing — so the timer never fires, it is not merely slow. Sending the advancing fill after the PdcSnapshot has been observed anchors the wait to a real event (checkpoint commit + consumer group join, several seconds) instead of a `Thread.sleep`, which Sonar rejects under java:S2925. **Production implication, not yet addressed**: the same rule applies on a real cluster at every job start and restart — with the §4-mandated `withIdleness(5min)`, event-time timers cannot fire until the two reference channels have idled. Fill traffic at 12–15/sec keeps the fill side active so it does recover, but the startup delay is real and undocumented. The clean fix is to stop letting event-time-less reference streams gate the operator watermark at all; deferred as a separate decision because §4 pins `withIdleness` and §11 makes watermark changes a stop-and-ask |
| D42 | 2026-08-05 | Coverage strategy: JaCoCo runs a **separate agent and report per test phase** (`prepare-agent` → `target/jacoco.exec` → `target/site/jacoco/`; `prepare-agent-integration` → `target/jacoco-it.exec` → `target/site/jacoco-it/`), both XML paths handed to Sonar via `sonar.coverage.jacoco.xmlReportPaths`, and the SonarCloud gate is left on Clean-as-You-Code (coverage on **new** code ≥80%) rather than an overall-coverage threshold. `AdherenceJob` is **not** added to `sonar.coverage.exclusions` — reversing that part of D14's plan — and is covered by `AdherenceJobTopologyTest` instead | `AdherenceJob` showed 0% because failsafe's `<argLine>` never included the agent, so nothing an integration test executed was ever recorded; `AdherencePipelineIT` is the only thing that calls `buildTopology`, and it runs under failsafe. Separate exec files (rather than appending to one) keep visible how much coverage comes from fast tests versus slow ones — a distinction that matters the first time someone asks why CI takes 20 minutes. D14 assumed wiring classes have "no unit-testable logic", but `env.getStreamGraph()` makes the topology assertable without executing it: `AdherenceJobTopologyTest` reaches 204/227 instructions in 0.8s, versus 211/227 for the full 23s integration test — so the exclusion was never necessary, and the test additionally guards two §4 invariants (operator UIDs, frozen broadcast descriptor names) that nothing else checked. Gating on new code rather than overall coverage avoids blocking unrelated PRs behind a legacy backlog and removes the incentive to write assertion-free tests to move a number. Also required an empty `<jacocoArgLine/>`/`<jacocoItArgLine/>` default in `<properties>`: IntelliJ does not implement Maven's `@{...}` late evaluation and passes the literal token to the JVM, which reads a leading `@` as an argfile and fails with `could not open '{jacocoItArgLine}'` |
| D43 | 2026-08-05 | The three `.map(DeadLetterRecord::from)` calls in `AdherenceJob.buildTopology` gain explicit UIDs (`rx-fill-events-dead-letter-record`, `ndc-drug-class-ref-dead-letter-record`, `alert-lead-time-ref-dead-letter-record`), and `AdherenceJobTopologyTest` gains an assertion that **no** operator in the StreamGraph has a null UID | Enumerating the graph found 3 of 22 operators without a UID — all three of these maps. §4 states "operator UIDs on every operator" without exception and calls a violation "a defect, not a style issue". Practical risk today is low, since `map` is stateless and contributes nothing to a savepoint, but an operator without an explicit UID is assigned a hash derived from graph structure, so its identity changes silently whenever the surrounding topology changes — a failure mode that only surfaces during a production restore. Per D33 nothing here has ever been in a live savepoint (the job has never been deployed), so this is the cheapest moment the change will ever be. Everything else was already covered: `KafkaTypedSourceBuilder` sets UIDs on all three sources and dead-letter splits, and Flink derives sink-committer UIDs from the writer's. The assertion matters more than the fix — the pre-existing test asserted only that 8 *named* UIDs exist, so it would not have caught these three; the new `allSatisfy` check fails the build the moment anyone adds an operator without a UID. Verified: `mvn clean verify` green, 117 unit tests (from 114) + 10 IT |
| D44 | 2026-08-05 | `SmokeJob` is retained, and its `sonar.coverage.exclusions` entry from D14 stands unchanged | Raised for review on 2026-08-05 because it is the last remaining 0%-covered class (172 instructions across `SmokeJob` and `SmokeJob.LoggingSink`) and dead excluded code is how an exclusion list grows until the quality gate stops meaning anything. User confirmed it is still needed for cloud smoke-testing (Phase 2 is not started). Note this is the opposite conclusion to D42's treatment of `AdherenceJob`: that one was un-excluded because `env.getStreamGraph()` makes topology wiring assertable without executing it, whereas `SmokeJob` is a `main()` entry point whose value is precisely that it runs against a real cluster. Revisit at Phase 2 or Phase 12 — if it has not been used by then, delete it and its exclusion together |
| D45 | 2026-08-05 | `AdherenceProcessFunctionTest`'s `setUp` keeps `harness.processBroadcastWatermark(Long.MAX_VALUE)`, and the min-watermark interaction between the keyed and broadcast inputs is accepted as **out of scope for harness tests** — it is covered only by `AdherencePipelineIT` | Reviewed on 2026-08-05 against §5 after D41, to check whether `onTimer` was genuinely unit-covered or only exercised by the 22s IT. Result: §5 is satisfied — 6 of the 7 timer-related tests advance event time explicitly via `processWatermark` and assert on full side-output contents, not counts; the 7th (`reversalToZeroCoverage…`) correctly advances nothing because that path is synchronous per D34. But the review surfaced *why* 10 green harness tests could never have caught D41: pinning the broadcast watermark to `Long.MAX_VALUE` means the broadcast input can never constrain the operator's min-watermark, which is the exact inverse of production, where both reference sources are `noWatermarks()` and therefore sit at `Long.MIN_VALUE` until marked IDLE. The line is nonetheless correct and necessary — without it no timer in this class could fire, and isolating operator logic from watermark propagation is what a harness test is for. Recorded so nobody later reads harness coverage as evidence of watermark correctness: **watermark propagation through the broadcast join has no unit test and cannot have one; `AdherencePipelineIT` is the only guard.** |
| D46 | 2026-08-06 | Metric names are promoted to public constants on `AdherenceMetricsReporter` (`METRIC_GROUP`, `CHRONIC_FILTER_DROPPED`, `MISSING_LEAD_TIME_LOOKUP`, `GAP_RISK_ALERTS_EMITTED`, `LAPSED_ALERTS_EMITTED`) and frozen by `AdherenceMetricsReporterTest`; counter *increments* are asserted through package-visible `…Count()` accessors on `AdherenceProcessFunction`, mirroring the `ChronicClassFilterFunction.droppedCount()` accessor that already existed. Rejects all three options originally listed on #92 | The risk being guarded is that a counter is renamed and a Phase 11 dashboard silently empties — the metric name is a contract with something outside this repo, exactly like a broadcast state descriptor name under §4. Testing Flink's *registration plumbing* is an indirect way to catch that and expensive: `InterceptingOperatorMetricGroup` cannot see it (`addGroup("adherence")` returns a child `GenericMetricGroup` whose registrations bypass the parent's `addMetric` override), and `TestingMetricRegistry` needs an `OperatorMetricGroup` built on it that `MockStreamingRuntimeContext` gives no way to inject. Promoting the names to constants addresses the risk directly, costs nothing, and gives Phase 11 dashboard config something to reference instead of retyping five literals that would then drift independently. Freezing a constant against its own literal is deliberate and is the same pattern already used for the descriptor names. For increments, the package-visible accessor was chosen over adding Mockito: it is not a new pattern in this codebase, and the alternative was a new dependency for one test. The cost is real and stated rather than glossed — it is test-only surface on main code, alongside the existing `currentadherenceState()` / `forceAdherenceStateForTest()` hooks; if that surface grows further it should be revisited. Two of the five new assertions are *negative* (`missingLeadTimeCount()` zero when both lookups succeed; `gapRiskAlertsEmittedCount()` zero when a stale timer fires and returns early) — over-counting is the more likely defect and inflates alert volume with no alert behind it. Also corrects the 2026-08-05 review: 3 of 4 counters were untested, not 4; `chronicFilterDropped` was already asserted in `ChronicClassFilterFunctionTest`, missed because those assertions go through `droppedCount()` rather than the counter name. Verified: `mvn clean verify` green, 118 unit tests (from 117) + 10 IT |
| D47 | 2026-08-07 | **Revises D8.** The `platform/` stack is persistent but *not* permanent: a Redpanda Cloud trial expiry destroys everything in it, leaving Terraform state describing resources that no longer exist. Recovery is `terraform state rm` of the Redpanda resources only, then a normal apply — never `terraform destroy`. Procedure recorded below | D8 split Terraform by lifecycle so `terraform destroy` could never take topics or checkpoints with it, and set `allow_deletion = false` on the cluster and topics so a destroy fails loudly. Both were right, and both became obstacles when the first trial expired on 2026-08-07: the account was gone, but state still described 25 live resources, and the protection that stops accidental deletion also blocks the cleanup. **Recovery, in order**: (1) new API credentials for the new account via `REDPANDA_CLIENT_ID`/`REDPANDA_CLIENT_SECRET`; (2) re-export `TF_VAR_redpanda_flink_password` / `TF_VAR_redpanda_test_producer_password` — these are `ephemeral`/write-only so Terraform has no memory of them and they cannot be read back from state or the console; (3) `terraform state pull > backup` before touching anything; (4) `terraform state rm -dry-run` with the 8 resource addresses (naming a `for_each` resource without an index removes all its instances), compare against `terraform state list \| grep redpanda_`, then run for real; (5) `terraform plan` must read **25 to add, 0 to change, 0 to destroy** — any destroy means it is targeting the GCS checkpoint bucket, service account or budget alert, which are still live and must not be touched. Executed 2026-08-07: new cluster `d9qk5le640j3p0dv7g0g`, 25 resources recreated, zero GCP resources affected. Two things this exposed for Phase 10: the broker and registry URLs are duplicated in `application-gke.properties` *and* `flink-deployment.yaml` and must agree — that duplication is what turned a cluster change into a hunt across files (#100 resolves it); and SASL passwords must be saved at creation time or rotated via `password_wo_version`, since nothing can recover them afterwards |
| D48 | 2026-08-07 | `redpanda_topic.topics`, `redpanda_user.flink` and `redpanda_acl.flink` switch from `redpanda_serverless_cluster.main.cluster_api_url` to `.dataplane_api.url`, matching the two `test_producer` resources that already used it. Applied **before** the D47 recreate, not after | The provider deprecated `cluster_api_url` and warns on plan; the file had drifted so that three resources used the deprecated attribute and two used the replacement, which had survived only because the original cluster was built before the deprecation. Sequencing was the real decision: `cluster_api_url` is plausibly force-new on these resource types, so changing it *after* recreating would have made Terraform want to replace the topics — and D8's `allow_deletion = false` would then block the destroy half of that replacement, forcing a second round of state surgery to escape. Making the change while the resources did not exist cost nothing and removed that scenario entirely. Caught one genuine error in the process: `dataplane_api` is an object, so `dataplane_api` alone fails with `Incorrect attribute value type` — `.url` is required. `terraform validate` is the authority here; the IDE reports "Unresolved reference" because its Terraform plugin does not load third-party provider schemas |
| D49 | 2026-08-07 | `maven-shade-plugin` gains a `ServicesResourceTransformer` and nothing else. Deliberately **not** added: signature-file filters, relocations, a `Main-Class` manifest transformer, or `createDependencyReducedPom=false` | A zip cannot hold two files at the same path, so when several dependencies ship the same `META-INF/services/*` file the shade plugin keeps one and discards the rest — silently, with no error, leaving a file that exists but has fewer lines than it should. Diagnosed rather than assumed: 5 service files were affected — `org.apache.flink.table.factories.Factory` (3 jars, 2 of 6 entries surviving), `org.apache.kafka.common.config.provider.ConfigProvider` (2 jars), and Jackson's `Module`, `ObjectCodec` and `JsonFactory` (2 jars each). Concrete proof: `jackson-datatype-jsr310` declares `JavaTimeModule` and `jackson-datatype-jdk8` declares `Jdk8Module`; the pre-fix uber-jar contained only the first. **None of the five is provably breaking the job today** — it uses DataStream not Table API, and no Kafka `ConfigProvider` indirection — so this is insurance, not a bug fix. What makes it worth doing now is where the failure lives: locally Maven puts ~100 separate jars on the classpath and every service file is intact, so this can only manifest inside the container, where the uber-jar *is* the whole classpath and reproduction is hardest. Fixing it before #99/#104 means a crash-loop on GKE is a real problem to debug rather than this one. The omissions are equally deliberate: the diagnostic found **no signed jars** on the runtime classpath, so the `META-INF/*.SF` / `*.DSA` / `*.RSA` filters most guides prescribe are unnecessary; no dependency conflict is known, so relocations would be unearned complexity; `entryClass` is set on the FlinkDeployment so a jar `Main-Class` is redundant; and `dependency-reduced-pom.xml` is already gitignored. Verified: all 5 files now contain the union of every contributing jar (`JavaTimeModule` + `Jdk8Module`, `Factory` 2→6 entries, `ConfigProvider` 3 entries), `mvn clean verify` green at 118 unit + 10 IT, uber-jar grew 195 bytes — consistent with recovered text lines and nothing else |
| D50 | 2026-08-07 | `Dockerfile` hardening plus a new `.dockerignore` written as an **allowlist** (`*`, then `!pom.xml`, `!src`) rather than a list of exclusions | Four problems, only two of which the issue anticipated. **(1)** No `.dockerignore` existed, so every build shipped the entire project to the Docker daemon — 396 MB, of which 353 MB was Terraform provider binaries under `infra/terraform/*/.terraform/` that the image never touches. Measured after the change: **18.92 kB**. The allowlist form was chosen deliberately over an exclude list: an exclude list has to be updated every time someone adds a large directory, and nobody remembers, whereas an allowlist stays correct on its own. **(2)** The runtime stage hardcoded `rx-vigilance-1.0.0-SNAPSHOT.jar`, so any version bump would break the build — and break it in CI rather than locally. Fixed by renaming to a fixed filename in the builder stage; note `target/` holds two jars after shade runs (`rx-vigilance-*.jar` and `original-rx-vigilance-*.jar`), so the glob must not match the pre-shade one. **(3)** The base image was `flink:1.18-java17`, a floating tag that follows the latest 1.18.x, while `pom.xml` pins Flink to 1.18.1 — the container could therefore run a different patch release than the job was compiled against. Pinned to `flink:1.18.1-java17`, matching the "pinned: same apply = same software" discipline already applied to every Helm chart in `helm.tf`. **(4)** `mvn clean package` ran `clean` against a `target/` that cannot exist in a fresh layer — removed as noise. `-DskipTests` stays: CI already runs `mvn clean verify`, and repeating it here would double image build time for nothing. Also added `--chown=flink:flink` so the jar is owned by the user Flink actually runs as, rather than relying on world-readable root-owned files. Verified: context 18.92 kB, jar owned `flink:flink`, and #98's merged service files confirmed present *inside the image* (`Jdk8Module` and all 6 `Factory` entries). One thing learned in the process worth recording — the jar built inside the image differs from the local `target/` jar by 322 bytes, because Maven jars embed timestamps and are not byte-reproducible by default; **checksums cannot be used to prove an image matches a local build**, so verification has to inspect contents. Flink images also ship no `unzip`/`jar`, so inspection means `docker create` + `docker cp` and reading the jar on the host |
| D51 | 2026-08-07 | `flink-deployment.yaml` now describes `AdherenceJob` rather than the Phase 2 smoke job: `upgradeMode` `stateless` → **`savepoint`** with a new `state.savepoints.dir`, and the `args` block collapsed from six entries to `--profile gke` | Three separate reasons. **(1) `stateless` discards all state on every deploy** — every member's coverage history and every pending timer. That would make Phase 9 inert: D43 gave every operator a UID specifically so savepoints restore cleanly, and UIDs only matter if you restore. The trap is that flipping the mode alone is not enough — `state.savepoints.dir` was set **nowhere** in the repo, and `--checkpoint.dir` is a different thing (automatic recovery snapshots, not deliberate pre-upgrade ones). Without it the first upgrade fails at the moment it is most needed. Now `gs://…/rx-vigilance-savepoints`, same bucket as checkpoints, different prefix. Behaviour worth knowing: `savepoint` mode needs the job **running and healthy** to snapshot before upgrading, so a crash-looping job cannot be upgraded; `last-state` would cover that but requires Kubernetes HA metadata we do not configure. **(2) Config was duplicated** across `application-gke.properties` and the manifest args, and both had to agree — D47 recorded the cost when PR #96 merged looking complete while the job still pointed at a dead broker. `JobConfig.fromArgs` already reads `--profile` and merges CLI args on top, so one flag replaces three pairs and the properties file becomes the single source. Residual cost: the file is compiled into the jar, so a URL change needs a rebuild — near-zero once #103 rebuilds on every merge, and #103 may go further by injecting the values from Terraform outputs so they are never committed. **(3) Memory deliberately not changed.** `2048m` was chosen for a job that held no state; `AdherenceJob` uses RocksDB, which allocates outside the JVM heap (~635m managed of that 2048m). Picking a new number by arithmetic would be guessing — the binding constraint is node Allocatable, which §10 warns about and which #101 measures. A comment marks it as carried-over rather than chosen, and #101 now owns the sizing. Image tag moved `:smoke` → `:main` as a readable default that #103 overwrites with a commit SHA; **nothing has been pushed to that tag yet**, so this manifest must not be applied before #104. The dead `schema.registry.basic.auth.credentials.source` line in `application-gke.properties` was left in place on purpose — it belongs to #109, where the real fix lives |
| D52 | 2026-08-08 | Registry credentials threaded through `TypedAvroSerializationSchema` and `TypedAvroDeserialisationSchema` as a `Map<String,String>` produced by a new `KafkaConnectionConfig.registryConfig()`; `AUTO_REGISTER_SCHEMAS` flipped **true → false**; the `redpanda.tf` schema list grown from three subjects to six | **The gap.** Only `SmokeJob` ever passed registry credentials. `AdherenceJob`'s two serialization schemas built `new CachedSchemaRegistryClient(url, capacity)` with no config map at all, so on Redpanda Cloud every schema lookup would have been unauthenticated. It survived this long because a local docker-compose registry needs no auth and every integration test uses a Testcontainers Redpanda that also needs none — there was no environment in which the bug could show up. **Why a Map and not the record.** `SerializationSchema` and `KafkaRecordDeserializationSchema` both extend `Serializable`: Flink ships these objects to every TaskManager. `KafkaConnectionConfig` is a plain record that does **not** implement `Serializable`, so passing it as a field would have failed at job submission, on the cluster, far from the change. `registryConfig()` is evaluated on the JobManager and only the resulting map travels. A Java-serialization round-trip test now guards each side so a future refactor back to the record fails locally instead. **Where the credentials go.** On the `CachedSchemaRegistryClient` only, not on the `KafkaAvroSerializer` config map. The client is the thing that opens HTTP connections; the serializer is handed an already-built client and never creates its own. Adding auth in both places would work but the second copy does nothing, and a config that does nothing is worse than none because the next reader cannot tell which is load-bearing. Key names (`basic.auth.credentials.source` = `USER_INFO` plus `schema.registry.basic.auth.user.info`) were copied from `SmokeJob` rather than from documentation: Confluent accepts several spellings and this pair is the one Phase 2 proved against real Redpanda Cloud. Note this is separate from `KafkaSourceUtil.securityProperties`, which authenticates to the **brokers** — two servers, two independent mechanisms, which is exactly why the broker side worked while the registry side was never wired up. **Scope deliberately widened, with approval.** Auto-register was left on until now. With it on, every serialize issues a POST to the registry even when the schema is already registered and the call is a no-op returning the existing ID — so the job needs WRITE on the registry. With it off the call becomes a lookup needing only READ. That removes a failure mode from #105 that would have been invisible until the first record was written. It also means an accidental `.avsc` edit now fails loudly instead of silently adding a version. **The missing subjects.** `redpanda.tf` claimed in a comment to mirror `scripts/bootstrap-local-topics.sh` and did not: the script registers six subjects, Terraform registered three. `pdc-snapshots-value` was absent entirely, so `AUTO_REGISTER_SCHEMAS: true` was quietly load-bearing for a sink that writes on every fill. Turning it off without fixing this would have broken the PDC sink on the cloud. All six are now Terraform-managed at `FULL_TRANSITIVE`; plan was 3 to add, 0 to change, 0 to destroy, and applied. Dead-letter needs no subject — it writes raw bytes. **The trap this exposed, worth remembering.** Every integration test invents a randomised topic name, so its subject is `gap-risk-alerts-<uuid>-value` and no external tool can pre-register it; with auto-register off the tests must register their own. The first attempt registered only the subjects each test "obviously" needed. That was wrong, and the way it was wrong matters: `onTimer`'s GAP_RISK stage registers a follow-up LAPSED timer, so a single watermark advance fires both, and the test that only expected a gap-risk alert also emitted a lapsed alert to an unregistered subject. The sink threw, the job's restart strategy is `maxNumberRestartAttempts=2147483647`, so it restarted forever and the gap-risk transaction never committed — surfacing as a 63-second **consume timeout** with nothing in the output naming a schema. Because checkpoints run every 500 ms it was a race: the test passed in isolation and failed in the suite. Rule adopted and written into the test file: any test that starts the full job registers all three Avro sink subjects, with no attempt to predict which fire. Second trap in the same area: the helper must resolve **its own** class's container, since each `@Testcontainers` class starts and stops a separate Redpanda — a static import of the other class's helper compiles fine and fails at runtime with "Mapped port can only be obtained after the container is started". The helper is therefore deliberately duplicated across the two IT classes with a comment saying why, so nobody deduplicates it later. Finally, the dead `schema.registry.basic.auth.credentials.source` property flagged in D51 was removed: nothing reads it, and `registryConfig()` now sets that key in code. Verified: 122 unit plus 10 integration tests green, run twice consecutively to rule out the race, with `AdherencePipelineIT` back to ~22 s versus 78 s while the restart loop was present |
| D53 | 2026-08-08 | GKE node pool `node_count` 1 → **2** (`e2-standard-4` spot, zonal, 50 GB disk all unchanged); TaskManager memory 2048m → **4096m**, JobManager left at 2048m. Revises D7 | **Why revisit D7 at all.** D7 chose one node when credits were the scarce resource. They are no longer: roughly ₹27,000 expires 2026-09-05 and two nodes cost about ₹4,700 across that window, so the waste is now unspent credit rather than spend. **Why two small nodes rather than one bigger one.** Capacity is the weaker argument — an `e2-standard-8` would also fix capacity. The real reason is that with a single spot node, a preemption removes cert-manager and the Flink operator along with the job, so nothing is left to reschedule anything and recovery needs a human. With two, the operator survives on the other node and reschedules, which turns a preemption into a demonstration of self-healing instead of an outage. Autoscaling was considered and rejected for now: this issue exists to produce one unambiguous Allocatable measurement, and a pool that resizes underneath that measurement makes it not one. **What was actually measured** (`kubectl describe node`, both nodes): 3920m Allocatable CPU and ~12.96 GiB Allocatable memory each. The whole platform stack — GKE system pods, cert-manager, Flink operator, kube-prometheus-stack — requests 1530m CPU and ~2.26 GiB in total across both nodes, leaving roughly 3.1 CPU and 11.7 GiB free **per node**. **Two premises in the issue turned out to be wrong.** It asserted "CPU is binding, not memory". That was true of one node; with two, neither binds. It also budgeted ~1.2–2.2 CPU for the platform components, and the measured figure sits at the very bottom of that range, so the single-node picture was less dire than estimated — the preemption argument, not the capacity argument, is what justifies the second node. **Size against one node, not the cluster total.** JobManager and TaskManager are separate pods and the scheduler places each on a single node, so the constraint is ~3.1 CPU and ~11.7 GiB, not the ~6.3 CPU and ~23.6 GiB free cluster-wide. §10 warns about exactly this; the failure mode is a pod stuck `Pending` with a scheduling event, not a crash, so it is easy to misread. **Why 4096m.** Flink derives everything from `process.size`: at 2048m the split is 256m metaspace, 205m JVM overhead, then 159m network, 635m managed and 793m task heap — the 635m of managed memory is where RocksDB lives, and it is the number D51 recorded. At 4096m the same arithmetic gives 343m network, **1372m managed** and 1715m task heap, so RocksDB roughly doubles. JobManager stays at 2048m because it holds no keyed state; it coordinates checkpoints. 8192m was rejected as buying managed memory this workload will not use at 12–15 events/sec while making rescheduling after a preemption slower. Even with both pods on one node that is 6144m of ~11.7 GiB, leaving room for Prometheus retention to grow. **REVERT TRIGGER — when credits expire around 2026-09-05**, set `node_count` back to 1 in `gke.tf`. Be aware that at one node CPU becomes binding again: the platform stack plus a 1-CPU JobManager and a 1-CPU TaskManager is about 3530m of 3920m Allocatable, which schedules but leaves almost nothing spare, so kube-prometheus-stack may have to be dropped at that point. Memory is not the problem on revert — 4096m of TaskManager still fits. **Caught in review, worth remembering:** the manifest was first written `memory: "4096"` with no unit. The operator maps that to Flink's `taskmanager.memory.process.size`, which parses as a `MemorySize`, and **a value with no unit is read as bytes** — so it would have requested 4096 bytes and the TaskManager would never have started. The giveaway was `jobManager` in the same file correctly using `"2048m"`. **For #30:** capacity is not the blocker for Argo CD. A minimal install is roughly 0.5 CPU and 1–2 GiB against ~6.3 CPU and ~23.6 GiB free, so it fits without contention; whether to adopt it remains #30's decision, deliberately not folded into this issue |
| D54 | 2026-08-08 | **Argo CD adopted** for the Phase 10 deploy path, resolving #30 and D-open-10 and deviating from `spec.md`, which describes CI patching the FlinkDeployment directly. Shape: standard non-HA install via `helm.tf`, a dedicated `AppProject`, one `Application` scoped to `path: k8s/flink` with `automated: prune + selfHeal`, a `kustomization.yaml` for image-tag edits, and `deploy.yml` triggering with `paths-ignore: k8s/**`. #102 closed as dead work | **What settled it.** #30 listed four constraints. Capacity was the blocker and D53 answered it with measurement rather than estimate: ~6.3 CPU and ~23.6 GiB free cluster-wide against Argo CD's ~0.5 CPU and 1–2 GiB, so the "core mode" fallback #30 held in reserve is not needed and the UI comes along — worth having, since seeing sync status and diffs is most of the value of adopting GitOps as a learning exercise. **Why now rather than after Phase 10.** #30 made the argument and it holds: adopting later means writing `deploy.yml` twice and doing #102 for nothing. This is the one moment where the switch costs nothing beyond the install. **Prune and self-heal are ON, and the first draft of this decision had them off.** The initial recommendation was `prune: false` as caution against #30's warning that auto-prune would delete the hand-made `kafka-credentials` and `ghcr-pull` Secrets. That warning is overstated for the default configuration: Argo CD prunes only resources carrying its own tracking label, which hand-created Secrets never have. More importantly the caution was redundant — the real mitigation is the narrow `path: k8s/flink`, which means Argo tracks the FlinkDeployment and nothing else. Turning prune off would have produced a manual sync button rather than GitOps, so the standard setting is both the correct one and the safe one here. **Verification step, not a design compromise:** after the first sync, confirm both Secrets still exist. **Loop prevention by path filter, not commit message.** CI writing the image tag back to `main` would retrigger CI. `[skip ci]` was considered and rejected: it depends on every future commit message being right. `paths-ignore: k8s/**` is declarative, lives in the workflow, and cannot be forgotten. Argo CD Image Updater was also considered and rejected — in `argocd` write-back mode git stops being the source of truth for the image tag, which defeats the purpose, and in git write-back mode it is another component to install and debug during the first real deployment, which is exactly what #30 warns against. **Two things added because standard practice calls for them and the first draft omitted them.** A dedicated `AppProject` rather than `default`, restricting source repo, destination namespace and permitted resource kinds — this is what makes "Argo can only touch this one thing" true rather than assumed. And `k8s/flink/kustomization.yaml`, so CI runs `kustomize edit set image` instead of rewriting YAML with `sed`. **One standard practice deliberately declined: HA mode.** redis-ha plus multiple `application-controller` and `repo-server` replicas is the enterprise default, and it is theatre on a two-node **spot** cluster — the nodes themselves can be preempted, so HA Argo would buy availability the substrate does not offer. Standard non-HA install, recorded here as a conscious deviation rather than an oversight. **Consequences.** #102 (Workload Identity Federation) is closed as dead work: under GitOps, CI pushes to GHCR and commits to git, both native GitHub permissions, and never reaches GCP at all — the credential it existed to avoid is no longer needed. Reopen if CI is ever required to verify cluster health directly. #103 keeps its build and push steps unchanged; only the last mile becomes a `kustomize edit` plus commit, and its acceptance criterion "job reaches a healthy state" moves from CI polling the cluster to Argo CD's own sync and health status. **Ordering trap caught before it bit.** The FlinkDeployment references the `ghcr-pull` and `kafka-credentials` Secrets and the image tag `:main`, none of which exist yet — Secrets come from #104, the first image from #103. Creating the Application today would sync straight into `ImagePullBackOff` and missing-secret failures, a confusing first impression of a tool adopted to reduce confusion. Order is therefore: install Argo CD, then #104, then #103, then create the Application, then #105 |
| D55 | 2026-08-08 | Argo CD install hardening: chart 10.3.0 (Argo CD v3.5.0) pinned, `dex` and `notifications` disabled, and explicit resource requests set on the five remaining components. Records that the whole platform stack ships as `BestEffort` | **The chart ships no resource requests at all.** After the first install, all seven Argo CD pods were `BestEffort` and the node accounting had not moved by a single byte — 1530m CPU and 2,431,081,600 bytes of requests before and after, identical. Two consequences, both bad: the scheduler believes Argo CD is free, so every free-capacity figure is overstated; and `BestEffort` pods are the first evicted under node pressure, which makes the component that decides what gets deployed the least survivable thing on a pool whose nodes are already preemptible. Fixed with a `values` block setting requests on `controller`, `repoServer`, `server`, `redis` and `applicationSet` — 650m CPU and about 1.13 GiB in total, which lands inside the estimate D54 recorded. **CPU requests but deliberately no CPU limits.** A CPU limit throttles the controller even when the node is idle, which surfaces as slow syncs that look like a git or network problem. Memory limits are set, because memory is not compressible and an unbounded leak takes the node down. **`applicationSet.enabled: false` was silently ignored.** Chart 10.3.0 has no such key — the only `enabled` under `applicationSet` is `applicationSet.pdb.enabled` — so the ApplicationSet controller ships unconditionally. Helm accepts unknown values without warning, so a values key that does nothing is indistinguishable from one that works; the only evidence was the pod still running afterwards. This is the general lesson: **verify Helm value changes by observing the cluster, not by reading the values file.** `dex` and `notifications` do have real `enabled` keys and were removed as intended — no identity provider is configured and no Slack or email routing exists. The ApplicationSet controller was given requests instead of being removed. **Discovered in passing, and larger than Argo CD:** the entire pre-existing platform stack is also `BestEffort` — cert-manager and its cainjector and webhook, the Flink operator, and six kube-prometheus-stack pods. The 1530m of CPU requests measured in #101 was therefore GKE system pods alone; none of the platform charts were in it. **This does not invalidate D53.** Measured with `kubectl top`: actual consumption is 311m CPU and about 4.5 GiB across both nodes against roughly 2180m of requests, and the ten unaccounted pods use only about 60m CPU and 1.1 GiB between them, the largest being Prometheus at 23m and 368 MiB, Grafana at 15m and 307 MiB, and the Flink operator at 5m and 385 MiB. Requests exceed actual usage by a wide margin, which is the safe direction, so the ~11.7 GiB of free memory per node and the 4096m TaskManager sizing both stand unchanged. It is a resilience problem, not a capacity one, and is therefore filed as its own issue sequenced **after** #105 rather than allowed to grow #30. A prediction recorded here because it was wrong: the gap was expected to appear in CPU and it is almost entirely memory. **Two whitespace traps, both invisible when reading.** The AppProject was rejected with `spec.description: Too long: may not be more than 255 bytes` while appearing to be about 120 characters — trailing whitespace on a line inside a YAML folded scalar written with `>` is preserved rather than stripped, and the value was actually 280 bytes. Fixed by using a single-line plain scalar, which cannot accumulate invisible trailing whitespace. Separately, one extra leading space on `server:` inside a Terraform `<<-EOT` heredoc broke the values YAML: `<<-` strips the common indentation, so six spaces became zero and seven became one, and the key parsed as a child of nothing. `terraform fmt` does not lint inside heredocs because it treats them as opaque strings, so neither error is catchable by formatting tools — comparing the indentation of sibling keys with `grep` is the practical substitute. **The AppProject is applied by hand** and documented in `k8s/README.md` beside the Secrets from #104. Not managed by Argo CD, because the object that constrains Argo CD must not be something a bad sync can widen; not managed by Terraform either, because the runtime stack configures only the `helm` provider and adding a `kubernetes` provider for one manifest is more moving parts than the problem deserves. The cost is that a `terraform destroy` and rebuild loses it, exactly as it loses the hand-created Secrets |
| D56 | 2026-08-08 | Secrets stay hand-created for now; **Google Secret Manager as the store plus External Secrets Operator as the mechanism** chosen to replace them, deferred to #118 after #105. Makefile stops applying `k8s/flink/` now that Argo CD owns it | **Why a store at all.** The Redpanda SASL password exists in exactly one place, `~/.redpanda-cloud.env` on one laptop. Terraform holds it write-only (`password_wo`) and the Redpanda console will not show it, so losing that machine means rotating via `password_wo_version` rather than recovering. That is a real single point of failure, and the question of where secrets should live was raised directly. **The criterion that decided it.** Every candidate needs a root credential to unlock it — Vault and OpenBao need unseal keys, Sealed Secrets needs its controller key backed up or every sealed value dies with the cluster, SOPS needs an age key. Each of those relocates the problem rather than solving it. GSM plus ESO is the only option that reaches **zero stored credentials**, because the ESO pod authenticates to Secret Manager through the Workload Identity already configured in `gke.tf` and receives a short-lived token derived from its Kubernetes identity. Nothing long-lived sits on any laptop. **The honest tension.** Open source was asked for and Secret Manager is not; ESO is (Apache 2.0). The judgement made was that the decisive property is not the licence of the store but whether a long-lived credential must be kept somewhere, and only this combination gets to none. It is also not a one-way door: **ESO is the abstraction**, so moving to Vault or OpenBao later means rewriting one `SecretStore` resource, leaving the `ExternalSecret` manifests and the workload untouched. Self-hosting a Vault server on a two-node spot pool with four weeks of credits remaining was judged disproportionate for two secrets. **It also resolves a standing contradiction:** §9 forbids credentials in git while GitOps wants everything in git. An `ExternalSecret` contains only a reference, so it can live in `k8s/` and be synced by Argo CD like any other manifest. **Sequenced after #105 on purpose**, by the same reasoning #30 applied to Argo CD itself: introducing a new secrets mechanism before the job is proven makes any failure ambiguous between the job, the image, Argo CD and ESO. Replacing something that demonstrably works is far easier to verify than building both at once. **A process failure worth recording.** #104 was worked as a manual walkthrough — namespace, then each Secret by hand — before noticing that `make infra-up` already did all of it, and did it better: `check-env` guards both variables with a clear message, which is exactly the failure that cost several exchanges, and `--dry-run=client -o yaml \| kubectl apply -f -` makes Secret creation idempotent where plain `kubectl create secret` fails on a resource that already exists. The Makefile also already used the correct variable name `GHCR_READ_TOKEN`. The lesson generalises: on a cluster D8 describes as disposable, assume a rebuild script exists and read it before writing instructions. `k8s/README.md` still described the superseded manual path and now points at `make infra-up`. **Two Makefile changes from D54's ownership split.** `infra-up` no longer applies `flink-serviceaccount.yaml` or `flink-rbac.yaml`, because `k8s/flink/` belongs to Argo CD from #114 and two things with authority over one resource is what GitOps exists to remove; a comment marks the omission as deliberate so it is not restored. `infra-verify` gained an `argocd` readiness wait, and its Workload Identity annotation check was made tolerant of the `flink` ServiceAccount not existing yet — that check assumed the ServiceAccount was created by `infra-up`, so removing those lines would otherwise have made the target fail on every fresh cluster until #114. **`ghcr-pull` is now documented** beside the Kafka Secret, including the recovery asymmetry: a lost GitHub PAT is regenerated freely, a lost Redpanda password cannot be recovered at all |
| D57 | 2026-08-08 | `deploy.yml` ships an image on every merge to `main`, then **opens a pull request** carrying the new tag rather than committing to `main` directly. The `production` environment gate is dropped; merging that PR is the approval. Four GitHub settings had to change | **Shape.** Build and push are ungated: a merge always produces `ghcr.io/sourabhagari/rx-vigilance:<commit-sha>`, because building an image changes nothing about what is running. The deploy half writes the tag into `k8s/flink/kustomization.yaml` and opens a PR; merging it is what Argo CD reacts to. SHA tags rather than `latest`, so the running version is always answerable and rollback is a matter of pointing at a previous tag. **Why a PR and not a push.** The original design pushed straight to `main` and was rejected with `GH006: Protected branch update failed` — `main` requires a reviewed PR. The standard remedy is to let the `github-actions` app bypass that rule, and it is **not available on personal repositories**: the API returns HTTP 500 with an empty body, and the setting does not appear in the UI. The alternative was worse — a stored personal access token would push successfully, since `enforce_admins` is false, but that is a long-lived credential and exactly what #118 exists to remove. **The `production` environment gate was therefore dropped.** Keeping both would mean approving twice for one deploy, and the PR is the better of the two: it shows a diff naming the exact SHA about to go live, where an environment button shows only a name and a hash. The environment still exists, unreferenced; it costs nothing to leave. **Four settings changed, and the reasons matter.** (1) The GHCR package `rx-vigilance` already existed, created by a manual `docker push` during Phase 2, and a package created outside Actions **does not grant the repository write access** — it must be added explicitly under Manage Actions access with the Write role. The failure surfaced as `denied: permission_denied: write_package`, which reads like a token scope problem and sends you to the workflow permissions block, where the answer is not. (2) **A correction to a claim made to the user as a lesson during this work: a workflow permissions block CAN raise permissions above the repository default.** The repo default was reverted to `read` and the push still succeeded with `packages: write` declared at job level. The earlier statement that the block can only narrow the ceiling was wrong. (3) Allow GitHub Actions to create and approve pull requests had to be enabled, or `gh pr create` fails outright. That setting is **coarse — it grants approve as well as create** — so a future workflow could approve its own PR and satisfy the single required review. Nothing does today, and `main` still requires an approving review, but the capability now exists repo-wide and is recorded here as a known trade rather than a forgotten switch. (4) Required status checks are still **not** configured on `main`, so a red PR can be merged. The gap was identified and deliberately left: the approval on the deploy PR means a human sees CI's result before anything reaches the cluster. **Loop prevention by path filter, not commit message.** Both workflows carry `paths-ignore: k8s/**` on their `push` trigger, so the deploy PR's merge — which touches only `k8s/**` — starts nothing. Verified: after merging PR #123, no run was triggered. `[skip ci]` was rejected because it depends on every future commit message being correct. The `pull_request` trigger deliberately has **no** `paths-ignore`: if `build` ever becomes a required check, a PR where it never runs could never be merged. **`yq`, not `kustomize edit set image`.** `yq` is preinstalled on runners; kustomize is not, so using it would mean piping an install script to bash or pinning a third-party action for one field edit. The `select` on image name also makes the exact match visible — the match that silently failed when the transform was first tested. Note `yq -i` rewrites the whole document and stripped the blank lines between sections, so the first deploy commit was 1 insertion and 4 deletions; subsequent ones are a clean single line. **`docker/login-action` replaced with a plain `docker login`** after Sonar flagged `githubactions:S7637`, a mutable tag on a third-party action. Removing the dependency beats pinning it for a one-line GHCR login, and `--password-stdin` keeps the token out of the process argument list. **The planned deliberate-failure test was judged already satisfied** and skipped: the pipeline went red four separate times during construction, each for a cause nobody predicted, and each stopped with the reason visible. A synthetic bad-tag test would have proved less. **Process note:** four separate failures in this issue came from invisible whitespace or broken line continuations in pasted commands — trailing spaces in a folded YAML scalar, an extra leading space inside a Terraform heredoc, a backslash followed by spaces silently splitting a `gh api` call, and `run:` indented one level too deep so YAML read it as an environment variable named run. None are catchable by formatters, which treat heredocs and scripts as opaque. Prefer single-line commands when pasting, and verify structure by parsing rather than reading |
| D58 | 2026-08-09 | **A GitHub App token replaces D57's deploy PR.** `deploy.yml` now writes `newTag` and pushes straight to `main`; the `Application` and `AppProject` moved into `make infra-up` | **What changed and why it is not a reversal of D57's reasoning.** D57 pushed to `main`, hit `GH006: Protected branch update failed`, found that the `github-actions` app cannot be granted bypass on a personal repository, and refused a PAT because it is a long-lived credential. It settled on a deploy PR as the least-bad option. A GitHub App reaches the same place without the thing D57 objected to: `actions/create-github-app-token` mints a token per run that expires within the hour, and the App can hold a branch-protection bypass allowance where the built-in Actions identity cannot. The credential objection is answered rather than tolerated, and it moves in the same direction as #118 — fewer stored secrets, not more. Requires `APP_ID` and `APP_PRIVATE_KEY` as repository secrets, the App installed with Contents: read and write, and the App listed in `main`'s bypass allowances; a miss on any of the three fails only at the final push step. Loop prevention is unchanged and still `paths-ignore: k8s/**`, since the bot commit touches `kustomization.yaml` alone. **Verified end to end 2026-08-09**: PR #131 merged as `24b8a45`, workflow green, bot commit `437b9ce`, and no run triggered by the bot commit. **Second half of this decision: Argo CD's own bootstrap belongs in the Makefile.** #30 applied the `AppProject` by hand, and the cluster rebuild on 2026-08-09 came up with Argo CD installed and no permission to act — the AppProject had died with the cluster and nothing recreated it. Both it and the `Application` are now applied by `infra-up`, after the Secrets and before `infra-verify`; ordering matters, because an Application that syncs before `kafka-credentials` and `ghcr-pull` exist lands straight in `ImagePullBackOff`, which is D54's ordering trap relocated into the Makefile. Deliberately **not** Terraform: `kubernetes_manifest` resolves its CRD at *plan* time, and the Argo CD CRDs arrive during the same apply that installs the chart, so a fresh apply from empty state cannot plan them — the same class of trap as the helm-provider-on-fresh-cluster edge in §10. **One thing Argo CD did not give us.** Its `Healthy` status read green three separate times while the job was in fact broken: when the FlinkDeployment did not exist at all, and twice while the job was in a restart loop. Health here reflects the resources it can see, not the workload's behaviour. `kubectl get flinkdeployment` and the JobManager log are the signals worth trusting; Argo CD's health is not evidence for a phase exit criterion |
| D59 | 2026-08-09 | **Two defects reached the cluster that no local test could catch: a missing `TRANSACTIONAL_ID` ACL (#130) and an unset `transaction.timeout.ms` on the dead-letter sink.** Both are consequences of EXACTLY_ONCE sinks meeting an authenticated broker for the first time | **Defect one — authorization.** All four sinks run `DeliveryGuarantee.EXACTLY_ONCE`, which means Kafka transactions, which are authorized on the `TRANSACTIONAL_ID` resource type — separate from topic WRITE. `redpanda.tf` granted topic READ/WRITE and GROUP READ and nothing else, so the job authenticated, connected, read its sources, and failed the moment a sink called `initTransactions()`. Fixed with one PREFIXED WRITE on `rx-vigilance-`, matching `setTransactionalIdPrefix("rx-vigilance-" + topic)`; a single entry covers all four sinks, exactly as the existing PREFIXED GROUP entry does. **Defect two — transaction timeout.** With the ACL in place the next error was `The transaction timeout is larger than the maximum value allowed by the broker`. `KafkaTypedSinkBuilder` had always set `transaction.timeout.ms` to 60000, and the three alert sinks route through it. `AlertKafkaSinks.deadLetterSink` built its own `Properties` and set only the security keys, so Flink's `KafkaSink` default of **one hour** applied, against Redpanda's 15-minute `transaction.max.timeout.ms`. One sink out of four diverged because nothing forced them to agree — the same hazard D47 recorded for the broker URL, in a different file. The fix is not the missing line but the removal of the second construction site: `KafkaSourceUtil.producerProperties(config, params)` is now the only place a sink's producer config is built, so a future sink gets the timeout by construction rather than by remembering. **Why the test suite was green throughout.** The integration tests run Testcontainers Redpanda with no authentication and no transaction limit, so neither the ACL nor the timeout can fail there. This is the same shape as #109 — a defect class that only an authenticated cloud broker can surface — and it is now the second time it has cost a debugging session. The new unit tests assert that `transaction.timeout.ms` is **present** by default, that the parameter override still wins, and that SASL properties survive the delegation; the third exists because dropping the `securityProperties` call would break the three working sinks while every other test still passed. **The 60000 value is evidence-based, not chosen.** It is the number the three alert sinks had already used successfully against this broker, which is why it was kept while the job was down rather than raised to something more comfortable. It deserves a second look: 60s of transaction timeout against a 30s checkpoint interval means a restart lasting over a minute expires in-flight transactions. That is a separate decision needing the broker's actual `transaction.max.timeout.ms`, and the `// note: this needs to be set dynamically based on checkpoint behavior` comment that used to sit in `KafkaTypedSinkBuilder` is really about this — filed as **#133**. **Also outstanding**: the checkpoint directory is now duplicated between `application-gke.properties` and `flink-deployment.yaml`, because the operator's validating webhook needs it in `flinkConfiguration` before the JVM exists while `CheckpointConfig` requires it as a job parameter. Single-sourcing means having the job read the directory from the Flink configuration it is handed — a code change, filed as **#132** |
| D60 | 2026-08-09 | **`CloudEventPublisher` (test scope) is how cloud topics get test data**, because the Redpanda console cannot write Avro. Recorded together with the process failure that made it necessary: #105 was ticked done on infrastructure evidence while half its acceptance was untested | **Why a program and not the console.** The console's publish dialog offers null, text, JSON and binary — no schema-registry Avro. The job reads Confluent-framed Avro, so a JSON message would fail to decode and land in `dead-letter`, which would look like a job defect and would also fail #105's own "dead-letter is empty" check. Binary would mean hand-encoding the Avro body plus the five-byte registry header, which is not realistic. The console reads Avro perfectly well, so it stays the right tool for *verifying* — just not for producing. **Shape.** `src/test/java/.../integration/CloudEventPublisher.java`, test scope so it never enters the shipped image, reusing the `GenericRecord` construction already proven in `AdherencePipelineIT`. All four connection values come from environment variables (§9); the run is driven by a `refs`/`fills` argument rather than one run with a pause between the two halves. That argument exists for a Sonar finding — `java:S2925`, "Thread.sleep should not be used in tests" — and the rule was right for the wrong reason: the pause was there so reference data would reach the job before the fills, and since the reference topics are **compacted**, publishing them once is permanent and the job re-reads them from the beginning on every start. `refs` is therefore a one-time setup and `fills` is the repeatable part. Two settings are easy to miss and both come from earlier decisions: the registry connection needs its own credentials separate from the broker's SASL (D52), and `auto.register.schemas` must be **false** to match what #109 turned off in the job — a producer that auto-registers writes a schema version nobody expected. **The data that produces an alert** is four messages: NDC → `("DIABETES", trackable=true)`; `DIABETES\|RETAIL` → 10 lead days; a fill backdated 45 days so the alert instant is already past; and a second fill dated today whose only job is to move the event-time clock, since timers fire on watermarks and not on wall clock. Both fills go to **partition 0 deliberately** — the watermark is the minimum across partitions, so splitting them lets the older one hold the clock back until idleness clears it. **The process failure worth keeping.** #105's four acceptance checks were: healthy deployment, a checkpoint visible in GCS, an alert observed end to end, and no dead-letter traffic. Only the first two had evidence when the issue was ticked and the ledger updated, and the entry was corrected only because the user asked to revisit #105 before moving on. The evidence that should have caught it was already on screen: every checkpoint was exactly 33544 bytes, and constant state size means nothing is flowing. **A running Flink job proves the plumbing, not the product** — and D58 already recorded Argo CD's `Healthy` misleading us three times the same day. Both point at one rule: for a streaming job, "it works" means data went in and the right thing came out, and nothing weaker counts. **Result**: alert observed for the M001 fill, `dead-letter` empty, and the wait was **5 minutes** — D41's predicted `withIdleness(5min)` delay, confirmed against the real cluster and recorded on #94 |
| D61 | 2026-08-09 | **Resource requests set on cert-manager, the Flink operator and kube-prometheus-stack**, finishing what D55 started for Argo CD. Chart value keys were read from the charts themselves before writing any of them | **The problem restated plainly.** A pod that declares no resource requests is `BestEffort`. Two consequences: the scheduler counts it as needing nothing, so every free-capacity figure is optimistic; and under node pressure the kubelet evicts `BestEffort` pods first. On this cluster that meant the Flink operator — the component that restarts and upgrades the job — and Prometheus and Grafana — the components that would report a problem — were first in the queue to be killed, on a **spot** pool whose nodes are already preemptible by design. The ordering was exactly backwards. **This is resilience, not capacity, and D53 still stands.** The ten pods use about 60m CPU and 1.1 GiB between them; the requests added reserve about 460m and 2 GiB against roughly 23 GiB free across two nodes. Nothing was made to fit; what changed is who dies first. **Method, and it is the point of this entry.** D55 recorded that `applicationSet.enabled: false` was silently ignored because chart 10.3.0 has no such key — Helm accepts unknown values without warning, so a values file that does nothing looks identical to one that works. So every key here was read out of the chart with `helm show values` before being written: `resources`, `webhook.resources` and `cainjector.resources` for cert-manager; `operatorPod.resources` and `operatorPod.webhook.resources` for the Flink operator; `prometheusOperator.resources`, `prometheus.prometheusSpec.resources` and `alertmanager.alertmanagerSpec.resources` for kube-prometheus-stack. The last three — `grafana`, `kube-state-metrics` and `prometheus-node-exporter` — are **subcharts**, so their keys do not appear in the parent chart's values file at all and pass straight through; that is standard Helm behaviour but it looks identical to a typo until the cluster is checked. **Verified by observation, not by reading the file**: `kubectl get pods -A` with the QoS column returns `Burstable` for all eleven pods and `grep BestEffort` returns nothing cluster-wide. **Sizing follows D55's rule: CPU requests but no CPU limits, memory requests and limits.** A CPU limit throttles a pod even when the node is idle, which shows up as unexplained slowness; memory cannot be reclaimed once taken, so it needs a ceiling. Values are the measured usage in #115 roughly doubled — the largest are Prometheus and the operator at 512Mi requested against 368Mi and 385Mi actual. **Restart behaviour worth knowing.** Changing Helm values rolls every affected pod. The Flink operator was replaced and the running job stayed `RUNNING`/`STABLE` throughout, because a FlinkDeployment's JobManager and TaskManagers keep running when the operator is absent — the operator is only needed to change things, not to sustain them. That is useful to know before any future operator upgrade |
| D62 | 2026-08-09 | **#118 implemented as designed in D56**: Google Secret Manager holds the two secret values, External Secrets Operator 2.9.0 fetches them through Workload Identity, and `make infra-up` now needs no credential on the operator's machine at all | **What was built, and the split that matters.** Two `google_secret_manager_secret` **containers** in the *platform* stack (they outlive the cluster, like the checkpoint bucket, D8) with `deletion_protection = true` to match the `allow_deletion = false` protection already on the topics — the values themselves are added with `gcloud secrets versions add`, never Terraform, because Terraform writes every managed value into state in plain text (§9). A dedicated `rx-vigilance-eso` service account holds `roles/secretmanager.secretAccessor` **per secret, not project-wide**, so secrets added to the project later are not readable by default. ESO installs in `runtime` with resource requests from the start (#115/D61's lesson applied before the fact rather than after). **Only two values are actually secret**: the Redpanda password and the GHCR token. The usernames were already in `Makefile` and `k8s/README.md` in plain text and are not sensitive, so they are literals in the `ExternalSecret` template — one fewer thing to store and rotate. **The name that cannot drift.** The IAM binding names the Kubernetes identity literally as `[external-secrets/external-secrets]`. The chart generates its service account name by default, so `serviceAccount.name` is **pinned** in the Helm values; a generated name that changed on a chart upgrade would break impersonation with a 403 that reads like an IAM problem and is not. Recorded in `k8s/README.md` beside the `[rx-vigilance/flink]` pair, which has the same property. **Migration method, which is the transferable part.** The two Secrets were live and feeding a running job, and ESO will not adopt a Secret it does not own. So the `ExternalSecret`s were first pointed at **temporary names** (`kafka-credentials-eso`, `ghcr-pull-eso`), and the generated values compared against the live ones by **SHA-256 of the base64 field** — same hash, no secret printed. Only after they matched were the originals deleted and the targets renamed. The Docker config hash differed for a harmless reason worth knowing: a `|` block scalar keeps a trailing newline, so the JSON was byte-different but field-identical; `|-` fixes it. **Bytes matching is not proof.** The check that actually mattered was deleting the TaskManager pod and watching it come back `1/1` — pulling the image with the operator-managed `ghcr-pull` and reading `kafka-credentials`. Until a pod starts from a Secret, all that has been compared is content. Checkpoints continued through it (840, 841), and at **37016 bytes** rather than the flat 33544 seen before #105 — state now holds the M001 coverage, which is the same signal that exposed the idle job, pointing the other way. **What this removes.** `check-env` and both `kubectl create secret` blocks are gone from `infra-up`; the runtime stack takes only `project_id`, `region` and `zone`. A fresh cluster now needs no password, token or key file on the machine bringing it up. `~/.redpanda-cloud.env` remains only for the platform stack and `CloudEventPublisher`. **A real incident during this work.** The GHCR token was printed in full while comparing the two Docker configs — a `base64 -d \| json.tool` command that was suggested with a "do not paste this" warning, which is not a control. The token was revoked and replaced, and rotating it exercised the new mechanism immediately: one `gcloud secrets versions add` plus a `force-sync` annotation, with no laptop file and no `kubectl create secret`. **The lesson is about the verification method, not the incident**: comparing secrets should never involve decoding them, and the hash comparison used for the Kafka values was the right pattern for both |
| D63 | 2026-08-10 | **`checkpoint.dir` becomes optional rather than required**, so each environment has exactly one place that defines the checkpoint directory: the FlinkDeployment manifest on GKE, `application-local.properties` locally | **Why the duplication existed and why it could not simply be deleted.** The operator's validating webhook demands `state.checkpoints.dir` in `flinkConfiguration` before the JVM exists — that is what rejected the very first Argo CD sync in #114 — while `CheckpointConfig` demanded `checkpoint.dir` as a job parameter. Two files, same `gs://` path, nothing enforcing agreement. Exactly D47's hazard, which cost a debugging session when a broker URL diverged. **The observation that made the fix small.** When the operator writes `state.checkpoints.dir` into the cluster configuration, **Flink already uses it**; the job calling `setCheckpointStorage` on top was setting the value the cluster had anyway. So the job never needed to be told on GKE — only locally, where there is no cluster configuration. The change is therefore three small edits, not a redesign: `null` allowed in `CheckpointConfig` (blank still rejected, because blank is a typo and absent is a decision), a conditional in `AdherenceJob`, and the line deleted from `application-gke.properties`. **The risk this introduced, and how it was closed.** If neither source sets a directory, Flink 1.18 silently falls back to `JobManagerCheckpointStorage`: checkpoints report as completed and are lost when the JobManager dies. A duplication that is visible is safer than a fallback that is not, so the fix is only acceptable with evidence of which path was taken. The job logs it at INFO — deliberately not WARN, since on GKE the "not set" branch is the normal and correct one, and a warning on every healthy start teaches people to ignore warnings. **The verification worth copying.** Three checks were planned: the job's own log line, checkpoints completing, and a new directory appearing in the bucket. The cluster supplied a fourth and better one unprompted — Flink's `Using job/cluster config to configure application-defined checkpoint storage: FileSystemCheckpointStorage`. `Completed checkpoint` would read identically under the memory fallback; that line names the storage implementation, so it distinguishes the two directly instead of by inference. Worth remembering as a pattern: prefer the log line that names the mechanism over the one that reports success. **Result on the cluster**: new job directory in `gs://…/rx-vigilance-ckpt/`, checkpoints 2401-2403 at 37031 bytes against 37016 before the upgrade — the savepoint restore carried the M001 coverage across, so this also re-exercised the D51 upgrade path with real state |
| D64 | 2026-08-10 | **`transaction.timeout.ms` raised from 60000 to 900000**, the broker's maximum. The measurement that settles it: a real restart took **64 seconds**, so the old value was below actual restart time rather than merely close to it | **What the setting does.** The EXACTLY_ONCE sinks hold a Kafka transaction open from one checkpoint to the next and commit when the checkpoint completes. `transaction.timeout.ms` is how long the broker will keep an open transaction before discarding it. If the job is down longer than that, the transaction it had open is gone and the alerts in it are lost — silently, from the job's point of view, which is the worst kind of loss in an adherence pipeline. **The value was never chosen.** 60000 came from `KafkaTypedSinkBuilder`, and D59 kept it deliberately: with the job crash-looping on the dead-letter sink, matching the number the other three sinks had already proven against this broker was safer than guessing higher. That was right at the time and left this question open. **The broker's limit had to be measured, not assumed.** `rpk cluster config get` is unsupported on serverless, so a `probe` mode was added to `CloudEventPublisher`: build a transactional producer at a given timeout and call `initTransactions()`. 900000 accepted, and the 1h Flink default was already known to be rejected — so the cap is 15 minutes, matching Redpanda's documented default. The probe is kept in the repo, because without it the number in the code comment is an assertion rather than a finding. **Why the maximum rather than a rounder number.** Standard guidance for exactly-once Flink sinks is to set the timeout to at least the maximum tolerable downtime; Flink's own default is an hour, and production deployments usually raise the *broker's* `transaction.max.timeout.ms` to accommodate it. Serverless does not allow that. The honest description is that we asked for an hour and took the cap. An earlier draft of this decision argued for 600000 to leave "margin below the cap in case a future broker is lower" — that argument was withdrawn, because a broker that rejects the value fails loudly at `InitProducerId`, exactly as observed this morning. There is no silent risk to insure against, so 600000 would buy five minutes less protection for nothing. **The cost, stated plainly:** if the job dies and never returns, the abandoned transaction blocks read-committed consumers for up to 15 minutes. That only lands when the job is gone for good, while the benefit lands on every ordinary restart. **The measurement.** A savepoint upgrade on 2026-08-10 ran from container stop at 04:58:26 to `RUNNING` at 04:59:30 — 64 seconds, and the savepoint itself was taken before that window, so the real outage is longer. Image pull was 1.9s on a warm node and would be slower on a cold one. **Every deploy done today was therefore inside the failure window of the old setting**, and only the absence of in-flight alerts kept it from mattering. 900000 leaves roughly a fourteenfold margin. **Process note:** the code for this landed on `main` inside PR #140, whose title describes only #132's ledger, because the commits were made on the wrong branch. Nothing was lost and CI was green, so the history was left alone rather than rewritten after merge and deploy — but the PR title does not describe half of what it contains, and that is worth avoiding rather than repeating |
| D65 | 2026-08-10 | **`deploy.yml` no longer rebuilds on documentation-only changes.** `'**.md'` added to `paths-ignore`, which previously covered `k8s/**` alone | **The cost was not theoretical and not small.** Any push to `main` that touched no `k8s/` file ran the whole deployment machine: build the image, push about 600 MB to GHCR under a new commit tag, write that tag back into `kustomization.yaml`, and let Argo CD sync it — which makes the operator take a savepoint, kill the job and start a new one. D64 measured that restart at **64 seconds**. So a typo fix in a Markdown file took a healthy job down for a minute. It happened five times across 2026-08-09/10 for ledger and README edits alone, including one that restarted the job while its own decision entry was being written. **Why `paths-ignore` is the right tool and is safe.** A push is skipped only when **every** changed file matches the list. A commit touching both a Markdown file and Java still deploys, so there is no way to lose a real deployment by including a docs edit alongside it. That property is what makes an ignore list preferable to a `[skip ci]` convention in commit messages, which is the other common approach and depends on every future message being written correctly — the same argument D54 used when it chose `paths-ignore` over `[skip ci]` for loop prevention. **Scope deliberately limited to documentation.** `infra/**` also cannot change the image, and arguably belongs here too, but Terraform changes are rarer and the reasoning is less obvious at a glance; left out rather than folded in silently. **Verified by the merge of this entry.** The commit that adds D65 touches `IMPLEMENTATION.md` and nothing else, so it is exactly the case the change exists to skip. The check is that merging it produces a CI run and **no Deploy run**. Testing the fix with the artifact that documents it is a small thing, but it beats reading the YAML and concluding it looks right — which is how `applicationSet.enabled` got past review in D55. **One ordering mistake worth recording**: the ledger PR intended as the test merged *before* the fix did, so it deployed exactly as before and was wasted as a test. Merge order matters when the change under test is a merge-time behaviour |
| D66 | 2026-08-10 | **Phase 11 scope expanded beyond `spec.md`'s minimum**, and the phase renamed *Logging & Observability*. The spec asks for four things: Prometheus reporter + PodMonitor, one dashboard, two Alertmanager rules, one stall drill. The phase now also covers structured JSON logging with a standard field set, Kafka coordinates carried through to the dead-letter topic, seven new application metrics, four audience-specific dashboards, a full alert set with inhibition, a 40-scenario runbook, and enforced PHI log hygiene. `spec.md` stays read-only; the expansion lives in this ledger and in epic #145 | The spec's minimum makes the job *monitorable* — you can see that a number moved. It does not make the job *supportable*: nobody can go from an alert to a root cause without reading Java. Measuring the starting position made the gap concrete rather than theoretical. Three classes in the whole codebase log anything, and the core operator (`AdherenceProcessFunction`) is silent. Logs are plain-text, so Cloud Logging can only grep them, which kills every canned query the runbook depends on. `DeadLetterRecord` carries `(rawBytes, errorMessage)` and no Kafka coordinates, so a poison message cannot be traced back to the broker at all — the single biggest root-cause gap. Operators set `.uid()` but never `.name()`, and Flink's metric labels use the name, so every dashboard panel would show auto-generated strings. Two of these are cheap to fix now and expensive later: dead-letter coordinates travel as Kafka **headers**, which is additive with no schema-registry involvement (D67), and `.name()` is savepoint-safe because `.uid()` is what drives state. The cost argument also favours expansion: kube-prometheus-stack (Prometheus, Grafana, Alertmanager, kube-state-metrics, node-exporter) has been installed and billing since Phase 1 while scraping nothing from Flink, so most of the platform is already paid for and the remaining work is wiring, dashboards and a runbook rather than installing a stack. Deliberately excluded, with reasons, so the expansion does not become unbounded: no Loki (GKE already ships container stdout to Cloud Logging for free; Loki would cost memory on a two-node spot cluster for a capability we have), no distributed tracing (built for request/response call graphs across services — this is one streaming job, and `claimId` in structured logs answers the same questions; Flink 1.18 has no first-class OpenTelemetry support), no synthetic correlation ID (`claimId` is already unique and on every `RxFillEvent`; inventing one would mean an Avro change, a §11 one-way door, for no gain). Sequenced so #146–#150 are code and config verifiable locally and only #151–#153 need the cluster, which keeps GKE billing time down per §3 |
| D67 | 2026-08-10 | **Kafka source coordinates (topic/partition/offset/timestamp) are carried on `KafkaSourceResult` and `DeadLetterRecord` and written as Kafka message headers**, not as Avro fields — and adding those fields to `DeadLetterRecord` is treated as savepoint-safe | **Why headers.** The dead-letter topic carries raw bytes with no schema at all — `AlertKafkaSinks.deadLetterRecordSerializer` writes `DeadLetterRecord::rawBytes` directly and already attaches one header (`error-message`) via `setHeaderProvider`. Kafka headers are additive metadata: nothing to register in the schema registry, nothing to evolve, no `FULL_TRANSITIVE` compatibility question, and consumers ignore headers they do not recognise. Modelling the coordinates as Avro fields instead would have created a new subject and a real one-way door (§11) for metadata that is not part of the payload. **Why the record change is safe.** `DeadLetterRecord` is Kryo-registered in the job graph, which is close enough to §4/§11's state-schema line to be worth stating rather than assuming. It is safe because the type never reaches persisted state: `upgradeMode: savepoint` (D49) stops the job cleanly before an upgrade, and aligned checkpointing holds no in-flight records — the record exists only between the deserializer and the sink within a single run. Recorded explicitly so a future reader does not have to re-derive it, and so that if unaligned checkpoints are ever enabled this entry is the thing that flags the assumption as broken. **What this buys.** It closes the biggest root-cause gap in the project: today a deserialization failure tells you *that* something failed and roughly *why*, but not *which message*, so nobody can inspect the real record on the broker, replay it, or tell the producing team where the bad data is. D29's incident is the cautionary case — a permanently misconfigured serializer produced an infinite restart loop with no error pointing at the cause |
| D68 | 2026-08-11 | **`podmonitor.yaml` lives in `k8s/flink/`, not `observability/` as `spec.md`'s file layout specifies**, and the Argo CD `AppProject` gains a seventh permitted kind (`monitoring.coreos.com/PodMonitor`) | **Why the spec's path cannot work.** `k8s/argocd/application.yaml` sets `path: k8s/flink` — Argo CD reads that one directory and nothing else. A PodMonitor at `observability/podmonitor.yaml` would be valid YAML, committed to `main`, and never deployed, with nothing anywhere reporting a problem. Widening the Application's path was considered and rejected: the narrow path is precisely what makes `prune: true` safe (D54), so trading it away to satisfy a file-layout convention would be a poor exchange. Kustomize referencing a file above its own root was also rejected — the load restrictor blocks it by default, and disabling that is a bigger concession than moving one file. **Why the new location is right on its own terms, not just expedient.** This PodMonitor is created *inside* the `rx-vigilance` namespace, because that is where the pods it selects live, and `rx-vigilance` is exactly what the Application's `destination.namespace` and the AppProject's `destinations` scope to. The `observability/` directory remains meaningful for #151 and #152, whose dashboards and alert rules configure the monitoring stack in the `monitoring` namespace — which this Application has no business touching and no permission to reach. The split therefore encodes something true: `k8s/flink/` is what Argo CD deploys into the job's namespace, `observability/` is what configures Prometheus, Grafana and Alertmanager. **Second half: the AppProject widening.** `namespaceResourceWhitelist` listed six kinds, all of which corresponded to files that already existed; PodMonitor is the first kind added after the fact. The file's own comment anticipated this — "Adding a kind here is a deliberate act; a sync that needs an unlisted kind fails loudly instead of widening Argo CD's reach by accident" — so this entry is that act being recorded rather than a rule being bent. Two ordering facts worth keeping: the AppProject is applied by `make infra-up` and **not** managed by Argo CD (an Argo CD that could grant itself permissions would make the whitelist decorative), so it must be `kubectl apply`-ed by hand; and it must be applied *before* the PodMonitor reaches `main`, or the first sync fails on a kind the project does not permit — harmless to the running job, but a confusing failure several steps from its cause. **A third thing this changed, discovered rather than planned.** The implementation dropped a step: the spec and the original issue both assumed `ENABLE_BUILT_IN_PLUGINS` would need the metrics jar added. It does not. The Flink image pre-stages all seven metrics reporters as ready-made folders under `/opt/flink/plugins/`, which Flink loads automatically; `ENABLE_BUILT_IN_PLUGINS` exists for the *loose* jars in `/opt/flink/opt/`, and no prometheus jar is there at all. Adding it would have told the entrypoint to look for a file that does not exist. Enabling the reporter is therefore two configuration lines and nothing else — worth recording because the wrong assumption is the one every online guide repeats |
| D69 | 2026-08-12 | **Four constraints govern logging configuration under the Flink Kubernetes operator**, none of them visible from a local build, each found the same way — by deploying and reading what the cluster actually did. Recorded together because they compose: fixing one exposes the next, which is why #147 took four deploy cycles rather than one | **(1) The cluster never reads the job jar's `log4j2.properties`.** Flink reads `/opt/flink/conf/log4j-console.properties`, which the operator writes from `spec.logConfiguration`. The in-JAR file governs local runs only, and `src/test/resources/log4j2-test.properties` governs tests — three configs, three audiences, and the comments in each now say which. **(2) `logConfiguration` is a sibling of `flinkConfiguration`, not a key inside it.** Indented one level too deep, the block is accepted by the API server, appears in `kubectl get -o yaml`, greps positively for every string you look for, and does nothing: the operator's `lastReconciledSpec` records `"logConfiguration":null` and the pod mounts Flink's stock config, Apache licence header and all. No error is produced at any layer. Grepping for the key was not a sufficient check; the verification that works parses the rendered manifest and asserts `logConfiguration` is in `spec` and *absent* from `flinkConfiguration`. **(3) The operator mounts a ConfigMap over `/opt/flink/conf`, shadowing whatever the image put there.** `ls` on the running pod shows the `..data` symlink pattern of a projected ConfigMap holding exactly two files. Anything the Dockerfile copies into that directory is invisible at runtime — a `COPY` that succeeds at build time and silently disappears. `/opt/flink/usrlib` is not mounted (the job jar lives there and loads), so the JSON template goes there instead. The image path and `eventTemplateUri` must now agree with nothing enforcing it, which is D47's hazard again; accepted as the smaller cost, with a comment on each side naming the other. **(4) Log4j add-ons are constrained by Flink's version, not ours.** `classloader.parent-first-patterns.default` includes `org.apache.logging.log4j`, so Flink's bundled 2.17.1 always wins over anything shaded into the job jar. `log4j-layout-template-json:2.23.1` therefore failed with `NoSuchMethodError: Strings.toRootLowerCase` — a method added after 2.17.1 — and log4j responded by discarding the entire configuration and falling back to its built-in default. That fallback is what made this hard: logs came out plain text with framework loggers at INFO, looking exactly like "the config never applied", and the actual cause sat 500 lines up in a `StatusLogger` line that no tail would ever show. New property `${flink.log4j.version}` pins log4j add-ons to the **oldest** log4j in play; pinning the other direction cannot work, since a newer plugin calling newer APIs will always break against an older core. **The process lesson, which is the reason this entry is long.** `JsonLogLayoutTest` passed against 2.23.1 the whole time production was broken at 2.17.1 — a green test proving nothing about the environment it was meant to protect. Any dependency that must load inside Flink's framework classloader has to be tested at Flink's version, and the same class of trap has now hit this project twice: the `flink-connector-base` gap in #38 was also a non-transitive dependency invisible until a real run. Two verifications were added because of this and are worth reusing: parse the kustomize output rather than grepping it, and `docker run --rm <image> ls /opt/flink/lib` before pushing. **(5) Added 2026-08-12 — `flinkConfiguration` keys replace Flink's defaults, they do not merge with them.** The fourth constraint above led to setting `env.java.opts.all` for the JUL LogManager property, and that single line silently deleted Flink 1.18's JDK-17 module-access flags and crash-looped the job for 13 hours. Full account in D72; recorded here because it is the same family as (2) and (3) — a config surface that accepts what you write, reports no error, and does something other than what it looks like |
| D70 | 2026-08-12 | **`RecordKryoSerializer` is registered as a Kryo *default* for `Record.class` inside every entry point that configures an `ExecutionEnvironment`** — `AlertKafkaSinks.deadLetterSink` and `KafkaTypedSourceBuilder.build` — rather than type by type. The existing `registerTypeWithKryoSerializer` lines stay | **The failure.** Adding one field to `DeadLetterRecord` broke two integration tests with `Unable to create serializer "FieldSerializer" for class: KafkaCoordinates`, four frames below `RecordKryoSerializer.write`. **The chain.** Flink 1.18's type extractor does not recognise Java records, so every record in this project is a `GenericTypeInfo` and is serialized by Kryo. Kryo 2.24 cannot serialize a record at all — `sun.misc.Unsafe.objectFieldOffset` throws `UnsupportedOperationException: can't get field offset on a record class`. `RecordKryoSerializer` exists to work around exactly that, by reading record components through their accessors and rebuilding via the canonical constructor. But it writes each component with `kryo.writeClassAndObject`, which performs a **fresh serializer lookup for the component's own class**. A record nested inside a registered record is therefore not covered by the outer registration: `KafkaCoordinates` was new, unregistered, and fell straight back to the default `FieldSerializer`. **Why the job was fine and only tests failed.** `AdherenceJob` already carried `addDefaultKryoSerializer(Record.class, RecordKryoSerializer.class)`, which matches every record by assignability, so the deployed job path was never broken — `AdherencePipelineIT` passed throughout. The two failing tests build a bare `StreamExecutionEnvironment` and call the sink and source helpers directly, so only the helpers' own per-type lists applied. That asymmetry is the real defect: the catch-all was in the one place that did not need it least, and the helpers each maintained a list that a future record would silently fall off. It now lives in the helpers, so job and test callers are configured identically. **Why the per-type lines were not deleted.** `registerTypeWithKryoSerializer` also assigns the class a Kryo registration id, which changes the serialized byte layout; removing the lines would alter the wire format for no benefit. They are redundant, not wrong. **The rule for future work.** Adding a record type needs no action. Adding a new entry point that configures an environment does — register the `Record.class` default there, or the first nested record to appear will fail at `env.execute()` with a message naming Kryo rather than the missing registration. When Flink is next upgraded, re-check whether the type system handles records natively; if it does, this whole workaround retires with it |
| D71 | 2026-08-12 | **Two of #148's own bullets were not implemented, and the logging that was implemented follows four rules that are not obvious from the issue text.** (a) No ERROR on sink write failures. (b) Alert emissions log at DEBUG, not INFO | **(a) Sink failure ERROR has no place to live.** `KafkaSink` exposes no failure hook: writes happen inside Flink's `KafkaWriter`, which already logs the failure and fails the job. Getting our own ERROR would mean wrapping the sink purely to duplicate a message Flink prints anyway — real complexity for a second copy of the same fact, and a second copy that can drift. What was added instead is an INFO at construction naming the resolved topic, `deliveryGuarantee` and `transactionalIdPrefix`, on the reasoning that the useful sink question is "did it write where I think it did", not "did it throw". The prefix is logged because two `EXACTLY_ONCE` jobs sharing one fence each other's producers off, and nothing in the resulting hang names the prefix. **(b) Alerts stay at DEBUG because they carry `memberId`.** They are the business output and rare enough that INFO would be affordable, but §9 keeps member identifiers out of INFO and above, and an exception for "the important ones" is exactly how PHI reaches Cloud Logging. #150's counters carry the INFO-level signal instead, which is the right split: counts are not identifying, individual alerts are. **Rule 1 — guard on evaluation, not on level.** Sonar S2629 fires when a log call's arguments require evaluation, so `if (LOG.isDebugEnabled())` wraps every call whose arguments are method invocations, and calls whose arguments are locals or fields are left unguarded. Mechanical and checkable, versus "guard the hot ones", which drifts. **Rule 2 — sample the counters, never the first.** Broadcast entry counts and dead-letter WARNs log the first occurrence and then every Nth (500 and 100). The first is what proves the thing happened at all; the rest are volume. For dead letters this is only safe because #149 put the coordinates in the dead-letter topic's headers, so the topic is the complete record and the log is a pointer — without #149 this sampling would have lost information. **Rule 3 — log two forms of a timestamp.** `timerTs` in epoch millis matches what the Flink UI shows for watermarks; `timerAt` as an `Instant` is what a human reads. Comparing timer to watermark is the first move in every stalled-timer investigation (§10), and forcing an epoch conversion mid-incident is a bad trade for one field. **Rule 4 — every silent return names its reason.** Five existed in `AdherenceProcessFunction`, all correct, all invisible; the stale-timer one splits into three reasons because only two of the three are worth investigating. **One refactor was forced, not chosen**: adding the branch logging pushed `onTimer` to cognitive complexity 19 against Sonar's limit of 15, so the two timer stages were extracted into `handleGapRiskTimer` and `handleLapsedTimer`. Behaviour is unchanged and the method now reads as "is it stale, which stage, delegate", which is how the spec describes it. **A bug this caught, worth recording because the test caught it and review would not have**: the first implementation generated `alertId` into a local for the log and then called `UUID.randomUUID()` again inside the alert constructor, so the logged id and the emitted id were different values — the correlation the change exists to provide, silently broken. `gapRiskAlertEmissionIsLoggedWithTheAlertIdThatReachesKafka` asserts the two are equal, which is why it failed; a test asserting only "an alertId was logged" would have passed |
| D72 | 2026-08-12 | **JVM flags for this job go in the container's `JAVA_TOOL_OPTIONS`, never in `flinkConfiguration.env.java.opts.all`.** Recorded after #172: a one-line use of that key crash-looped the job for 13 hours | **What the key actually does.** `env.java.opts.all` **replaces** its value; it does not append. Flink 1.18's stock `flink-conf.yaml` — verified inside our own image, not from documentation — sets it to the JDK-17 module-access list: 8 `--add-exports` and 12 `--add-opens`. #147 set it to `-Djava.util.logging.manager=…` alone, so all 20 flags disappeared. Java 17 seals `java.base` and Kryo restores state by reflecting into JDK collection internals, so without `--add-opens=java.base/java.util=ALL-UNNAMED` the `chronic-class-filter` operator `ListState` could not be deserialized: `InaccessibleObjectException: Unable to make field private final java.lang.Object[] java.util.Arrays$ArrayList.a accessible`. Every restore failed, every restart re-restored, 470 cycles and 26,288 exceptions over 13 hours with zero records processed. The state on GCS was never corrupt — the JVM could not read it. **Why `JAVA_TOOL_OPTIONS` and not the obvious fix.** Repeating Flink's full 20-flag list with our flag appended was considered and rejected: it works today and pins a stale copy of Flink's defaults into this repo, so the next Flink upgrade reintroduces the same bug quietly. `JAVA_TOOL_OPTIONS` is read by the JVM *in addition to* the command line, so Flink's defaults stay owned by Flink. Cost is one plain-text `Picked up JAVA_TOOL_OPTIONS:` line per JVM start, which precedes log4j initialisation and therefore is not JSON — #153's runbook must say so, alongside the `docker-entrypoint.sh` lines that have the same property. The JUL LogManager class resolves because #147 also put `log4j-jul` in `/opt/flink/lib`; that half of #147 was correct and load-bearing. **The verification lesson, which is the reason this entry exists.** #147 was signed off by confirming JSON entries in Cloud Logging, and those entries were perfect the entire time the job was failing every restore. A job prints its logs before it restores its state, so log output proves nothing about whether the job runs. Two checks are now mandatory after any deploy that touches image, JVM or cluster config, and neither is about logs: `kubectl get flinkdeployment -o jsonpath='{.status.jobStatus.state}'` must read `RUNNING`, and the `JobStatusChanged`/restart counts in `kubectl describe` must not be climbing. A third, cheap and specific to this class of change: `kubectl exec <tm-pod> -- cat /proc/1/cmdline \| tr '\\0' '\\n' \| grep -c add-opens` must return 12. **How it was found, worth copying.** The `Last Stable Spec` field in the FlinkDeployment status still named the image from *before* #147, which dated the regression to a single deploy in one step and ruled out #148 and #149 without reading any of their code |
| D73 | 2026-08-14 | **Four decisions taken while implementing #150**, recorded together because each one changes something a later phase depends on: `IntervalMerger` reports outcomes instead of signalling by reference equality; metric registration is lazy; `timersFired` counts stale firings; `activeKeys` is dropped in favour of RocksDB's `estimate-num-keys` | **(1) The merger returns an outcome, and becomes pure.** #150 needs `duplicateClaimIdDropped` and `reversalWithoutOriginal` incremented caller-side, because §4 keeps Flink out of `IntervalMerger`. The caller *could* have counted them with no merger change at all — `merge`'s only no-op was a duplicate claim and `unwind`'s only no-op was an unmatched reversal, so the calling method already identified the reason. That was rejected because of what it would have been built on: the merger signalled "nothing happened" by returning **the same object it was given**, and the caller tested `merged == currentState`. Nothing in either signature said so. A defensive copy anywhere in the merger — an ordinary, well-intentioned change — would flip that check false and make every duplicate claim register a timer, emit a PDC snapshot and double-count coverage, with no test failing: the two tests guarding it asserted `isSameAs`, which pins the pointer trick rather than the contract. Building two dashboard metrics on that was the wrong foundation for the phase whose entire purpose is trustworthy signals. `MergeResult`/`UnwindResult` now carry an outcome enum. **Two enums rather than one shared enum**, so `merge` cannot report `NO_MATCHING_INTERVAL`: the impossible combination becomes a compile error instead of a thing to remember. Both `LOG.warn`s moved to the caller, which has the drug class the merger never had, and the merger lost SLF4J entirely — the §4 rule is "no Flink imports", but a pure function with no side effects at all is the stronger version and costs nothing here, with exactly one caller. Two corrections fell out: the fill-path log said `reason=duplicate-or-fully-covered`, and the "or" was never reachable — a fill sitting fully inside existing coverage still builds a new state and still registers a timer; and the reversal no-match path logged nothing at the caller at all. `IntervalMergerTest`'s new `merged`/`unwound` helpers assert `APPLIED` internally, so all seven arithmetic tests now cover the outcome for free. **(2) Metric registration is lazy.** `register()` created all four counters and each caller took the one or three it needed, so chronic-class-filter published three permanently-zero series. Tolerable at four metrics and two operators; at eleven metrics across five operators — `DeadLetterSplitFunction` is generic, so Flink instantiates one operator per source — it becomes ~40 junk series that every #151 panel has to filter around. Counters are memoised on first request. The map is not only about noise: Flink's `MetricGroup` rejects a second registration of the same name and hands back a counter that nothing reports, so without memoisation a duplicated accessor call would increment into a void. `count(name)` and `gaugeValue(name)` **throw** on an unknown name rather than returning zero, because a typo in a test assertion would otherwise read as "the counter never moved" and pass green — the exact failure the accessors exist to prevent. This also replaced what would have been five more `…Count()` methods on `AdherenceProcessFunction`, holding the line D46 drew about test-only surface on main code. **(3) `timersFired` is incremented on the first line of `onTimer`, before the stale check.** Counting only firings that did work seems more precise and is worse: a bug making every timer stale would show `timersFired` flat at zero, which is indistinguishable from a frozen watermark — two unrelated problems with one signature, and #152 could not tell them apart. Counting at the door means the metric answers exactly one question, "is the timer service delivering firings at all". Whether a firing did anything is already in #148's DEBUG reasons. `staleTimerFiresAsNoOp` asserts this and carries the reasoning in a comment, so a future "fix" that moves the increment fails there. Consequence worth knowing before reading a dashboard: `timersRegistered` counts registrations over time, not live timers, so a refill that deletes and re-registers shows 2 against one live timer — `refillBeforeThresholdCancelsAndRegistersExactlyOnceTimer` asserts both numbers side by side to make that concrete. **(4) `activeKeys` was dropped from the issue's list.** Flink has no API to count keys in keyed state — the operator sees one key at a time and RocksDB is not queried for cardinality — so the only implementable version was a per-subtask counter of first-time-seen keys. That never decreases, so it would overstate live state permanently and would be flatly wrong after TTL expiry, which on a 400-day TTL is the normal steady state. A gauge that is wrong is worse than no gauge, because someone will eventually make a capacity decision from it. RocksDB's `estimate-num-keys` answers the same question honestly, per column family, and decreases when keys expire; it costs one config line. Three RocksDB natives are enabled — `estimate-num-keys`, `estimate-live-data-size` (a million small states and a thousand huge ones look identical in key count, and only one fills the disk) and `estimate-pending-compaction-bytes` (the leading indicator for write stalls, which slow the job with no error, no failed checkpoint and no exception). `state.backend.rocksdb.metrics.statistics` stays off — it is the expensive one. A fourth key, `column-family-as-variable`, is not a metric: without it the column family is baked into the metric *name*, so `adherence-state` and `pre-broadcast-buffer` become differently-named metrics and no single query spans them. Enabling it after #151's panels exist would mean rewriting all of them. **What is not verified**: every test in this repo runs on `HashMapStateBackend`, so nothing local exercises RocksDB and the metric *names* are unconfirmed. What is proven is that the four keys land under `spec.flinkConfiguration` as strings, checked by parsing `kubectl kustomize k8s/flink` rather than grepping it (D69). #151 reads the real strings off `curl localhost:9249/metrics` first, as #146 did — that same step is what caught the unreadable `operator_name` labels |
| D74 | 2026-08-14 | **`spot = false` on the GKE node pool is retained, deviating from D7, for the duration of the Phase 11 observability work (cluster up 2026-08-14 → 2026-08-16).** The comment on the line is corrected so the file stops contradicting itself | D7 chose spot nodes at ~70% saving on the reasoning that preemption is fine on a self-healing demo, and for Phases 1–10 it was. Phase 11 is the exception: #151–#153 are about watching metric and log behaviour over time, and a preemption restarts the job, which resets every per-subtask counter and gauge to zero. #175 is the concrete illustration — `broadcastEntriesLoaded` reads 0 after a restart whether or not anything is wrong — so a preemption mid-session produces a dashboard full of zeros that looks like a genuine failure and costs an investigation to disprove. Paying non-spot rates for two days to avoid that is the cheaper trade. **This is time-boxed, not a reversal of D7.** The value returns to `true` when the runtime stack is destroyed on 2026-08-16, and D7 stands for every phase after. Recorded because the change reached `main` inside PR #174 with no issue, no PR mention and a comment that still described the old value, which is exactly the drift §2 exists to prevent — the line now reads `spot = false # D74: non-spot for the Phase 11 observability window; revert to true per D7` |
