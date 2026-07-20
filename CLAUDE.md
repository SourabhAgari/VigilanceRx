# CLAUDE.md — RxVigilance Operating Rules

Real-time medication adherence & refill-gap detection pipeline.
Apache Flink 1.18 · Java 17 · Redpanda (Kafka) · RocksDB · Avro · Terraform · GKE.

This file is loaded automatically every session. It defines **how to work in
this repo**. It is not the design — the design lives in `spec.md`.

---

## 1. Document hierarchy (read in this order of authority)

| File | Role | Your relationship to it |
|---|---|---|
| `CLAUDE.md` | Operating rules | Obey always |
| `spec.md` | Engineering blueprint — READ-ONLY source of truth | Read before implementing any component; never edit without explicit instruction |
| `IMPLEMENTATION.md` | Phase ledger: status, tasks, exit criteria, Decision Log | Keep updated as you work |
| `hld.md`, `project_functional_document.md` | Design rationale, resolved decisions | Consult when spec references them |

Conflict resolution: if code, spec, and ledger disagree, **stop and ask** —
do not silently pick one.

---

## 2. Workflow — plan before apply (enforced)

1. Read the current phase in `IMPLEMENTATION.md` and the relevant `spec.md`
   sections **before writing anything**. Confirm the phase's epic and child
   issues exist (§2.2) — if not, create them first.
2. Present a plan: files to create/modify, approach, tests, risks. For
   source code, the "plan" takes the explain-first form defined in §2.3 —
   the user implements; you guide and review.
3. **Wait for explicit approval before applying.** This includes
   `terraform apply`, `helm` changes, and `kubectl` mutations — for those,
   present `terraform plan` / dry-run output as the plan.
4. After applying: run the verification step, then **immediately update
   `IMPLEMENTATION.md`** (see §2.1 — this step is never skipped or deferred).
5. Work **one phase at a time, in order**. Do not start a phase until the
   previous phase's exit criteria all pass. Do not mark a task done without
   its verification passing.
6. Any design decision not already resolved in `spec.md`/`hld.md` →
   propose it, get approval, record it in the Decision Log with date and
   rationale.

Scope discipline: implement what the current phase's tasks specify — no
opportunistic refactors, no extra features, no "while I'm here" changes.
Suggest them instead; they go in as Decision Log entries or new tasks.

### 2.1 Progress tracking in IMPLEMENTATION.md (MANDATORY — same as ClaimGuard)

`IMPLEMENTATION.md` is the single source of truth for project progress.
Updating it is part of completing a task, not an optional follow-up. A task
whose ledger entry is not updated is **not done**, even if the code works.

After **every completed task**:
- Tick its checkbox `[ ]` → `[x]` in the current phase.
- Append a one-line completion note under the task or phase:
  `— done 2026-07-XX: <verification evidence, e.g. "12/12 IntervalMergerTest green">`.
- If the task produced a decision, add the Decision Log row in the same edit.

At the **end of every working session** (even mid-phase):
- Ensure the phase status table reflects reality (☐ / ◐ / ✅ / ⏸ with reason).
- Nothing in your context that isn't written to the ledger survives the
  session — if it matters, write it down.

At **phase completion**:
- Confirm each exit criterion explicitly (list them with pass evidence).
- Set the phase to ✅ with completion date in the status table.
- Do not touch the next phase's tasks in the same commit.

Commit convention for ledger updates: include them in the task's commit
(`feat(coverage): implement IntervalMerger + ledger update`) — the ledger
must never lag the code on `main`.

At the **start of every session**: read the status table first and resume
from the ledger, not from memory or conversation history.

### 2.2 GitHub issue workflow (MANDATORY — same as the RxVigilance repo convention)

No phase starts without its issues existing. Use the `gh` CLI.

**Before starting any phase:**
- Create one **epic issue**: title `[EPIC] Phase N — <phase name>`, label
  `epic`. Body: goal (one paragraph), link to the `IMPLEMENTATION.md` phase,
  the exit criteria copied as a checklist, and a task list that will link
  the child issues.
- Create one **child issue per task** (or per coherent task group) from the
  phase's checklist: title `Phase N: <task>`, labels `phase-N` + area label
  (`coverage`, `operators`, `infra`, `observability`, `ci`). Body: what,
  acceptance/verification step from the ledger, and `Part of #<epic>`.
- Present the created issue list to the user before writing any plan for
  the first task.

**While working:**
- Work one child issue at a time; the per-task plan (§2) names the issue it
  serves. Branch: `phase-N/issue-<num>-<slug>`. Commits and the PR reference
  the issue (`Closes #<num>`), so merge closes it automatically.
- Anything discovered mid-task that is out of scope → new issue (linked to
  the epic), not an inline detour.

**Closing:**
- A child issue closes only when its verification passed and the ledger is
  updated (§2.1) — the same "not done until written down" rule.
- The epic closes only when every child is closed and each exit criterion is
  checked off in the epic body with evidence. Epic closure and the phase's
  ✅ in `IMPLEMENTATION.md` happen together.

GitHub issues are the work queue; `IMPLEMENTATION.md` remains the canonical
ledger. If they ever disagree, the ledger wins — fix the issues to match.

### 2.3 Explain-first: Claude Code does NOT write source code directly (MANDATORY)

This is a learning-first repository. The user writes the code; Claude
teaches, guides, and reviews.

- **Never create or edit source files directly** (`src/main`, `src/test`,
  `infra/`, `k8s/`, workflows). No `create_file`/edit-tool usage on them, no
  "I'll just scaffold this quickly."
- For every task, instead provide, in this order:
  1. **Why** — the design reasoning first, always: why this approach, why
     this Flink primitive, what breaks if done differently, which spec
     invariant it protects. The *why* is the point of this project.
  2. **What** — file path, class/function signatures, and the structure to
     build.
  3. **How** — step-by-step instructions the user can follow, with short
     illustrative snippets (snippets are teaching material to be typed and
     understood, not blobs to paste wholesale).
  4. **Verify** — the exact command/test to prove it works.
- After the user implements: review their code against the spec and
  invariants (§4), point out defects with reasoning, and only then proceed
  to verification and ledger/issue updates.
- Permitted direct writes, and nothing else: `IMPLEMENTATION.md` ledger
  updates (§2.1), GitHub issue bodies (§2.2), and files the user explicitly
  hands over for that specific task with words like "you write this one."
  When in doubt, explain — don't write.
- Running read-only and verification commands (`mvn verify`, `terraform
  plan`, `kubectl get`, tests) is allowed and encouraged; mutations still
  follow plan-before-apply (§2).

---

## 3. Environments

- **Local is the default for everything.** Docker Compose Redpanda
  (`docker-compose up -d`) + MiniCluster/test harnesses. Tests must never
  require cloud connectivity.
- Only phases tagged **[CLOUD]** in `IMPLEMENTATION.md` (1, 2, 10, 11, 12)
  may touch GKE / Redpanda Cloud.
- Redpanda Cloud is on a limited trial — never burn cloud time on work that
  runs locally.
- GKE bills while it exists: after cloud sessions, remind the user to
  `terraform destroy` (never run it unprompted).

---

## 4. Hard invariants (violating any of these is a defect, not a style issue)

- **Operator UIDs on every operator** in the job graph — savepoint
  compatibility.
- **Broadcast `MapStateDescriptor` names are frozen** (`NDC_CLASS_DESCRIPTOR`,
  `LEAD_TIME_DESCRIPTOR`) — renaming breaks savepoints.
- **`alertLeadDays` is never a constant anywhere.** It comes from the
  `(drug_class, dispensing_channel)` broadcast lookup only. The 5-day figure
  in examples is illustrative.
- **Timer hygiene**: every FILL/REVERSAL path leaves **at most one**
  registered timer per key, and `activeTimerTimestamp` in state always
  matches the registered timer.
- **Watermark idleness is mandatory** (`withIdleness(5min)`): at ~12–15
  events/sec, an idle partition silently freezes every event-time timer.
  Omitting it is a correctness bug.
- **Reversal correction guarantee is binding**: a reversal that leaves zero
  coverage emits the corrective alert immediately in `processElement`
  (spec — Event handling, REVERSAL step 5).
- **Domain objects and `IntervalMerger` have zero Flink imports.**
- **RocksDB TTL: 400 days** on `AdherenceState`, defined once in
  `StateBackendConfig`.
- **All config via `ParameterTool`** (with optional `--config.file` merge;
  CLI wins). No hardcoded brokers, topics, paths, or thresholds in pipeline
  code.

---

## 5. Testing standards

- Order within any component: pure logic first, harness second, integration
  last. `IntervalMerger` edge cases (`IntervalMergerTest`) are the gate for
  all adherence work.
- Timer tests must **advance event time explicitly** via watermarks and
  assert on **side-output contents**, not counts. A timer test that passes
  without watermark advancement is invalid — rewrite it.
- Operator tests: `KeyedOneInputStreamOperatorTestHarness` (+ broadcast
  variant). Integration: `MiniClusterWithClientResource` + Testcontainers
  Redpanda.
- Never weaken, skip, or delete a failing test to make a phase pass. If a
  test is wrong, say so and propose the fix in the plan.
- Definition of done for any task: code + tests + green build + ledger
  updated.

## 6. Build & verify commands

```
docker-compose up -d                        # local Redpanda (broker + registry)
./scripts/bootstrap-local-topics.sh         # topics + schemas (mirrors redpanda.tf)
mvn clean verify                            # build + all tests (the gate)
mvn test -Dtest=IntervalMergerTest          # fast pure-logic loop
```

Local job runs: use the IDE, not `mvn exec:java` — Flink's local mini-cluster
spawns threads that don't see `exec:java`'s bolted-on classloader (confirmed
during #38: `ClassNotFoundException` on core Flink classes even with all
dependencies present). Run the job class directly from IntelliJ with:

- Program arguments (space-separated — `ParameterTool` does not parse
  `--key=value`, only `--key value`):
  ```
  --kafka.brokers localhost:9092 --schema.registry.url http://localhost:8081 --checkpoint.dir file:///tmp/rx-vigilance-checkpoints
  ```
- "Add dependencies with 'provided' scope to classpath" enabled in the run
  configuration — Flink's own classes (`flink-streaming-java`, etc.) are
  `provided` scope (supplied by the cluster in real deployments) and are
  excluded from the run classpath by default otherwise.

Run `mvn clean verify` before declaring any task complete.

---

## 7. Code standards

- Java 17: records for domain objects, switch expressions, no raw types.
- Package root `com.healthcare.rxvigilance`; structure per spec "Project
  structure" — do not invent new top-level packages without approval.
- Serialization: Avro via schema registry (`flink-avro-confluent-registry`);
  `.avsc` files in `src/main/resources` are the schema source of truth.
  Schema changes must be backward-compatible (`FULL_TRANSITIVE`) — call out
  any schema change explicitly in the plan.
- Undeserializable events go to the dead-letter side output — never swallow,
  never crash the job.
- Logging: SLF4J, no `System.out`; parameterized messages; no PHI-style
  identifiers (memberId etc.) at INFO level or above — use DEBUG for
  key-level detail.
- SonarQube Cloud quality gate must be green on every PR; treat new Sonar
  findings in touched files as part of the task.

---

## 8. Git & PR conventions

- Never commit directly to `main`; branch per phase/task:
  `phase-3/interval-merger`.
- Conventional commits: `feat(coverage): ...`, `fix(operators): ...`,
  `test(timers): ...`, `infra(terraform): ...`, `docs(ledger): ...`.
- One phase (or coherent task slice) per PR; PR description links the
  `IMPLEMENTATION.md` tasks it completes.
- CI (`ci.yml`) must be green — `mvn verify` + Sonar gate + docker build —
  before merge. `deploy.yml` runs on `main` only.

---

## 9. Security & secrets (non-negotiable)

- **Never** write credentials, tokens, SASL configs, or kubeconfigs into
  git-tracked files — including examples, tests, and this ledger.
  Placeholders only (`<REDPANDA_PASSWORD>`).
- Kafka credentials live in the Kubernetes Secret
  (documented manual creation) and env vars locally — never in Terraform
  (state-file plaintext) and never in `application-*.properties` committed
  values.
- Terraform state is remote (GCS backend) and never committed; never edit
  state by hand.
- This is a synthetic-data portfolio project in a healthcare domain: treat
  member-level data as if it were PHI anyway (log hygiene above) — that
  discipline is part of the point.

---

## 10. Known sharp edges (check before debugging blind)

- **Watermark stalls** are the #1 silent failure: if timers "don't fire,"
  check watermark progression and source idleness before touching timer code.
- **Helm-provider-on-fresh-cluster**: first `terraform apply` from empty
  state may need a targeted apply of the GKE cluster first — documented in
  `infra/terraform/README`. Not a mystery; don't refactor around it.
- **cert-manager before Flink operator**: the operator's webhook needs
  cert-manager ready; respect the `depends_on` chain in `helm.tf`.
- **GKE Capacity vs Allocatable**: TaskManager memory requests must fit
  Allocatable on small nodes; RocksDB off-heap is part of that budget.
- **Event-time everywhere**: fill dates are day-granularity and batchy;
  never mix in processing-time semantics or `System.currentTimeMillis()`
  in pipeline logic.

---

## 11. When to stop and ask

Stop and ask the user before proceeding if:
- spec, ledger, and code disagree, or a spec section seems wrong/ambiguous;
- a task requires cloud access outside a [CLOUD] phase;
- a change would touch keying, state schema, broadcast descriptor names, or
  Avro schemas (savepoint/compatibility one-way doors);
- exit criteria for the previous phase are not verifiably met;
- anything would require weakening a test or invariant.

Asking is always cheaper than unwinding a one-way door.
