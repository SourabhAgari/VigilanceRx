# RxVigilance — High-Level Design (hld.md)

**Phase:** 1 — High-Level Design closure (`IMPLEMENTATION.md` §3, Phase 1)
**Status:** COMPLETE — all 8 sections decided and written; pending PR
review (Phase 1 Definition of Done: reviewed, no TBD).
**Derived from:** `spec.md`, `project_functional_document.md`,
`docs/scale_and_capacity_estimates.md`, `docs/business_impact_and_cost_analysis.md`
**Definition of Done:** every section below has a written, non-provisional
answer; the two `spec.md` §11 HLD-scoped open questions carry final
decisions; no "TBD" anywhere in this document.

| # | Section | Epic | Status |
|---|---------|------|--------|
| 1 | System context & boundary | #2 | Done |
| 2 | Keying strategy — final decision | #2 | Done |
| 3 | Alert retraction semantics — final decision | #6 | Done |
| 4 | Component architecture | #2 | Done |
| 5 | NFR targets | #3 | Done |
| 6 | PHI/security posture | #4 | Done |
| 7 | Schema registry strategy | #5 | Done |
| 8 | Downstream interface contracts | #6 | Done |

---

## 1. System context & boundary

*(C4 Level 1 — system context. Fixes what RxVigilance is responsible for
and, with named owners, what it is not.)*

### 1.1 Context diagram

```
                     ┌─────────────────────┐   ┌──────────────────────┐
   UPSTREAM          │ Claims adjudication │   │ Reference data mgmt  │
   (producers)       │ system (NCPDP)      │   │ (pharmacy/clinical)  │
                     └──────────┬──────────┘   └──────────┬───────────┘
                                │ FILL / REVERSAL          │ NDC→class table
                                │ events (Avro)            │ (class,channel)→lead_days
                                ▼                          ▼
                     ╔══════════════════════════════════════════════╗
                     ║                 RxVigilance                  ║
                     ║  per-(member, drug_class) coverage tracking, ║
                     ║  event-time gap prediction, reversal unwind  ║
                     ╚══════╤═══════════════╤═══════════════╤═══════╝
                            │ GapRiskAlert  │ LapsedAlert   │ PDC snapshots
                            ▼               ▼               ▼
                     ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐
   DOWNSTREAM        │ Outreach /   │ │ Care mgmt /  │ │ Adherence-calc   │
   (consumers)       │ notification │ │ escalation   │ │ system → Star    │
                     │ systems      │ │ workflows    │ │ Ratings reporting│
                     └──────────────┘ └──────────────┘ └──────────────────┘
```

### 1.2 Inside the boundary — RxVigilance owns

- **Coverage state per `(member_id, drug_class)`** — the rolling picture of
  when each member's supply runs out, updated incrementally per event
  (`spec.md` §7).
- **Forward-looking gap prediction** — the event-time timer firing at
  `currentSupplyEndDate − alertLeadDays` (`spec.md` §8.1/§8.2). This is the
  capability a batch job structurally cannot provide (`spec.md` §2 #4).
- **Reversal unwind** — interval-level state correction at the moment the
  reversal event arrives (`spec.md` §8.3).
- **Emission of alerts and coverage facts to Kafka topics** — responsibility
  ends at the topic boundary; delivery guarantees for that emission are
  specified in §8 (interface contracts).

### 1.3 Outside the boundary — with named owners

| Not RxVigilance's job | Owner | Decided in |
|---|---|---|
| Alert delivery (SMS/call/outreach execution) | Notification systems consuming `gap-risk-alerts` | `spec.md` §3 |
| Final PDC ratio, measurement-year denominator, PQA eligibility rules (2-fill minimum, hospice, disenrollment) | Downstream adherence-calculation system | FR-6 (resolved 2026-07-04) |
| Historical backfill / state seeding at go-live | Nobody — accepted ~90-day ramp-up gap, by decision | `project_functional_document.md` §6 |
| Distinguishing real gaps from prescriber-ordered discontinuation | Outreach-side suppression (confirmed-discontinued lists) | FR-3, accepted limitation |
| Acute + specialty/infusion inclusion decisions | Reference-data table content (the filter stage only obeys it) | `spec.md` §6, FR-9 |

Three of these five exclusions are documented accepted limitations, not
gaps: they were considered and deliberately not built.

### 1.4 Boundary decisions recorded

- **Single upstream producer (v1).** The claims adjudication pipeline is
  the only fill/reversal producer. A second producer (e.g., another PBM
  platform post-acquisition) is not anticipated in the diagram — that is
  deliberate YAGNI. The extensibility seam for a future producer is the
  schema contract + registry compatibility rules (§7), not speculative
  architecture. Any second producer must publish the same Avro schemas to
  the same topics under the same compatibility mode.
- **`LapsedAlert` is a committed output edge, not optional.** `spec.md`
  §8.2 phrases the second "harder" timer as optional, but the
  implementation plan commits to it (Phase 7 task: lapsed timer
  registration; Phase 10 task: `LapsedAlert` sink). The *consumer* shown
  (care management / escalation) is illustrative; the topic and its
  contract are committed.
- **RxVigilance is a derived-state system, not a system of record.** Its
  keyed state is reconstructible in principle from the fill-event stream;
  claims adjudication remains the source of truth for what was dispensed.
  Consequence: the acceptable data-loss window (§5, NFR targets) can be
  bounded by "temporarily degraded alerting" rather than "corrupted
  ledger" semantics — state loss is a quality degradation, never a
  financial/clinical record loss.
- **Reference data enters as broadcast state, not as an event stream.**
  Fills/reversals are keyed, stateful stream input; the two lookup tables
  (NDC → class + standard-formulation flag; (class, channel) → lead days)
  are slowly-changing, small, and needed by every event — replicated to
  all tasks via Flink broadcast state (`spec.md` §5, §5.1, §6).

---

## 2. Keying strategy — FINAL DECISION (closes `spec.md` §11 item 2)

> **Decision: `keyBy(member_id, drug_class)` is final for v1.**
> Cross-class alerting, if ever required, will be implemented as a
> downstream consumer of `gap-risk-alerts` keyed by `member_id` — it is
> not grounds to re-key this pipeline.

### 2.1 Why this decision is a one-way door

Flink state and savepoints are organized per key. Changing the keying
scheme after go-live makes existing savepoints unrestorable — recovery
would require an offline State Processor API migration or a from-empty
restart (and go-live seeding is already decided against, §1.3). Keying is
therefore treated like a database primary key: final at design time.

### 2.2 Options considered

- **A — `keyBy(member_id, drug_class)`** (~30–34M keys): one key per
  member×class, one `AdherenceState` and one active timer per key.
- **B — `keyBy(member_id)` + `MapState<drug_class, AdherenceState>`**
  (~16–20M keys): one key per member, classes multiplexed inside the key.

### 2.3 Rationale (decisive factors)

1. **Timer semantics.** A Flink `onTimer` callback receives only (key,
   timestamp). Under A, one key has exactly one active timer — the firing
   context is unambiguous. Under B, a comorbid member holds multiple
   independent timers on one key; disambiguating which drug class fired
   requires a reverse `timestamp → drug_class` index maintained through
   every fill/reversal/cancellation, doubling the failure surface of the
   pipeline's core differentiating component (Phase 7).
2. **Cross-class alerting has a cheaper home.** The one real advantage of
   B — same-member visibility across classes — is not a v1 requirement
   (`spec.md` §5) and, if required later, is served by a small downstream
   job consuming `gap-risk-alerts` (a few thousand events/day) keyed by
   `member_id`. Paying a one-way-door cost in a 30M-key stateful pipeline
   for a feature obtainable downstream at trivial scale is a bad trade.
3. **Neutral factors, stated honestly:** total state volume is identical
   under both options (same data, different arrangement); RocksDB access
   cost per event is comparable (`ValueState` get/put vs `MapState`
   per-entry access). State efficiency does not favor either option.
4. **Operational simplicity.** One key = one therapy timeline: simplest
   unit for debugging, for the Phase 6–8 `TestHarness` suites, and for
   TTL (a discontinued therapy expires its own key; under B the map entry
   lingers while any other therapy keeps the key alive). Load also
   distributes more evenly (comorbid members spread across slots).

---

## 3. Alert retraction semantics — FINAL DECISION (closes `spec.md` §11 item 1)

> **Decision: supersede-by-key; no retraction event type in v1.**
> `GapRiskAlert`s are immutable point-in-time facts. The latest alert per
> `(member_id, drug_class)` supersedes all earlier ones for that key. The
> pipeline guarantees a superseding alert whenever a reversal invalidates
> an already-emitted one.

### 3.1 The semantic foundation

An alert is a **fact**, not a command: "at time T, based on all fills
known at T, coverage was predicted to end on date X." Facts are never
retroactively wrong; they are superseded by newer facts. Downstream
consumers must treat the alert stream accordingly (contract in §8).

### 3.2 Why retraction is semantically unreachable

A reversal can only **remove** coverage, never extend it. Therefore, for
an already-emitted alert, a later reversal can only make the prediction
*more* urgent (coverage ends sooner) or *already breached* (no coverage
left). No reversal can produce the message a retraction exists to carry —
"ignore that alert, the risk is gone." An all-clear can only result from
a **new fill**, which cancels/re-registers the timer so no stale alert
fires in the first place. A retraction event type would exist to carry a
message its only trigger can never truthfully produce.

### 3.3 Options rejected

- **Compensating `AlertRetracted` events:** requires alert identity
  tracking and retraction-capable consumers (a notification system cannot
  unsend an SMS); imposes cross-event ordering handling (retraction
  arriving before its alert) on every consumer; and per §3.2 the message
  would be false in every reversal scenario. Rejected.
- **Pure no-op (the previous documented limitation):** leaves one case
  silently broken — see §3.4. Rejected in favor of closing that hole.

### 3.4 The correction guarantee (new, binding on Phase 8)

If a reversal invalidates an already-emitted alert:
- **Coverage remains** → the recomputed timer fires and emits the
  superseding alert with the corrected, earlier `expiresOn`. (Existing
  mechanics — no new machinery.)
- **No coverage remains** → per `spec.md` §8.3.4 no new timer is
  registered, so nothing would ever supersede the stale alert. Therefore
  the reversal handler **emits the corrective alert immediately in
  `processElement`**. This is a deliberate Phase 8 scope refinement and
  is exactly the "alert already fired" case whose explicit, tested
  behavior Phase 8's Definition of Done already demands.

### 3.5 Contract consequences (feeds §8)

- Alert schema must carry the fields that make supersession computable:
  the key `(member_id, drug_class)` plus an event-time
  `emitted_at`/`as_of` timestamp; consumers keep latest-per-key.
- The alert topic is eligible for Kafka key-compaction: late-joining
  consumers then receive exactly the latest prediction per key.

---

## 4. Component architecture

### 4.1 The job graph

```
 KAFKA API (Redpanda Cloud, SASL_SSL)   FLINK JOB (GKE, Flink Kubernetes Operator)
┌──────────────────┐
│ fill-events      │──KafkaSource──┐
├──────────────────┤               ├─► union ─► ChronicDrugClassFilter ─► keyBy(member_id, ─► AdherenceTracker ─┬─► sink: gap-risk-alerts
│ reversal-events  │──KafkaSource──┘           (BroadcastProcessFn:        drug_class)        (KeyedBroadcast-  ├─► sink: lapsed-alerts
├──────────────────┤                            filter + ENRICH with                          ProcessFn: state, ├─► sink: pdc-snapshots
│ ndc-drug-class-  │──broadcast─────────────►   drug_class, FR-9 check,                       timers, reversal  │
│   ref            │                            heartbeat drop)                               unwind)           │
├──────────────────┤                                                     ┌──────────────────►    │              │
│ alert-lead-time- │──broadcast──────────────────────────────────────────┘                       ▼              │
│   ref            │                                                                     RocksDB state backend  │
├──────────────────┤                                                                     + event-time timers    │
│ gap-risk-alerts  │◄────────────────────────────────────────────────────────────────────────────────────────────┘
│ lapsed-alerts    │◄──        exactly-once sinks           Schema Registry ◄── all sources/sinks (Avro)
│ pdc-snapshots    │◄──                                     Checkpoints/savepoints ──► GCS bucket
└──────────────────┘
        ▲
        │ heartbeat records (watermark advancement — §4.5)
┌───────┴──────────┐
│ heartbeat        │
│ producer (cron)  │
└──────────────────┘
```

Seven topics (Phase 3's provisioning list), one Flink job, two broadcast
joins, three outputs. Every operator carries a frozen `.uid()` from birth
(`CLAUDE.md` §2.4) — the precondition for Phase 13's savepoint-based
zero-downtime deploys.

### 4.2 Enrich-then-key: the filter stage has a second job

`keyBy(member_id, drug_class)` requires `drug_class`, which is **not on
the event** — it comes from the NDC broadcast lookup. The broadcast join
therefore sits **before** the keyBy, and `ChronicDrugClassFilter` is
filter-**and-enrich**: it drops out-of-scope NDCs (acute; specialty/
infusion per FR-9), and stamps survivors with `drug_class` so the keyBy
has a key to derive. Structural rule recorded: a derived key's derivation
must sit upstream of the partitioning, which places the NDC reference
lookup on the critical path of the pipeline.

### 4.3 Two input topics ⇒ cross-topic ordering is not guaranteed

Kafka orders per partition; `fill-events` and `reversal-events` are
separate topics. A reversal can therefore reach `AdherenceTracker`
**before** the fill it reverses. The architecture admits
"reversal referencing an unknown `original_claim_id`" as a *normal*
arrival, not an error; Phase 8 defines the concrete handling (with a
metric either way). The alternative — one merged, member-keyed topic —
would require changing the upstream producer's contract, which is outside
our boundary (§1.3).

### 4.4 One job, not two

Filter/enrich and adherence tracking stay in a single Flink job. A
two-job split with an intermediate topic would add a serialization
round-trip, an extra topic, independent failure domains, and end-to-end
exactly-once complexity — for no benefit at ~40 events/sec peak, since
both stages scale together. Revisit only if the filter stage ever needs
independent scaling or its output gains other consumers.

### 4.5 Watermark stall & the heartbeat producer — DECIDED

Alerts fire from event-time timers; event time advances only via
watermarks; watermarks advance only when events arrive. During globally
quiet periods (overnight — pharmacies closed), watermarks freeze and
**every pending gap-risk timer freezes with them** until the next
morning's first event. Per-partition `withIdleness` handles skewed
partitions, not a globally idle stream.

> **Decision: a synthetic heartbeat producer.** A trivial scheduled
> producer emits a watermark-only heartbeat record to each `fill-events`
> partition every few minutes. Heartbeats carry current event time,
> advance watermarks, and are dropped by `ChronicDrugClassFilter` (they
> never reach keyed state). Alert latency becomes bounded by the
> heartbeat interval, independent of traffic. The rejected alternative —
> documenting overnight stalls as a latency caveat — would make alert
> delivery contingent on unrelated traffic, which contradicts the NFR
> latency target (§5) and the pipeline's core purpose (spec §2 #1).

Consequences: the heartbeat producer is a (small) new deployable owned by
this project (built in Phase 5 alongside reference-data plumbing;
deployed in Phase 13); heartbeat schema registered like any other (§7);
the filter's drop logic and a "heartbeat lag" metric become explicit
Phase 4/5 test and observability items.

### 4.6 Platform fixings

- **State backend:** RocksDB with **incremental checkpoints to a GCS
  bucket** (deployment target is GKE); savepoints to the same bucket.
- **Serialization:** Avro end to end; every source/sink is
  schema-registry-bound (§7 chooses the registry).
- **Connectivity:** Redpanda Cloud over `SASL_SSL` per the Phase 3
  pattern (vendor decision 2026-07-05).
- **Deployment:** Flink Kubernetes Operator on GKE (Phase 13).

---

## 5. NFR targets

Discipline applied: every number traces to a consumer's real need or a
mechanical constraint of the architecture — no invented precision, no
hand-waving. Capacity basis: `docs/scale_and_capacity_estimates.md`
(~12–15 ev/s avg, 30–40 peak; ~30–34M live keys/timers; 6–13.6 GB state).

### 5.1 The targets

| NFR | Target | Traces to |
|---|---|---|
| Alert latency: event-time threshold crossing → alert visible on `gap-risk-alerts` | **p99 ≤ 10 min** (typical ~6 min) | heartbeat 5 min + checkpoint 60 s + propagation margin |
| Fill/reversal arrival → keyed state updated | **p99 ≤ 5 s** | ample headroom at 40 ev/s peak |
| Checkpointing | **60 s interval, incremental (RocksDB→GCS), 30 s min-pause, 10 min timeout, unaligned OFF** | per-minute deltas are KB–MB at ~1M events/day; no backpressure at this throughput |
| Availability (processing job) | **99.9% monthly; RTO ≤ 10 min** | derived-state system, async consumers — downtime delays alerts, never loses/corrupts data; restore ≤13.6 GB from GCS ≈ 2–3 min + restart |
| Degraded-mode alarm (pager threshold, distinct from "down") | input-topic consumer lag **sustained > 15 min** | fires before the latency SLA is breached |
| Data loss | **RPO = 0** for outages ≤ input-topic retention (**7 days**); loss of the checkpoint bucket itself → **accepted from-empty restart** | replayable sources + checkpointed offsets + transactional sinks; cold-start posture deliberately mirrors the go-live no-seeding decision (§1.3) |
| Heartbeat cadence | **5 min** | drives the alert-latency SLA (§4.5) |

### 5.2 The two couplings worth recording

1. **Alert latency ⇔ checkpoint interval.** Exactly-once Kafka sinks are
   transactional: records become visible to read-committed consumers only
   when the covering checkpoint completes. End-to-end latency therefore
   has a hard floor of ~one checkpoint interval regardless of processing
   speed. These two NFRs are one decision wearing two hats.
2. **Alert latency ⇔ heartbeat cadence.** During quiet traffic, a
   threshold crossing is *noticed* only when the next heartbeat advances
   the watermark (§4.5). SLA = heartbeat + checkpoint + margin, by
   construction.

### 5.3 Availability classification (why 99.9 and not more)

The pipeline is not user-facing and its consumers are asynchronous
topics. While paused: Kafka buffers events, state rests in the last
checkpoint, timers fire on restore after watermark catch-up. Downtime =
delayed alerts, never lost or wrong data. A 99.99% target would buy
multi-region Flink HA for a system where a 20-minute pause is clinically
invisible against day-granularity data — an unjustified spend. 99.9%
(~44 min/month) with JobManager HA via the Flink K8s Operator is the
defensible target.

### 5.4 Open verification item (flagged, owned by Phase 3)

Kafka **transactional-producer support on the Redpanda trial/serverless
tier** must be verified during Phase 3 connectivity validation — the
exactly-once sink design (Phase 10) and the visibility semantics above
depend on it. If unsupported on the tier: fallback is at-least-once +
idempotent consumers, which changes the §8 contract — a decision to
escalate the moment it's known, not in Phase 10.

---

## 6. PHI/security posture

*(This section's field inventory is the gating deliverable for Phase 11.
All controls below become the Phase 11 checklist's substance.)*

### 6.1 Framing

PHI = health information joined to an identifier. Two governing
consequences: (1) the health info alone is sensitive — `drug_class`
reveals a diagnosis — so controls cannot focus solely on `member_id`;
(2) **tokenization reduces exposure, it does not declassify** — a
detokenization path must exist for outreach, so tokenized records remain
re-identifiable-by-linkage and every control (encryption, ACLs, log
hygiene) still applies.

### 6.2 PHI field inventory (Phase 11 gate)

| Field | Classification | Appears in | Handling |
|---|---|---|---|
| `member_id` | Direct identifier — PHI | inputs, keyed state, checkpoints, all outputs | tokenized upstream (§6.3); never in logs/metrics/errors |
| `ndc_code` | Health info (drug ⇒ condition) | inputs, filter stage | needed for function — protect, don't mask |
| `drug_class` (derived) | Health info (⇒ diagnosis) | key, state, outputs | same |
| `fill_date`, `day_supply`, `quantity`, `refills_authorized` | Health info / date identifiers | inputs, state, snapshots | same |
| `rx_number` | Direct identifier | inputs only | **dropped at filter stage** — never enters keyed state |
| `pharmacy_id` | Quasi-identifier (location) | inputs only | **dropped at filter stage** (v1 uses `dispensing_channel` for the lead-time lookup) |
| `claim_id`, `original_claim_id` | Indirect identifiers | inputs, state | retained — required for reversal unwind (§3) and dedupe |

**Data minimization is the strongest control:** the filter stage is the
PHI choke point — what passes it is exactly what the pipeline is entitled
to hold. Fields not needed by the logic do not get carried "just in case."

### 6.3 Tokenization — DECIDED: upstream, at the ingestion boundary

The producer publishes an already-tokenized `member_id`; raw identifiers
never enter this system's topics, state, checkpoints, or outputs.
Detokenization exists only in the outreach system via a secured token
vault, at the moment of member contact. Recorded as a **producer
contract requirement** in §8.

Rejected: pipeline-side tokenization (an external service's latency and
availability on the critical path of every event, and raw identifiers
would already have transited the input topics — too late by
construction); no-tokenization (raw identifiers in every checkpoint and
topic; one misconfigured consumer ACL exposes identified health records).

Build honesty: with no real upstream producer in this project, upstream
tokenization is an asserted contract plus synthetic test data — which is
the correct HLD form regardless (the boundary owns it, §1.3 pattern).

### 6.4 Encryption

- **In transit:** `SASL_SSL` to Redpanda (Phase 3); TLS to schema
  registry and GCS.
- **At rest:** Redpanda Cloud-managed topic-storage encryption; GKE
  persistent disks (RocksDB working state) encrypted by default; and —
  the commonly forgotten surface — **the GCS checkpoint bucket is a full
  copy of all keyed state, i.e., PHI at rest outside Kafka**: default
  encryption (CMEK if key custody is required), IAM restricted to the
  pipeline service account + break-glass, no public access, GCS audit
  logging enabled.

### 6.5 Topic ACLs (least privilege per service account)

| Service account | Access |
|---|---|
| `adjudication-producer` | WRITE `fill-events`, `reversal-events` |
| `refdata-producer` | WRITE the two `*-ref` topics |
| `heartbeat-producer` | WRITE `fill-events` |
| `rxvigilance-pipeline` | READ inputs + refs; WRITE the three output topics |
| `outreach-consumer` | READ `gap-risk-alerts`, `lapsed-alerts` |
| `adherence-calc-consumer` | READ `pdc-snapshots` |

Phase 3's DoD already requires proving an out-of-scope access *fails*.

### 6.6 Audit & hygiene

- **Alert emission is self-auditing:** output topics are immutable,
  offset-ordered facts (a consequence of §3's supersede semantics) — the
  topic is the audit trail; retention/compaction therefore has audit
  implications (recorded in §8).
- Reference-data changes audit via the ref topics (append log).
- GCS access → cloud audit logs; cluster admin actions → Redpanda audit
  logging (**trial-tier availability: Phase 3 verification item**).
- **No PHI in logs, metric labels, or exception messages — ever**
  (`CLAUDE.md` §3 binds it; Phase 11 adds a log-scrubbing test to prove
  it).
- Test fixtures: synthetic members only (feeds the Phase 2 test-data
  plan).

---

## 7. Schema registry strategy — DECIDED

> **Decision: Redpanda Cloud's built-in schema registry
> (Confluent-API-compatible), with `FULL_TRANSITIVE` compatibility on all
> subjects.**

### 7.1 Registry choice

The registry is the contract-enforcement point for every arrow in the §1
context diagram: an incompatible schema is rejected at **produce-time**
instead of breaking consumers at runtime. The Phase 1 task predates the
vendor swap ("Confluent/Apicurio choice"); Redpanda Cloud ships a
built-in registry speaking the Confluent API, so:

- **Confluent SR / Apicurio:** rejected — each adds a hosted/self-hosted
  deployable solely to provide an API the cluster already exposes.
- **Redpanda built-in:** zero extra infrastructure; standard client code
  (`KafkaAvroSerializer`, Flink's
  `ConfluentRegistryAvroDeserializationSchema`) works unchanged, so the
  client-side pattern remains the industry-standard Confluent pattern.
- Honest tradeoff: registry availability is coupled to the cluster
  vendor — acceptable shared fate, since the pipeline is unusable
  without the cluster anyway.
- Registry **auth on the trial tier** is a Phase 3 verification item;
  registry credentials are `credential-sensitive`.

### 7.2 Compatibility mode — an organizational decision, decided

Compatibility modes encode deploy ordering: `BACKWARD` ⇒ consumers
upgrade first; `FORWARD` ⇒ producers first; `FULL` ⇒ no ordering;
`*_TRANSITIVE` ⇒ compatible with *all* prior versions, protecting
consumers replaying old data (ours do: compacted alert topics, 7-day
input retention).

Our consumers are other teams whose deploy schedules this project does
not control (§1.3). Any mode requiring cross-team deploy sequencing is a
standing organizational risk. Therefore **`FULL_TRANSITIVE` everywhere**.
Cost accepted: evolution is limited to adding/removing optional
fields with defaults — no renames, no type changes. Escape hatch for a
truly breaking change: a new topic + versioned subject with deliberate
consumer migration, never a compatibility-mode downgrade.

### 7.3 Mechanics

- **Subject naming:** `TopicNameStrategy` — one schema per topic,
  **8 subjects** (7 topics + the heartbeat record; no unregistered
  "special" messages).
- **Schemas as code:** `.avsc` files versioned in this repo; registered
  idempotently against the dev registry in Phase 2 (which finalizes the
  schema contents); CI-enforced registration in later phases.

---

## 8. Downstream interface contracts

A topic contract is five things: **schema + key + delivery guarantee +
topic configuration + consumer obligations**. Each contract below states
all five.

### 8.1 `gap-risk-alerts`

**Schema** (Avro, subject `gap-risk-alerts-value`):

| Field | Type | Meaning |
|---|---|---|
| `member_id` | string | **tokenized** (§6.3) — never raw |
| `drug_class` | string | tracked class |
| `predicted_exhaustion_date` | date | supply end given all known fills |
| `alert_lead_days` | int | resolved lookup value used (audits *why now*) |
| `as_of` | timestamp-millis | event time of the prediction — the supersession ordering field (§3.5) |
| `trigger` | enum `TIMER` \| `REVERSAL_CORRECTION` | scheduled warning vs. §3.4's immediate corrective emission |

- **Key:** `(member_id, drug_class)` — the supersession unit.
- **Delivery:** exactly-once, transactional. Consumers **must** set
  `isolation.level=read_committed`.
- **Consumer obligations:** latest-per-key wins; alerts are immutable
  facts; no retraction event type exists (§3).
- **Topic config:** `cleanup.policy=compact,delete`, delete retention
  30 days — reconciles the §6.6 audit-trail role (full ordered history
  for 30 days) with §3.5 compaction (latest alert per key survives
  indefinitely, so new consumers bootstrap current risk state).
  Compliance-grade audit beyond 30 days = archival sink to GCS
  (Phase 14 backlog item).

### 8.2 `lapsed-alerts`

Same key, delivery, obligations, and topic config as §8.1. Schema:
`member_id`, `drug_class`, `supply_exhausted_on` (date), `as_of`.
Semantics: the gap-risk window closed without a refill — supply is now
exhausted. The escalation fact, deliberately minimal.

### 8.3 `pdc-snapshots`

**Schema:** `member_id`, `drug_class`, `covered_intervals`
(array of `{start: date, end: date}`), `total_days_covered` (int),
`as_of` (timestamp-millis).

The FR-6 boundary is enforced **by the shape itself**: no ratio field, no
denominator, no measurement-year — the schema physically cannot carry a
finished PDC. The adherence-calc consumer performs year-boundary slicing
against `covered_intervals` (FR-6c), which is why the intervals ship and
not just the count.

- **Key:** `(member_id, drug_class)`. **Delivery:** exactly-once.
- **Emission:** on every state change (fill or reversal).
- **Topic config:** `cleanup.policy=compact` — snapshots are cumulative;
  latest-per-key is the complete current picture.

### 8.4 Upstream (producer) contract — inputs

1. `member_id` arrives **already tokenized** (§6.3); raw identifiers on
   the input topics are a producer defect.
2. Avro via the registry, `FULL_TRANSITIVE` (§7) — mechanically enforced
   at produce-time.
3. NCPDP grain: one claim = one event; `original_claim_id` present on
   every REVERSAL.
4. Producer delivery may be **at-least-once**: the pipeline dedupes by
   `claim_id` (mechanism: Phase 9; obligation: contractual now).
5. No cross-topic ordering promise between `fill-events` and
   `reversal-events` (§4.3); reversal-before-fill is handled, not owed.

### 8.5 Contingency, restated

Every exactly-once guarantee above depends on the Phase 3 verification of
transactional-producer support on the Redpanda trial tier (§5.4). If it
fails: contracts degrade to at-least-once + consumer idempotency by
`(key, as_of)` — a contract change, to be decided the moment it is known,
which is why the check precedes any consumer integration.
