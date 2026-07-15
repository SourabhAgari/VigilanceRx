# RxVigilance — Project Functional Document

## 1. Business problem

**RxVigilance** addresses the following problem: medication adherence gaps are currently discovered retrospectively — a batch
report at month-end or year-end reveals a member's PDC (Proportion of Days
Covered) fell below the CMS threshold. By then, the member has already gone
without medication for weeks, and there is no time left in the measurement
period to intervene.

This project reframes adherence monitoring as a **real-time, forward-looking**
problem: detect a supply gap before it happens, with enough lead time, while there is still
time for a pharmacist call, a text reminder, or an auto-refill nudge.

## 2. Why it matters financially

- CMS Medicare Part D Star Ratings weight medication adherence measures (for
  diabetes, statins, and hypertension medications) among the highest-weighted
  categories in the overall formula.
- Higher Star Ratings unlock CMS quality bonus payments.
- PDC ≥ 0.80 is the CMS threshold for "adherent." A plan's Star Rating on these
  measures is essentially the percentage of tracked members who cross that line.
- A modest shift in the number of adherent members can move a plan's overall
  Star Rating tier, which has direct revenue consequences at scale.

## 3. Problems with the current daily batch process, and how this project solves them

*(This is the business-language telling of the five batch-vs-real-time
problems; `spec.md` §2 is the canonical/technical version — update that one
first if the underlying reasoning changes.)*

Today, adherence status is typically computed once a day by a batch job. This
creates five recurring problems that this project is specifically designed to
fix.

| # | Problem with the current daily process | How this project solves it |
|---|---|---|
| 1 | **Findings are already old by the time they're seen.** A batch run at midnight won't notice a member's supply running low until up to a full day later, and gives no sense of how close a member is to a gap in between runs. | The system detects the risk the moment it occurs, not on the next scheduled run — alerts go out the same day the configured lead-time threshold is crossed (see FR-8). |
| 2 | **Every member is re-evaluated from scratch, every day.** The batch process re-derives every member's status nightly, regardless of whether anything actually changed for them that day — an expensive and largely redundant exercise at population scale. | Only members with new activity (a fill or a cancellation) trigger any recalculation. Members with no change simply carry their existing status forward at no extra cost. |
| 3 | **Cancelled claims can leave incorrect information in place for a day or more.** If a fill is cancelled mid-day, any report or alert generated before the next batch run may have been based on a claim that no longer counts. | Cancellations are corrected the moment they're received, not on the next scheduled run. |
| 4 | **The process can't represent "still waiting to see what happens."** A daily check can only say "as of last night, this member looked fine or at risk" — it has no way to hold an open prediction like "this member will run out on a specific future date unless they refill." | The system explicitly tracks a forward-looking, per-member prediction that updates or cancels itself the instant new information arrives. |
| 5 | **Every flagged member looks equally urgent.** A once-a-day list has no way to distinguish a case just discovered from one flagged 20 hours ago. | Alerts are produced continuously as they're detected, giving outreach teams a live, freshness-ranked view instead of one flat daily list. |

**Where the daily process still has a place.** Official CMS Star Ratings PDC
figures are measured over a full year, so a daily reconciliation process
remains a reasonable, lower-risk way to produce the official number for
regulatory submission. This project is not intended to replace that
reconciliation — it feeds it, while adding the early-warning capability the
daily process cannot provide on its own.

---

## 4. Why it matters clinically and operationally

- A member who lapses on chronic medication (e.g., a diabetic stopping
  Metformin) is statistically more likely to require costly downstream care —
  adherence monitoring is a medical cost-avoidance lever, not only a quality
  score lever.
- Early, targeted detection lets outreach teams focus limited call-center
  capacity on members who are actually about to lapse, rather than
  blanket-calling every member on a chronic medication.

## 5. In scope

- Members on CMS Star Ratings-tracked chronic/maintenance therapy:
  diabetes, statin (cholesterol), and RAS antagonist (hypertension) drug
  classes.
- Real-time detection of an approaching supply gap, with a configurable lead
  time rather than a single fixed day count (see FR-8).
- Real-time, incrementally-updated PDC computation feeding Star Ratings
  reporting.
- Correct handling of claim reversals so that cancelled fills do not produce
  false alerts or inflate PDC.
- Members with multiple concurrent chronic conditions, tracked independently
  per condition.

## 6. Out of scope

- Acute, short-course prescriptions (e.g., a 7-day antibiotic course). These
  are explicitly excluded — see §7, Functional Rule 1.
- Delivery of the alert itself (SMS, outbound call) — this system produces the
  alert signal; existing outreach systems own delivery.
- Specialty/infusion medications with non-standard dosing schedules (see                                                                                                                                                               
  FR-9).
- Historical backfill/retroactive PDC correction — assumed to be a separate,                                                                                                                                                           
  existing batch reconciliation process. This also means the system does                                                                                                                                                               
  **not** pre-load a member's existing coverage at launch (or at any later                                                                                                                                                             
  onboarding of a new drug class or population). **Decision:** no go-live                                                                                                                                                              
  state seeding will be built. The system starts with empty state and                                                                                                                                                                  
  learns each member's coverage naturally from their next fill after                                                                                                                                                                   
  go-live. Accepted consequence: for up to ~90 days after launch (the                                                                                                                                                                  
  longest realistic day supply), a member whose coverage began before                                                                                                                                                                  
  go-live may have a real gap the system cannot detect, since it has no                                                                                                                                                                
  knowledge of the fill that started that coverage. This is a known,                                                                                                                                                                   
  accepted limitation, not an oversight — see `spec.md` §7 for the                                                                                                                                                                     
  corresponding technical note.

## 7. Actors

| Actor | Role |
|---|---|
| Member | Person filling prescriptions; subject of adherence tracking |
| Pharmacy | Submits fill/reversal claims |
| Prescriber | Writes the original prescription, including refill authorization |
| Outreach team / system | Consumes gap-risk alerts and acts on them |
| Star Ratings reporting | Consumes PDC output for CMS submission |

## 8. Functional requirements (business rules)

**FR-1 — Scope filter.** The system must only track fills belonging to a
defined list of chronic drug classes (diabetes, statin, RAS antagonist). A fill
for a drug outside these classes — regardless of its refill count or day
supply — must never generate a coverage prediction or an alert.

> Example: a 7-day, 0-refill Amoxicillin prescription must be excluded                                                                                                                                                                 
> entirely. A 30-day Metformin fill showing 0 refills remaining (because the                                                                                                                                                           
> authorization expired, not because therapy is complete) must still be                                                                                                                                                                
> tracked — refill count alone is not a valid basis for exclusion.

**FR-9 — Specialty/infusion exclusion.** The system must exclude specialty                                                                                                                                                             
and infusion-administered medications from tracking, even when the                                                                                                                                                                     
underlying drug would otherwise belong to a tracked chronic drug class                                                                                                                                                                 
(diabetes, statin, RAS antagonist — see FR-1). A drug's presence on the                                                                                                                                                                
tracked-class list is not sufficient by itself to include it; each NDC must                                                                                                                                                            
be independently confirmed as a standard, non-specialty, non-infusion                                                                                                                                                                  
formulation before its fills are tracked.   

This splits into two sub-requirements:

- **FR-9a — Drug-class membership does not override specialty/infusion                                                                                                                                                                 
  status.** Being classified as diabetes, statin, or RAS-antagonist (FR-1)                                                                                                                                                             
  does not automatically qualify a fill for tracking. Exclusion must be                                                                                                                                                                
  determined by an explicit, positive classification signal per NDC — never                                                                                                                                                            
  inferred by omission. ("Not marked specialty/infusion" is not sufficient                                                                                                                                                             
  grounds to include a fill; the system must positively confirm standard                                                                                                                                                               
  formulation status.)
- **FR-9b — Non-standard supply patterns must be excluded regardless of drug                                                                                                                                                           
  class.** Coverage tracking (FR-2) depends on a simple day-supply                                                                                                                                                                     
  calculation. Any drug whose supply cannot be meaningfully expressed this                                                                                                                                                             
  way — dose-titration schedules, clinician-administered dosing visits,                                                                                                                                                                
  variable-interval specialty dispensing — must be excluded, since including                                                                                                                                                           
  it would silently produce incorrect coverage intervals rather than a                                                                                                                                                                 
  visible error.

> Example: suppose a member's diabetes medication is dispensed through a                                                                                                                                                               
> specialty pharmacy channel with clinician-managed, variable-interval                                                                                                                                                                 
> administration rather than a fixed day supply. Even though "diabetes" is a                                                                                                                                                           
> tracked class under FR-1, this specific NDC must still be excluded — the                                                                                                                                                             
> exclusion is about the *administration/dispensing pattern*, not the drug                                                                                                                                                             
> class. An oral Metformin fill for the same member continues to be tracked                                                                                                                                                            
> normally.

*(This closes the gap in the §6 "Out of scope" bullet, which named                                                                                                                                                                     
specialty/infusion drugs as excluded without defining how or by what signal                                                                                                                                                            
that exclusion happens. See `spec.md` §3 and §6 for the corresponding                                                                                                                                                                  
technical filter-stage implications.)*

**FR-2 — Coverage tracking.** For every tracked member and drug class, the
system must continuously know the date through which the member currently has
medication on hand, based on all fills received so far.

**FR-3 — Early warning.** The system must produce a gap-risk signal a                                                                                                                                                                  
configurable number of days before a member's known supply is due to run out,                                                                                                                                                          
provided no refill has occurred by that point. This lead time is a business                                                                                                                                                            
parameter, not a fixed value — see FR-8.

**Known limitation — cannot distinguish a true gap from a legitimate                                                                                                                                                                   
discontinuation.** The system has no way to know whether a member's absence                                                                                                                                                            
of further fills means they need a refill (a real gap) or that a                                                                                                                                                                       
prescriber intentionally stopped that therapy (no gap at all — the member                                                                                                                                                              
is doing exactly what they were told). No discontinuation signal exists in                                                                                                                                                             
the event data (see `spec.md` §4), so this cannot be distinguished                                                                                                                                                                     
upstream. **Decision:** accepted as a known limitation for v1, rather than                                                                                                                                                             
building a discontinuation-signal intake (a larger feature requiring a new                                                                                                                                                             
upstream data source). Consequence: some gap-risk and lapsed alerts will be                                                                                                                                                            
false positives for members whose therapy was legitimately discontinued.                                                                                                                                                               
Downstream outreach systems consuming these alerts should maintain their                                                                                                                                                               
own suppression mechanism for members they've already confirmed as                                                                                                                                                                     
discontinued, since RxVigilance cannot make that distinction itself.

**FR-8 — Configurable alert lead time.** The number of days of advance warning
must not be a single fixed value applied uniformly to every member and drug.
It must be configurable based on:

- **Drug class / clinical risk** (e.g., a higher-risk medication may warrant a
  shorter, tighter warning window than a lower-risk one).
- **Dispensing channel** (mail-order fills need more lead time than retail
  pickup, to account for shipping/fulfillment delay).
- **Supply cycle length** (a fixed number of days is a much larger or smaller
  fraction of total supply depending on whether the fill is for 14, 30, or 90
  days).

Member-level, behavior-based adjustment (e.g., a longer lead time for members
with a documented history of late refills) is a reasonable future extension
but is not required for the initial release. This lead-time value is expected
to change over time as business rules evolve, and should be managed as
configuration/reference data rather than embedded in code.

**FR-4 — Warning cancellation.** If a member refills before a previously
predicted gap would occur, the system must not surface that warning — it
silently replaces the prediction with one based on the new fill.

**FR-5 — Reversal correction.** If a previously reported fill is later
cancelled/reversed, the system must recompute the member's coverage as though
that fill never occurred, and cancel or correct any prediction that depended
on it.

**FR-6 — Adherence scoring.** The system must maintain a running, date-level                                                                                                                                                           
record of medication coverage per member and drug class, sufficient to                                                                                                                                                                 
support PDC (Proportion of Days Covered) reporting for CMS Star Ratings —                                                                                                                                                              
following the same PQA-endorsed measure definition CMS already requires for                                                                                                                                                            
the diabetes, statin, and RAS antagonist classes in scope (see §5).

This splits into three explicit sub-requirements:

- **FR-6a — Coverage is period-agnostic.** The system tracks *which calendar                                                                                                                                                           
  dates* a member had medication on hand, independent of any measurement                                                                                                                                                               
  year. It does not reset, roll over, or bucket coverage by year internally.
- **FR-6b — RxVigilance does not compute the final PDC ratio.** The system                                                                                                                                                             
  emits coverage facts (covered date ranges and a running day-count) via the                                                                                                                                                           
  PDC snapshot output. It does not determine a member's measurement-year                                                                                                                                                               
  denominator, does not apply PQA population-eligibility rules (e.g., the                                                                                                                                                              
  2-fill-minimum inclusion rule, hospice exclusions, disenrollment                                                                                                                                                                     
  truncation), and does not perform the division. A separate, existing                                                                                                                                                                 
  adherence-calculation system owns the official ratio and CMS submission —                                                                                                                                                            
  this system's output is a feed into that process, not a replacement for                                                                                                                                                              
  it (see §3, "Where the daily process still has a place").
- **FR-6c — Period boundaries are resolved by whoever consumes the coverage                                                                                                                                                            
    feed, not baked into this system's state.** When an official PDC figure is                                                                                                                                                           
    needed for measurement year Y, the consuming system slices this system's                                                                                                                                                             
    covered-date ranges against that member's index prescription start date                                                                                                                                                              
    (their first qualifying fill in year Y) through the earliest of Dec 31 of                                                                                                                                                            
    year Y, plan disenrollment, or death — per standard PQA measure                                                                                                                                                                      
    methodology. A fill spanning a year boundary (e.g., Dec 20–Jan 19)                                                                                                                                                                   
    therefore correctly contributes covered days to both years' figures once                                                                                                                                                             
    sliced, without requiring any special-case handling inside this system.

> Example: Priya's Dec 20 fill (30-day supply, covering through Jan 19)                                                                                                                                                                
> contributes 12 covered days toward last year's PDC and 19 covered days                                                                                                                                                               
> toward this year's PDC — but only once a downstream consumer performs                                                                                                                                                                
> that slice against each year's window. RxVigilance itself just knows                                                                                                                                                                 
> "Priya had medication on hand Dec 20–Jan 19"; it has no concept of                                                                                                                                                                   
> "last year" or "this year."

*(This resolves the open item previously listed in `spec.md` §11, point 4.                                                                                                                                                             
See `spec.md` §7, §10, and §11 for the corresponding technical-state                                                                                                                                                                   
implications.)*

**FR-7 — Independent tracking per condition.** A member on more than one
tracked chronic medication (e.g., diabetes and a statin) must be tracked
independently per condition. Adherence on one medication must not be affected
by, or averaged with, adherence on another.

## 9. Illustrative scenario

*(This walkthrough uses a 5-day lead time as a concrete example. Per FR-8, the
actual value used in production would be resolved per drug class and
dispensing channel, not hardcoded.)*

| Date | Event | System behavior |
|---|---|---|
| Jan 1 | Member fills a 30-day diabetes medication | Coverage recorded through Jan 30. Early-warning check scheduled for Jan 25. |
| Jan 3 | That Jan 1 fill is reversed by the pharmacy/insurer | Coverage recomputed as if the fill never happened; the Jan 25 check is cancelled. |
| Jan 5 | Member fills again, 30-day supply | Coverage recorded through Feb 3. New check scheduled for Jan 29. |
| Jan 29 | No further fill has occurred | Gap-risk alert issued: supply exhausts in 5 days. |
| Feb 2 | Member refills just in time | Coverage extended through Mar 4. Prior alert condition is superseded; new check scheduled. |

## 10. Success criteria

- Gap-risk alerts are emitted within a bounded delay (target: under 1 hour) of                                                                                                                                                         
  a member crossing their configured alert lead-time threshold (FR-8), for                                                                                                                                                             
  any coverage the system has itself observed via a fill event. This                                                                                                                                                                   
  criterion does not apply to coverage that began before go-live and was                                                                                                                                                               
  never seeded into the system — see §6, "Historical backfill" note.
- Zero gap-risk alerts generated for out-of-scope (acute) medications.
- Reversed fills do not leave stale alerts unaddressed downstream.
- PDC figures computed by this system reconcile with the existing batch/
  reporting process within an agreed tolerance.

## 11. Known open items

- Whether a reversed fill that already triggered an alert should produce an
  explicit retraction, or whether downstream consumers treat alerts as
  best-available-information at time of emission.
- Whether cross-condition alerting (a member lapsing on two or more tracked
  medications simultaneously, a stronger clinical risk signal) is a future
  requirement — this would affect the keying/state design described in the
  companion technical spec.

## 12. Related documents

- `spec.md` — technical design: event schema, state model, Flink-specific
  implementation approach, scale estimates.
