# IOI Colony Opportunities Blackboard

## Purpose
This blackboard is the shared stigmergic environment for IOI Colony workers.

Workers must:
- add new opportunities when valid signals appear
- update existing opportunities when better evidence arrives
- reinforce only with material evidence
- allow stale entries to decay through evaporation rules
- preserve clarity, consistency, and auditability

---

## Active Opportunities

### [Initial Monitoring State]

- source: system initialization
- date_identified: 2026-03-20
- description: IOI Colony deployed on Contabo VPS and awaiting first monitored signals.

- leverage_score: 0.10
- risk_level: Low
- confidence: 0.90

- score_components:
  - revenue: 0.10
  - scalability: 0.10
  - ease: 0.90
  - strategic: 0.50
  - wellbeing: 0.40

- evidence_sources:
  - local deployment completed
  - workspace initialized
  - rules and mission files created

- rationale: Colony is operationally deployed, but no external or business signal has yet been evaluated.

- last_reinforced:
  - date: 2026-03-20
  - delta: 0.00
  - reason: initial baseline entry

- status: Active
- review_status: Pending
- last_updated: 2026-03-20

---

## Exploring Opportunities

<!-- Use this section for emerging, uncertain, or partially supported opportunities -->

---

## Priority Review Queue

<!-- Move opportunities here when leverage_score > 0.80 or when prompt human review is recommended -->

---

## Archived Opportunities

<!-- Move stale, invalid, completed, or decayed opportunities here with history preserved -->

---

## Blackboard Rules

### 1. Entry format
Every opportunity should contain:
- source
- date_identified
- description
- leverage_score
- risk_level
- confidence
- score_components
- evidence_sources
- rationale
- last_reinforced
- status
- review_status
- last_updated

### 2. No duplicate opportunities
If an opportunity already exists:
- update the existing entry
- merge evidence
- do not create noisy duplicates

### 3. Confidence discipline
Use confidence conservatively:
- 0.00-0.30 = weak support
- 0.31-0.60 = partial support
- 0.61-0.80 = strong support
- 0.81-1.00 = very strong support

### 4. Review status values
Use only:
- Pending
- Seen
- Prioritized
- Archived

### 5. Status values
Use only:
- Active
- Exploring
- Archived

### 6. Human boundary
This file supports human decisions only.
It must never be used to auto-authorize:
- purchases
- trades
- negotiations
- approvals
- commitments
- external contact

### 7. Swarm principle
Workers coordinate by changing this environment.
No worker assigns tasks to another worker.
Intelligence emerges through updates to the blackboard.

