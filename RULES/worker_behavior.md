# IOI Colony Worker Behavior Engine

## Purpose
Define how workers process signals, update opportunities, and maintain the blackboard.

---

## Core Loop (Every Worker Executes)

1. Read new signals
2. Match signal to existing opportunities
3. Decide:
   - reinforce existing opportunity
   - create new opportunity
4. Update blackboard
5. Apply decay to stale opportunities

---

## Step 1 — Read Signals

- scan SIGNALS/normalized/
- process only signals with status: new

---

## Step 2 — Match Logic

Match signal to opportunity if:

- same category
- same signal_type
- similar description or intent

If match found:
→ go to reinforcement

If no match:
→ create new opportunity

---

## Step 3 — Create Opportunity

Create new entry in OPPORTUNITIES.md with:

- source signal_id
- initial leverage_score (0.50–0.70 range)
- confidence from signal
- evidence_sources
- status: Active

---

## Step 4 — Reinforcement Logic

If matching opportunity exists:

- check evidence is NEW (not duplicate)
- apply reinforcement delta based on strength
- update:
  - leverage_score
  - last_reinforced
  - last_updated
  - evidence_sources

---

## Step 5 — Duplicate Protection

Do NOT reinforce if:

- same signal_id already used
- identical evidence already recorded

---

## Step 6 — Decay Application

For each opportunity:

- check last_reinforced date
- if stale → apply decay

New Score =
Current Score * (1 - decay_rate)

---

## Step 7 — Archive Logic

Move opportunity to Archived if:

- score < 0.20
- no updates over time

---

## Step 8 — Priority Awareness

Workers should prioritize:

1. High urgency signals
2. High confidence signals
3. Multi-source confirmations

---

## Step 9 — Output Discipline

Every update must:

- be traceable to signal_id
- include reason for change
- update timestamps

---

## Behavioral Principle

Workers do NOT guess.

Workers act only on:
- evidence
- rules
- validated signals

