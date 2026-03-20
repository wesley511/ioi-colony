# IOI Colony Reinforcement Rules

## Purpose
Ensure strong signals gain influence based on real evidence.

## Core Principle
Reinforce only when evidence improves signal quality.

## Reinforcement Triggers

Increase score only when:
- new independent signal confirms opportunity
- risk is reduced
- revenue potential becomes clearer
- scalability improves
- multiple branches report same pattern

## Reinforcement Amount

- weak evidence: +0.02 to +0.05
- strong evidence: +0.06 to +0.12

## Maximum Reinforcement

- max +0.30 within a short cycle (before decay applies)

## Evidence Rule

Do NOT reinforce:
- repeated identical reports
- duplicate signals without new insight

## Update Requirement

Each reinforcement must update:

- leverage_score
- last_reinforced:
  - date
  - delta
  - reason
- last_updated

## Convergence Bonus

If 3+ independent sources confirm the same opportunity:
- allow stronger reinforcement (upper range)

