# IOI Colony Scoring Rules
## Leverage Score, Risk Logic, Reinforcement, and Escalation

## Purpose
These rules define how IOI Colony evaluates and ranks opportunities in a consistent, evidence-aware, swarm-friendly way.

The colony must prioritize:
- high leverage
- low risk
- scalability
- operational feasibility
- alignment with business strengthening and human benefit

## Core Score

Each opportunity receives a Leverage Score between 0.00 and 1.00.

Use the following weighted model:

Leverage Score =
0.30 × Revenue Potential
+ 0.25 × Scalability
+ 0.20 × Ease of Implementation
+ 0.15 × Strategic Alignment
+ 0.10 × Wellbeing Alignment
- Risk Penalty

All component values must be normalized between 0.00 and 1.00.

Final score must be clamped:
- minimum: 0.00
- maximum: 1.00

## Component Definitions

### 1. Revenue Potential
How much financial upside the opportunity may create if humans act on it.

Guidance:
- 0.00–0.20 = negligible upside
- 0.21–0.40 = small upside
- 0.41–0.60 = moderate upside
- 0.61–0.80 = strong upside
- 0.81–1.00 = exceptional upside

### 2. Scalability
How repeatable, expandable, or systematizable the opportunity is.

Guidance:
- 0.00–0.20 = one-off only
- 0.21–0.40 = limited repeatability
- 0.41–0.60 = moderate repeatability
- 0.61–0.80 = highly repeatable
- 0.81–1.00 = strong compounding potential

### 3. Ease of Implementation
How practical it is for humans to test or execute the opportunity.

Guidance:
- 0.00–0.20 = very difficult
- 0.21–0.40 = difficult
- 0.41–0.60 = manageable
- 0.61–0.80 = easy
- 0.81–1.00 = very easy

### 4. Strategic Alignment
How strongly the opportunity strengthens the business over the long term.

Guidance:
- 0.00–0.20 = weak alignment
- 0.21–0.40 = partial alignment
- 0.41–0.60 = useful alignment
- 0.61–0.80 = strong alignment
- 0.81–1.00 = core strategic fit

### 5. Wellbeing Alignment
How much the opportunity supports positive human outcomes, team benefit, customer value, or organisational resilience.

Guidance:
- 0.00–0.20 = no clear benefit
- 0.21–0.40 = weak benefit
- 0.41–0.60 = useful benefit
- 0.61–0.80 = strong benefit
- 0.81–1.00 = exceptional benefit

## Risk Penalty

Risk must reduce enthusiasm, not be ignored.

Use these standard penalties:

- Low Risk    = 0.05
- Medium Risk = 0.15
- High Risk   = 0.30

If an opportunity is highly speculative, uncertain, or weakly supported, do not assign Low Risk.

## Score Bands

Interpret final Leverage Score as follows:

- 0.00–0.20 = weak signal / monitor only
- 0.21–0.40 = possible opportunity / early exploration
- 0.41–0.60 = meaningful opportunity / worth attention
- 0.61–0.80 = strong opportunity / human review recommended
- 0.81–1.00 = exceptional opportunity / priority human review

## Reinforcement Rules

A score may increase only when at least one of the following occurs:

- new supporting evidence appears
- risk is reduced by new information
- scalability becomes clearer
- financial upside becomes more credible
- multiple independent signals converge on the same opportunity

Do not reinforce scores based on repetition alone.

Recommended reinforcement step:
- small evidence update: +0.02 to +0.05
- strong confirming evidence: +0.06 to +0.12

Never increase score beyond 1.00.

## Evaporation Interaction

Opportunity scores decay over time through colony evaporation rules.

Workers must not manually preserve stale opportunities without evidence.

Evaporation exists to:
- reduce noise
- remove stale signals
- keep the blackboard adaptive
- prevent dead opportunities from dominating attention

## Escalation Thresholds

Use these thresholds for worker behavior:

- Score < 0.21
  - monitor only
  - do not escalate

- Score 0.21–0.40
  - keep in Exploring or Active
  - gather more evidence

- Score 0.41–0.60
  - mark as meaningful
  - keep visible for human scanning

- Score 0.61–0.80
  - recommend human review
  - maintain Active unless risk rises

- Score > 0.80
  - mark as priority candidate
  - recommend prompt human review

## Confidence Discipline

A high score does not mean certainty.
A low score does not always mean uselessness.

Workers must be conservative when:
- evidence is sparse
- timing is unclear
- source reliability is weak
- risks are hard to estimate

If uncertain, reduce score instead of inflating it.

## Duplicate Opportunity Rule

If the same opportunity appears multiple times:
- merge evidence into one entry
- do not create unnecessary duplicates
- reinforce only if the new evidence is materially useful

## Human Boundary

Scores are advisory signals only.

Scores do not authorize:
- execution
- purchases
- negotiations
- commitments
- trading
- approvals

All action remains human-controlled.

## Summary Principle

The colony should prefer:
- fewer high-quality opportunities
- higher signal-to-noise
- conservative scoring
- evidence-backed reinforcement
- adaptive decay of weak signals

