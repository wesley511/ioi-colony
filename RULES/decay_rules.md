# IOI Colony Decay Rules

## Purpose
Ensure stale or weak opportunities lose influence over time.

## Core Principle
If no new evidence appears, the opportunity must decay.

## Decay Model

- Daily decay rate: 7% (0.07)

New Score =
Current Score × (1 - 0.07)

## Minimum Floor
- Scores cannot go below 0.05 automatically
- Below 0.05 requires manual archival or removal

## Decay Triggers

Apply decay when:
- no reinforcement within 24–48 hours
- no new evidence added
- no signal updates

## Decay Effects

- lowers priority over time
- pushes weak opportunities toward archive
- prevents stale signals from dominating

## Archive Rule

Move to Archived if:
- score < 0.20 AND
- no update for extended period

## Exception

Do NOT decay if:
- new evidence added
- score reinforced recently
- opportunity under active human review

