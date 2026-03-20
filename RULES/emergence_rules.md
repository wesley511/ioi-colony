# IOI Colony Emergence Rules
## Pure Stigmergic Worker Behavior

---

## Purpose
Define how workers behave as a decentralized swarm without central control.

The colony must produce coordinated intelligence through:
- local worker decisions
- shared blackboard state (OPPORTUNITIES.md)
- reinforcement (positive feedback)
- evaporation (negative feedback)
- duplicate suppression
- evidence-driven convergence

No worker may act as a central planner.

---

## Core Emergence Principle

Global intelligence must emerge from local actions.

Each worker may only use:
- normalized signals
- blackboard state
- rule files (RULES/)
- scoring rules
- decay rules

Workers must not require direct assignment from another worker.

---

## No Central Control Rule

Workers must never:
- assign tasks to other workers
- wait for approval before updating the blackboard
- impose global priorities
- write instructions such as "focus on this"
- behave as coordinator, manager, or "queen"

All coordination must occur indirectly through:
- OPPORTUNITIES.md
- score movement
- reinforcement
- decay
- status transitions

---

## Local Decision Rule

For each validated signal, a worker must choose ONE:

- Ignore
- Create
- Reinforce
- Defer

### Ignore
When:
- validation fails
- confidence < threshold (0.30 default)
- no meaningful evidence
- duplicate noise

### Create
When:
- no matching opportunity exists
- leverage potential is meaningful (> 0.40 expected)
- evidence is credible

### Reinforce
When:
- matching opportunity exists
- evidence is NEW and additive
- signal strengthens urgency, scale, or clarity

### Defer
When:
- borderline signal
- unclear matching
- insufficient evidence

---

## Matching Rule

Before creating, workers MUST attempt match.

Match using:
- category
- signal_type
- semantic intent of description
- overlap of evidence

If match found:
→ Reinforce

If no match:
→ Create

---

## Duplicate Suppression Rule

Workers must NOT create new opportunities if:
- same signal_id already recorded
- same category + signal_type + intent exists
- evidence is already present

Workers must NOT reinforce if:
- no new information is added

---

## Worker Behavioral Constraint

When writing to OPPORTUNITIES.md:

- Only create if leverage_score potential ≥ 0.40
- Never delete another worker’s entry
- Never overwrite unrelated entries
- Never issue instructions to other workers
- Never centralize decisions

Workers contribute signals, not commands.

---

## Reinforcement Rule (Pheromone Increase)

Reinforcement is allowed ONLY if:
- evidence is new
- signal quality is strong
- duplication check passed

Effects:
- increase leverage_score
- increase confidence
- append evidence_sources
- update rationale
- update last_reinforced

Reinforcement must be incremental, not exaggerated.

---

## Evaporation Rule (Pheromone Decay)

All opportunities must decay over time.

Formula:

new_score = max(0.05, old_score × (1 - 0.07)^days_since_update)

Where:
- ρ (rho) = 0.07
- floor = 0.05

Implications:
- stale opportunities fade naturally
- weak ideas disappear without deletion
- no manual cleanup required

Workers must NOT block decay.

---

## Convergence Rule

Multiple signals pointing to same opportunity must:

→ reinforce ONE shared entry  
→ NOT create duplicates  

Desired outcome:
- fewer, stronger opportunities
- high signal-to-noise ratio
- natural prioritization

---

## Divergence Rule

Split opportunity ONLY if:
- evidence clearly separates into distinct categories
- different signal types or markets
- improves clarity

Avoid unnecessary fragmentation.

---

## Priority Emergence Rule

Priority must emerge from:
- repeated reinforcement
- higher confidence
- stronger leverage_score
- broader evidence

Workers must NOT assign priority manually.

---

## Clutter Control Rule

Workers must NOT contribute to clutter.

Clutter occurs when:
- duplicates increase
- weak signals are promoted
- noise accumulates faster than value

Clutter is controlled by:
- evaporation (ρ = 0.07)
- strict validation
- duplicate suppression
- conservative creation thresholds

---

## Human Boundary Rule

Workers must NEVER:
- execute business actions
- place orders
- contact suppliers
- approve decisions
- spend money

Workers provide intelligence only.

---

## Local Failure Containment Rule

Errors must remain local.

System must self-correct via:
- decay
- better signals
- reinforcement override

No single worker can corrupt the colony.

---

## Auditability Rule

Every action must be traceable.

Workers must log:
- decision type (create/reinforce/ignore)
- signal_id
- affected opportunity
- reason

No silent modifications.

---

## Healthy Colony Pattern

- fewer duplicates over time
- strong opportunities rise naturally
- weak ones decay automatically
- signals converge
- no central coordination required

---

## Unhealthy Colony Pattern

- duplicate explosion
- stale dominance
- repeated weak reinforcement
- manual overrides
- central planning behavior
- uncontrolled growth of blackboard

Workers must avoid contributing to this.

---

## Summary Rule

Workers must behave as independent local evaluators whose interactions with the shared blackboard produce a coherent, high-quality global pattern.

That coherence must emerge from:
- reinforcement
- evaporation
- threshold filtering
- convergence

It must NOT be manually imposed or centrally orchestrated.

