# Validation Rules

## Signal Validation

Each signal must contain:
- signal_id
- date
- category
- signal_type
- description
- evidence
- confidence

Reject signal if:
- confidence < 0.30
- missing required fields

## Opportunity Validation

- leverage_score must be between 0.00 and 1.00
- must reference at least one signal_id
- must include last_updated

## Duplicate Protection

- same signal_id cannot be reused
- identical evidence cannot be counted twice

