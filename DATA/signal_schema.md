# IOI Colony Signal Schema

## Purpose
This schema standardizes how raw business signals are converted into colony-readable inputs.

## Required Fields

- signal_id
- date
- source_type
- source_name
- category
- signal_type
- description
- evidence
- urgency
- confidence
- suggested_opportunity
- status

## Source Types
- branch_observation
- customer_request
- inventory_report
- supplier_update
- sales_pattern
- manual_entry

## Signal Types
- demand_gap
- stockout
- overstock
- pricing_opportunity
- supplier_advantage
- allocation_gap
- margin_opportunity
- trend_signal
- operational_inefficiency

## Confidence Guidance
- 0.00-0.30 = weak
- 0.31-0.60 = partial
- 0.61-0.80 = strong
- 0.81-1.00 = very strong

## Status Values
- new
- processed
- archived

