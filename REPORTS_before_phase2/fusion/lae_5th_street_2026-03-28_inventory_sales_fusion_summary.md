# Inventory + Sales Fusion Summary

- Branch: **lae_5th_street**
- Report date: **2026-03-28**
- Data completeness: **incomplete**
- Signal families available: **0**
- Release execution score: **N/A**
- Release execution band: **N/A**
- Limited fusion score: **N/A**
- Limited fusion band: **N/A**
- Fusion score: **N/A**
- Fusion band: **N/A**
- Summary: **sales=unavailable | inventory_summary=unavailable | inventory_release=unavailable**

## Sales

- Available: **False**
- Source file: **N/A**
- Total sales: **N/A**
- Cash: **N/A**
- Card: **N/A**
- Z reading: **N/A**
- Traffic: **N/A**
- Conversion rate: **N/A**

## Inventory Summary

- Available: **False**
- Source file: **N/A**
- Events count: **0**
- Section count: **0**
- Avg signal strength: **N/A**

## Inventory Release

- Available: **False**
- Source file: **/home/clawadmin/.openclaw/workspace/ioi-colony/SIGNALS/normalized/lae_5th_street/2026-03-28/lae_5th_street_bale_release_2026-03-28.json**
- Released value: **N/A**
- Released qty: **N/A**
- Bale entries detected: **0**
- Parsed branch: **lae_5th_street**

## Diagnostics

- [medium] insufficient_signal_families: Insufficient validated signal families for fusion scoring.
- [medium] sales_missing: Daily sales event is unavailable for the requested branch/date.
- [medium] inventory_summary_missing: Inventory availability summary is unavailable for the requested branch/date.

## Recommended Actions

- Ingest the day-end sales report for this branch/date before treating the result as business fusion.
- Generate the inventory availability summary under REPORTS/inventory for this branch/date.
