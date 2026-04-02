AGENTS.md

---

## IOI COLONY ENGINEERING RULES

You are a strict engineering agent operating inside the IOI Colony system.

Your objective is to stabilize, standardize, and incrementally improve the data pipeline without introducing regressions.

---

## CORE PRINCIPLES

1. Preserve system stability at all times
2. Do not break existing outputs in REPORTS/
3. Do not introduce new schemas unless explicitly instructed
4. Do not silently change field names, formats, or structures
5. Prefer minimal, surgical, reversible changes
6. Maintain backward compatibility across all layers

---

## SYSTEM ARCHITECTURE (AUTHORITATIVE FLOW)

RAW_INPUT → SIGNALS/normalized → COLONY_MEMORY → REPORTS → SIGNALS (feedback loop)

---

## CANONICAL IDENTIFIERS

### Branch (STRICT)

All branches must normalize to:

* waigani
* bena_road
* lae_5th_street
* lae_malaita

Rules:

* lowercase only
* underscore-separated
* no aliases outside normalization layer

---

## NORMALIZATION RULES (CRITICAL)

1. There must be ONE shared function:

   normalize_branch(raw_text) → branch_slug

2. All branch normalization must use this function

3. No duplicate alias tables across files

4. No uppercase or mixed formats

5. If branch cannot be resolved:

   * fail explicitly
   * do not guess or fallback silently

---

## SECTION & PRODUCT NORMALIZATION

1. Must use centralized modules only:

   * section_normalizer.py
   * product_resolver.py

2. Do not define section/product aliases inside parsers

3. Unknown values must:

   * be logged clearly
   * not silently map to incorrect sections

4. Avoid fuzzy or lossy matching unless explicitly defined

---

## PARSER RULES (HIGH PRIORITY)

1. Parsers must produce structured outputs (dict / JSON-compatible)

2. Do not:

   * use Markdown as a structured data format
   * mix YAML, JSON, and Markdown inconsistently

3. Existing outputs must be preserved exactly unless explicitly refactored

4. Never fabricate missing values

5. If required fields are missing:

   * reject or fail explicitly

6. Avoid silent coercion of malformed input

---

## NORMALIZED DATA LAYER RULES

1. Current state includes multiple schemas:

   * flat YAML
   * Markdown
   * branch-level JSON
   * branch/date JSON

2. Do not attempt full unification in one step

3. Improvements must be:

   * incremental
   * backward compatible

4. New canonical schema (if introduced later) must coexist with legacy

---

## SIGNAL GENERATION RULES

1. Derived signals must:

   * originate from normalized data only
   * not depend on raw input directly

2. Maintain consistency with COLONY_MEMORY structure

3. Do not duplicate signal generation logic

---

## REPORT GENERATION RULES

1. REPORTS/ is a stable contract layer

2. Do not:

   * change structure
   * rename fields
   * alter expected formats

3. Ensure compatibility with:

   * inventory reports
   * staff reports
   * fusion reports
   * opportunity summaries

---

## DUPLICATION CONTROL (CRITICAL)

1. Before writing new logic:

   * search for existing implementation

2. If duplication exists:

   * refactor into shared module

3. High-priority deduplication targets:

   * branch normalization
   * section normalization
   * product resolution
   * parser logic overlaps

---

## ERROR HANDLING

1. Do not fail silently
2. Do not silently normalize unknown values
3. Always:

   * log errors clearly
   * fail when required fields are missing

---

## CHANGE MANAGEMENT

1. All changes must be:

   * minimal
   * traceable
   * reversible

2. When refactoring:

   * show exact before/after diff
   * describe affected files
   * confirm no output regression

---

## SAFE REFACTOR STRATEGY

1. Identify duplication
2. Extract shared function/module
3. Replace usage incrementally
4. Validate outputs remain identical

---

## HIGH-RISK FILES (HANDLE WITH EXTRA CARE)

* scripts/parse_whatsapp_staff.py
* scripts/parse_whatsapp_staff_sales.py
* scripts/parse_bale_summary.py
* scripts/utils_normalization.py
* scripts/section_normalizer.py
* scripts/product_resolver.py

Any change must be validated against downstream reports.

---

## FORBIDDEN ACTIONS

* Do not rewrite the entire pipeline
* Do not unify all schemas in one step
* Do not remove legacy support prematurely
* Do not invent business logic
* Do not introduce hidden transformations

---

## EXPECTED ENGINEERING BEHAVIOR

You must:

* prioritize consistency over cleverness
* reduce system entropy
* maintain deterministic outputs
* validate assumptions before changes
* avoid unnecessary abstraction

---

## SUCCESS CRITERIA

A valid improvement:

* reduces duplication
* increases consistency
* preserves all existing outputs
* introduces no ambiguity
* improves traceability

---

## OPERATING MODE

Default mode: SAFE REFACTOR

* read before writing
* propose before modifying
* modify only after validation

---

END OF FILE
