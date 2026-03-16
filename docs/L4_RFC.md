# L4 Hardening vs Rewrite RFC (Historical)

> This RFC captures a past design discussion and is retained for context.
> Current behavior is documented in `README.md` and `docs/SCHEMA.md`.
> Where this file conflicts with current implementation/docs, treat this file as historical.

## Context

Recent correctness issues (coinbase visibility, V0 fallback matching, recipient typing drift) indicate the current L4 implementation is sensitive to join assumptions and missing intermediate rows.

Performance risk is primarily in repeated per-credit lookups performed in `L4Client::update_blocks_impl`:

- Per credit note: query `notes` for version, then optionally `notes` again for JAM decoding.
- Per V1 credit: query `name_to_lock`, then query `spend_conditions`.
- Per block: decode block JAM for coinbase recipient mapping.

This creates an effectively quadratic behavior shape across larger sync windows due to repeated round-trips and repeated row-level decoding.

## What Was Added Now

- Block-level profiling logs in L4 (`l4 block derivation profile`) including:
  - `tx_credit_count`
  - `coinbase_credit_count`
  - derived `credit_info_rows`
  - per-block elapsed time
- Recipient typing normalization now includes explicit `v0pk` classification.
- Query-side wallet matching supports `pk`, `v0pk`, and DB public key fallback.

## Options

### Option A: Harden existing schema/implementation

Pros:
- Fastest path to ship.
- No migration risk.

Cons:
- Keeps multi-query per-credit pattern.
- Complexity remains high in update path.

### Option B: Rewrite implementation, keep schema (**recommended**)

Pros:
- Can preserve existing DB/API contracts.
- Enables batch prefetch strategy per block range:
  - preload `(first -> version, jam)` once,
  - preload `(first -> lock_root)` once,
  - preload `(lock_root -> spend_condition)` once,
  - derive recipients in-memory.
- Eliminates most repeated lookup overhead.

Cons:
- Medium engineering effort.
- Requires careful parity testing with current behavior.

### Option C: Schema redesign + implementation rewrite

Pros:
- Best long-term model clarity (explicit recipient identity table).

Cons:
- Highest migration and rollout risk.
- Requires versioning and backfill strategy.

## Recommendation

Proceed with **Option B** in the next iteration:

1. Keep current schema and external output contracts.
2. Replace row-by-row lookup pattern with per-range prefetch maps.
3. Keep new profiling logs as guardrails and compare before/after metrics.
4. Retain `lock` only for truly unresolved V1 recipients.
5. Revisit schema redesign only after measured bottlenecks remain.

## Acceptance Criteria for Option B

- Same result counts for `credit_info` as baseline on fixture ranges.
- Recipient type distribution remains deterministic across reruns.
- No regression in V0/V1 coinbase inclusion and audit invariants.
- Per-block elapsed time decreases materially on representative sync windows.
