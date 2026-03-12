# PR Clean Checklist

This is a living checklist to keep PRs focused on production/runtime changes.
Update this file whenever local-only work is added (tests, tooling workarounds, notes).

## Current Snapshot

### Keep In PR (Production Flow)

- [ ] `src/layers/l2/schema.rs` (new L2 runtime schema/models)
- [ ] `src/layers/l2/mod.rs` runtime layer logic (`L2Client`, `LayerBase`, `LayerImpl`)
- [ ] `src/layers/l1/mod.rs` dependency propagation fix (`L1` now forwards `update_blocks` to downstream deps like `L2`)
- [ ] `src/layers/mod.rs` (`pub mod l2`)
- [ ] `src/main.rs` wiring (`L2Client` added under `L1Client`)
- [ ] `migrations/sqlite/2026-03-11-000002_l2/up.sql`
- [ ] `migrations/sqlite/2026-03-11-000002_l2/down.sql`
- [ ] `Cargo.toml` dependency alignment to upstream `iris-grpc-proto` git source
- [ ] `Cargo.lock` updates required by dependency resolution

### Local-Only / Optional (Drop Before PR if not desired)

- [ ] `src/layers/l2/mod.rs` `#[cfg(test)] mod tests` block (local DB-backed validation harness)
- [ ] `Cargo.toml` `dev-dependencies.iris-crypto` (only needed by local L2 signer test)
- [ ] `agent.md` (session guidance file, not runtime)
- [ ] Any temporary local DB path assumptions (`TEST_DB_PATH`, `/tmp/nockdb.sqlite`) in developer notes/scripts

## Pre-PR Sanity Pass

- [ ] Confirm runtime-only diff is intentional (`git diff`)
- [ ] If dropping local-only tests, remove corresponding dev-dependency entries
- [ ] Run compile checks:
  - [ ] `cargo +nightly check`
- [ ] Run relevant tests for included code:
  - [ ] `cargo +nightly test layers::l2::tests:: -- --nocapture` (if test module kept)
- [ ] Re-run lints/diagnostics for touched files
- [ ] Re-open this file and mark final keep/drop decisions

## Decision Log

Use this section to record what was intentionally removed before PR:

- _Example_: "Removed `agent.md` and local `#[cfg(test)]` L2 harness for production-only PR."

---

## Maintenance Rule

Whenever we add new non-production changes (temporary debugging, local-only tests, compatibility shims, helper notes), append them to **Local-Only / Optional** immediately so cleanup is explicit at PR time.
