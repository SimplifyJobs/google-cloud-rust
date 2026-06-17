# Implementation Plan: Datastore data-plane client (`google.datastore.v1`)

> Status: proposal / working plan. Tracking issue:
> [#5907](https://github.com/googleapis/google-cloud-rust/issues/5907).
> This document lives on the `feat-datastore-data-plane` working branch of the
> `SimplifyJobs/google-cloud-rust` fork. It is a planning artifact and is not
> required to land in upstream as part of the feature PRs.

## 1. Goal and non-goals

### Goal

Deliver an idiomatic, production-quality Rust client for the Cloud Datastore
**data-plane** API (`google.datastore.v1`): `Lookup`, `RunQuery`,
`RunAggregationQuery`, `BeginTransaction`, `Commit`, `Rollback`, `AllocateIds`,
and `ReserveIds`. The client must feel "natural" to Rust developers and match
the ergonomics of the canonical Datastore clients (Go, Java, Python, Node),
while complying with every standard in `ARCHITECTURE.md`, `CONTRIBUTING.md`, and
the conventions of the existing hand-crafted veneers (Spanner, Firestore).

The crate is `google-cloud-datastore`, already reserved in `librarian.yaml`
(`output: src/datastore`, `skip_release: true`) by the scaffold from #1802.

### Non-goals

- The admin API (`google-cloud-datastore-admin-v1`) is already generated and is
  out of scope.
- Releasing the crate. It stays `skip_release` until the API surface settles.
- Datastore Mode for Firestore migration tooling, GQL parsing client-side, or
  any offline/local query engine.

## 2. Architecture decision

**Datastore is "Firestore-shaped, minus gRPC."**

Firestore is the reference hand-crafted data-plane veneer: it vendors its
generated client inside `src/firestore/src/generated` (via librarian `modules`)
and adds a thin hand-written layer. Firestore uses gRPC **only because** it has
bidirectional streaming (`Listen`, `Write`).

The Datastore data-plane has **no streaming** and full REST/JSON support, and
per the maintainer rationale in #2655 it carries **none** of the correctness
hazards (lease extension, chunk-to-row merging, resume-token rewind, client-side
checksums) that justify a thick client for Pub/Sub, Bigtable, Spanner, Storage,
or Firestore.

Decision:

> Vendor a generated `google.datastore.v1` GAPIC using the default **REST/HTTP
> (`reqwest`) transport** inside the reserved `src/datastore` crate, and build an
> idiomatic hand-written layer on top of it. This mirrors the Firestore crate
> structure but uses the REST transport instead of gRPC, keeping it aligned with
> the project's "REST/JSON over gRPC" default policy.

Alternatives considered and rejected:

- **Plain generated `google-cloud-datastore-v1`** (standalone GAPIC): correct
  and policy-compliant, but forces users to hand-assemble
  `Entity { properties: HashMap<String, Value> }`. No canonical Datastore client
  does this. Acceptable as a stepping stone (Phase 1) but not the end state.
- **gRPC + prost like Firestore**: adds the heavy dependency the project
  explicitly avoids, with zero benefit because there is no streaming. Rejected.

The generated GAPIC provides, for free: the `Key`/`Entity`/`Value`/`Query`/
`Mutation` model types with manual ProtoJSON ser/de, the `Stub` trait (enabling
mocking via `from_stub`), `RequestOptions`/retry/backoff plumbing, and the REST
transport. The hand-written layer's sole job is ergonomics.

## 3. Module layout

```
src/datastore/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs                 # wiring + crate docs (mirror firestore/spanner lib.rs)
│   ├── generated/             # vendored google.datastore.v1 REST GAPIC (librarian)
│   ├── client.rs              # Datastore + Datastore::builder(); emulator detection
│   ├── builder.rs             # public re-exports of request builders
│   ├── key.rs                 # idiomatic Key + key! macro
│   ├── value.rs               # Value wrapper + Kind accessors
│   ├── to_value.rs            # ToValue trait + scalar/array/option impls
│   ├── from_value.rs          # FromValue trait + impls + serde_json bridge
│   ├── entity.rs              # ToEntity / FromEntity traits
│   ├── query.rs               # Query / Filter / Order builders + cursor stream
│   ├── aggregation.rs         # count / sum / avg
│   ├── transaction.rs         # closure runner + read-only snapshots
│   ├── mutation.rs            # batched insert/update/upsert/delete + transforms
│   ├── error.rs               # ConvertError + helpers (gax::Error is the public error)
│   └── retry_policy.rs        # DatastoreRetryPolicy (ABORTED + idempotent transports)
└── tests/                     # integration tests (emulator + production smoke)

src/datastore-derive/          # proc-macro crate for #[derive(Entity)] (feature: derive)
```

## 4. API design summary

Illustrative signatures (final names TBD in review). Conventions follow Spanner:
`set_*` for domain fields, `with_*` for gax options; `::builder()` everywhere;
terminal `.build()`; fallible `try_*` accessors; `#[non_exhaustive]` enums.

### Value conversion (foundation, mirrors `spanner::{to_value, from_value}`)

```rust
pub trait ToValue   { fn to_value(&self) -> Value; }
pub trait FromValue: Sized { fn from_value(v: &Value) -> Result<Self, ConvertError>; }
// impls: i64, f64, bool, String/&str, Vec<u8> (blob), wkt::Timestamp,
//        GeoPoint (google_cloud_type::LatLng), Key, Vec<T> (array),
//        Option<T> (null), nested entity-as-Value, serde_json::Value bridge.
```

### Entity mapping (the headline; see §11 open question 1)

```rust
#[derive(Entity)]
struct Task {
    #[datastore(key)]             id: Key,
    #[datastore(rename = "desc")] description: String,
    #[datastore(noindex)]         notes: String,   // exclude_from_indexes
    done: bool,
    #[datastore(skip)]            cache: Cache,
    tags: Vec<String>,
}
```

The derive emits `ToEntity`/`FromEntity`. Attributes mirror Go's proven set:
`key`, `rename`, `noindex`, `skip`, `flatten`.

### Keys

```rust
let k = Key::builder("Task").id(42).build();
let child = Key::builder("Task").name("inbox").parent(&root).build();
let incomplete = Key::incomplete("Task");          // id allocated on insert
let k = key!["Customer" => 1, "Order" => "A17"];   // ancestor path
```

### Client surface (matches Go/Java/Python/Node)

```rust
let db = Datastore::builder().build().await?;       // auto-detects DATASTORE_EMULATOR_HOST

let task: Option<Task> = db.get(&key).await?;
let tasks: Vec<Option<Task>> = db.get_multi(&keys).await?;

let key = db.put(&task).await?;                     // upsert, single NON_TRANSACTIONAL commit
db.put_multi(&tasks).await?;
db.insert(&task).await?; db.update(&task).await?; db.delete(&key).await?;

let mut it = db.query::<Task>(Query::new("Task")
    .filter(Filter::eq("done", false))
    .order(Order::desc("priority"))
    .limit(20));
while let Some(task) = it.next().await { /* ... */ }

let n: i64 = db.aggregate(Query::new("Task")).count().await?;

db.run_in_transaction(async |txn| {
    let mut t: Task = txn.get(&key).await?.unwrap();
    t.done = true;
    txn.put(&t)?;                                   // buffered, committed atomically
    Ok(())
}).await?;

db.allocate_ids(&incomplete_keys).await?;
db.reserve_ids(&keys).await?;
```

### Two implementation subtleties that distinguish "best" from "fine"

1. **Cursor-driven query streaming.** `RunQuery` does not use page tokens. It
   returns a `QueryResultBatch` with `end_cursor` and
   `more_results ∈ {NOT_FINISHED, MORE_RESULTS_AFTER_LIMIT,
   MORE_RESULTS_AFTER_CURSOR, NO_MORE_RESULTS}`. The generated `by_page`/
   `by_item` paginators do **not** apply. We implement a custom stream that
   re-issues `RunQuery` with `start_cursor = previous end_cursor`, decrementing
   the remaining `limit`, until `more_results != NOT_FINISHED`. We expose both
   `.next().await` (typed items) and `.by_batch()` (cursor access for
   checkpoint/resume).

2. **Mutually exclusive read options.** `ReadOptions` is a oneof:
   `read_consistency` (STRONG/EVENTUAL) XOR `transaction` XOR `new_transaction`
   XOR `read_time`. The typed API makes illegal states hard to construct (e.g.
   `EVENTUAL` is unsettable once a transaction is attached).

3. **Transactions.** Read-write transactions retry on `ABORTED` and pass
   `previous_transaction` on retry. Use a closure-based runner identical in
   spirit to Spanner's `TransactionRunner`, plus read-only snapshots
   (`read_time`) and single-use `new_transaction` for atomic read-then-commit.

## 5. Phased delivery plan

Each phase is one reviewable PR. All PRs: conventional-commit title
`feat(datastore): ...` (or `chore`/`test`/`docs`), copyright header on every new
file, all quality gates green (§7), and reference the relevant issue.

| # | PR title | Scope | Acceptance criteria |
|---|----------|-------|---------------------|
| 0 | `chore(datastore): file design issue + sub-issues` | Open data-plane tracking issue (done: #5907) and per-phase sub-issues; get maintainer sign-off on §11 open questions before code. | Maintainer agreement on transport placement and entity-mapping approach recorded on the issue. |
| 1 | `feat(datastore): vendor google.datastore.v1 REST client` | Add `librarian.yaml` `modules` config for `src/datastore`; run `librarian generate`; wire top-level `Cargo.toml`; crate builds with the raw generated client. | `cargo build -p google-cloud-datastore` passes; raw `Lookup`/`Commit` reachable; no veneer yet. |
| 2 | `feat(datastore): add Key, Value, ToValue/FromValue` | `key.rs`, `value.rs`, `to_value.rs`, `from_value.rs`, `key!` macro, `error.rs` (`ConvertError`). | Round-trip unit tests for every `Value` variant incl. arrays, null, nested entity, GeoPoint, blob vs string; `clippy-strict` clean. |
| 3 | `feat(datastore): #[derive(Entity)] + ToEntity/FromEntity` | New `datastore-derive` proc-macro crate; `entity.rs` traits; attributes `key`/`rename`/`noindex`/`skip`/`flatten`. | Struct round-trips to/from `Entity`; per-field index exclusion verified; UI/compile-fail tests for bad attributes. |
| 4 | `feat(datastore): client CRUD (get/put/delete + _multi)` | `client.rs`, `builder.rs`, emulator detection in `Factory`, non-transactional commit path, `MutationResult` handling (allocated keys, versions). | Emulator integration test for CRUD; mock-stub unit tests; `from_stub` works. |
| 5 | `feat(datastore): typed query builder + cursor stream` | `query.rs`: `Query`/`Filter` (incl. composite AND/OR, IN/NOT_IN/HAS_ANCESTOR), `Order`, projection, `distinct_on`, `keys_only`, ancestor, GQL escape hatch; cursor-driven async stream. | Streamed paging across batch boundaries verified against emulator; `more_results` handling correct; checkpoint via `by_batch()`. |
| 6 | `feat(datastore): aggregation queries` | `aggregation.rs`: count/sum/avg with aliases. | Emulator tests for each aggregation; null/overflow semantics documented. |
| 7 | `feat(datastore): transactions` | `transaction.rs`: closure runner with ABORTED retry + `previous_transaction` replay, read-only snapshots, `new_transaction`; `retry_policy.rs` (`DatastoreRetryPolicy`). | Contention/retry test; read-only snapshot test; total/per-attempt timeout budgets honored. |
| 8 | `feat(datastore): advanced writes` | Property transforms (increment/min/max/array/server-time), optimistic concurrency (`base_version`/`update_time` + conflict strategy), `property_mask`. | Transform + conflict-detection emulator tests. |
| 9 | `docs(datastore): user guide chapter + samples` | `guide/` chapter; compilable samples under `guide/samples`; crate `README.md`. | `mdbook test guide` passes; `cargo build -p user-guide-samples` passes. |
| 10 | `test(datastore): endurance + coverage hardening` | Add to `tests/endurance` smoke path; raise coverage; auto-trait assertions. | Endurance run clean; `cargo llvm-cov` meets project bar. |

Dependencies: 1 → 2 → {3, 4}; 4 → {5, 7}; 5 → 6; 7 → 8; all → {9, 10}. Phases 3
and 4 can proceed in parallel after 2.

## 6. Generation mechanics (Phase 1)

`librarian.yaml` — replace the placeholder `google-cloud-datastore` entry with a
`modules` configuration analogous to Firestore, but targeting the REST GAPIC
(no `grpc-client`/`prost` templates). Sketch (exact templates confirmed with the
librarian team during Phase 1):

```yaml
- name: google-cloud-datastore
  copyright_year: "2025"
  output: src/datastore
  skip_release: true
  rust:
    modules:
      - output: src/datastore/src/generated
        api_path: google/datastore/v1
        # REST GAPIC template (same family as datastore-admin-v1), not grpc-client
```

Then:

```bash
V=$(go run github.com/googleapis/librarian/cmd/librarian@latest config get version)
go run github.com/googleapis/librarian/cmd/librarian@${V} generate google-cloud-datastore
typos && cargo fmt && cargo build && cargo test && cargo doc
git add src/datastore Cargo.toml Cargo.lock librarian.yaml
```

If `librarian` does not yet support emitting the REST GAPIC under a custom
`output` for a hand-crafted crate, that is a librarian change (see the "Making
changes to librarian" workflow in
`doc/contributor/howto-guide-generated-code-maintenance.md`) and a Phase 1
blocker to raise with the librarian team.

Prerequisites: `protoc >= v23`, Go, Rust >= 1.88. Verify `google/datastore/v1`
exists at the pinned `sources.googleapis.commit` in `librarian.yaml`.

## 7. Quality gates (must be green on every PR)

Run locally before pushing (these mirror the GCB and `sdk.yaml` checks):

```bash
cargo fmt
cargo clippy-strict                              # strict lints for handwritten crates
cargo clippy --profile=test -- --deny warnings
cargo test -p google-cloud-datastore --all-features
cargo test -p google-cloud-datastore --no-default-features
cargo doc -p google-cloud-datastore --all-features --no-deps
cargo deny check licenses bans sources
cargo run --package check-versions
cargo run --package check-copyright              # copyright header on every file
# non-Rust formatters (skip generated + testdata):
typos
git ls-files -z -- '*.toml' ':!:**/generated/**' ':!:**/testdata/**' | xargs -0 taplo fmt
git ls-files -z -- '*.md'   ':!:**/generated/**' ':!:**/testdata/**' | xargs -0 mdformat
git ls-files -z -- '*.yaml' '*.yml' ':!:**/generated/**' ':!:**/testdata/**' | xargs -0 yamlfmt
# docs/samples:
mdbook test guide
cargo build -p user-guide-samples
# minimal + semver (when touching released/core crates):
cargo minimal-versions check --package google-cloud-datastore
```

`clippy-strict` enforces `missing_docs` and `clippy::exhaustive_enums` — every
public item is documented and every public enum is `#[non_exhaustive]`.

## 8. Testing strategy

- **Unit tests:** implement the generated `stub::Datastore` trait (or `mockall`)
  and inject via `Datastore::from_stub(...)`. No network. Cover serialization,
  conversion, query assembly, retry/timeout, and error mapping.
- **Conversion tests:** exhaustive round-trips for `Value`/`Entity` (every
  variant), mirroring `src/firestore/src/convert.rs`.
- **Integration tests:** against the **Datastore emulator**
  (`DATASTORE_EMULATOR_HOST`) for CRUD, queries, aggregations, and transactions;
  plus a minimal production smoke test wired into `tests/integration`.
- **Endurance:** add a Datastore path to `tests/endurance` (many RPCs, no
  leaks/panics).
- **Auto-traits:** `assert_impl_all!(Datastore: Debug, Clone, Send, Sync)` and
  `assert_not_impl_any!(... UnwindSafe ...)`, as Spanner does.
- **Docs build as tests:** doc examples use `# async fn` wrappers and must
  compile; `mdbook test guide` runs guide snippets.

## 9. Process compliance (CONTRIBUTING.md)

- Issue-first: #5907 filed; per-phase sub-issues before each PR.
- CLA signed before the first PR.
- Conventional-commit titles; squash-merge; `Fixes #NNNN` / `For #NNNN`.
- Two Googler approvals per PR (multi-approver check) — expect longer review
  cycles as an external contributor; keep PRs small and focused.
- Every `TODO` links to an issue:
  `// TODO(https://github.com/googleapis/google-cloud-rust/issues/NNNN): ...`.
- PRs opened from this fork with "Allow edits from maintainers" enabled; one
  logical change per PR; ready-for-review only (no long-lived drafts).

## 10. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Maintainers prefer to own the data-plane internally (cf. #5592, BigQuery). | Resolve on #5907 **before** writing code; offer to do generation (Phase 1) regardless and hand off if desired. |
| `librarian` can't emit a REST GAPIC into a custom hand-crafted `output`. | Identified as a Phase 1 blocker; coordinate a librarian change or fall back to a standalone `google-cloud-datastore-v1` (see §11 Q2). |
| `#[derive(Entity)]` scope creep / bikeshedding. | Land trait foundation (Phase 2) first; gate the derive (Phase 3) behind its own design issue and a `derive` feature. |
| Entity-mapping semantics disputes (serde vs bespoke). | Decide via §11 Q1 on the issue with a written rationale before Phase 3. |
| Cursor-stream correctness across batch boundaries / limits. | Dedicated emulator tests for multi-batch paging and `more_results` states (Phase 5). |
| Crate accidentally released while unstable. | Keep `skip_release: true`; `version 0.0.0`; `publish = false`. |

## 11. Open questions for maintainers (resolve on #5907 first)

1. **Entity mapping: bespoke `#[derive(Entity)]` vs. a serde data-format.**
   Recommendation: bespoke `ToValue`/`FromValue` + a derive, consistent with the
   Spanner precedent and able to express per-field index exclusion and
   `Key`/`GeoPoint`/`blob` typing that serde cannot. Offer optional serde-compat
   later.
2. **Transport placement: vendor the REST GAPIC inside `src/datastore` (Firestore
   model) vs. a standalone `google-cloud-datastore-v1` the veneer depends on.**
   Recommendation: vendor, to match the reserved scaffold and the
   Firestore/Spanner precedent — but confirm, since it is hard to reverse.
3. Proc-macro crate naming and whether `derive` is a default feature.
4. Minimum supported emulator behaviors to test in CI.

## 12. References

- Tracking issue: #5907; admin precedent: #979 / #980; scaffold: #1802;
  thick-client rationale: #2655; external-contributor precedent: #5592.
- `ARCHITECTURE.md` (design decisions, REST/JSON over gRPC, ProtoJSON).
- `doc/contributor/howto-guide-generated-code-maintenance.md` (librarian).
- `doc/contributor/howto-guide-set-up-development-environment.md` (toolchain,
  formatters, integration tests).
- Reference veneers: `src/spanner` (modern conventions), `src/firestore`
  (vendored-generated + hand-written layer).
- Datastore data API: <https://cloud.google.com/datastore/docs/reference/data/rpc>.
