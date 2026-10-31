# Database design contract

Applies to content, Explorer, social and every domain added to `catalyst`: our
application of Richard Fabian's [*Data-Oriented Design*](https://www.dataorienteddesign.com/dodbook/)
to PostgreSQL. Fewer crates is an architecture target; table count follows
the data and its operations.

## Principles and decisions

- **Describe the workload first**: inputs, outputs, row counts, skew, fan-out,
  read/write rates, payload sizes, freshness. Keep expected and measured scale
  apart.
- **Store facts with explicit keys and dependencies.** Separate independently
  changing relationships; declare uniqueness, required values and ranges.
  Give every derived value a named source and update rule.
- **Process useful sets.** Select due work and needed rows in SQL, transform
  bounded batches, keep external validation out of the commit transaction.
- **Move fewer bytes.** Filter and deduplicate keys before fetching large
  payloads; choose indexes from predicates and ordering.
- **Confirm improvements** against an explicit latency, throughput or resource
  budget, write costs included.

## Schema and ownership

1. A schema names an authority and lifecycle. Shared facts have one
   authoritative relation; join across schemas in SQL. Match meanings and keys
   before merging duplicate schemas or same-named tables. Preserve source IDs;
   chain-derived identity includes network, contract and token.
2. Model memberships, friendships, favorites and permissions as keyed
   relations, with explicit acceptance/blocking and local-versus-mirrored
   semantics. No universal entity/attribute table; no JSON document holding
   unrelated, independently updated facts.
3. Keep opaque protocol metadata and ordered auth chains as documents when read
   as a unit; use typed columns or relations for fields used to join, filter or
   update independently. Do not split columns mechanically. Original
   hash-addressed bytes stay immutable in the blob store; never reserialize
   JSON to reconstruct signed or content-addressed input.
4. Start with `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, row-local `CHECK` and
   suitable foreign keys. For every omitted relationship constraint, record its
   invariant, enforcing write path and deletion behavior. Use a cross-schema FK
   only with a shared restore/deletion lifecycle; otherwise keep FKs inside the
   owning schema and define reconciliation. `CHECK` is not a cross-table
   invariant mechanism.
5. Label projections. `active_pointers` and `deleter_deployment` derive from
   deployment ordering and must change atomically with accepted deployments; a
   rebuild must reproduce tie-breaking and deletion semantics. Peer cursors,
   failed retries and `local_entities` provenance are durable: a cache rebuild
   must not discard them or infer local authority from peer data.
6. Keep frequently scanned keys and state small; fetch auth chains, metadata
   and blobs only where needed. Split out a payload relation only after
   measuring row width, updates and joins (TOAST already stores large values
   out of line).
7. Land a DDL change twice once a database is stamped: in `schemas/*.sql` for
   new databases and, when it touches an existing table, as an additive step in
   `schemas/migrations/` (`catalyst schema migrate` applies each once and
   records it in `meta.schema_steps`; `services/catalyst/tests/`). A step
   that drops, renames, retypes or deletes is refused: that change is its own
   reviewed release (record the data-safe release procedure and evidence in the task report).

## Worlds, persistent storage and scene hosting

One worlds subsystem owns deployment/permission lookup, world/player storage
and authoritative scene hosting, on the shared pool with explicit
realm/scene/namespace and player scope. Keep the storage namespace distinct
from a deployment's content hash. Each world scene has a stable identity
(`explore.world_scene_place`) following Places' overlap rule: overlapping
exactly one active scene retains it, including a moved base; overlapping none
or several creates a new identity. Retired identities keep their storage rows.
Genesis storage management requires LAND rights at the requested parcel, using
the published LAND manager index or its configured document source. Without a
document source, only ownership at that parcel grants authority; scene-admin
roles do not grant storage authority.

Storage collaborators are wallets in an active local world deployment's or
active Genesis deployment's `metadata.logsPermissions`; Genesis deployments
accepted through peer sync are eligible too. Per-key and list `/values*` and `/players*`
operations probe the resolved deployment by primary/unique key on every
request; redeployment without the wallet revokes that grant. Collaborator
access excludes clears, `/env*` and `/usage*`.
`GET /world-storage/collaborator` is a rare, user-initiated, self-scoped listing.
It derives membership at read time through `worlds.logs_permissions`, with no
stored projection. The partial GIN `world_scenes_logs_permissions_idx` covers
local world scenes; `deployments_scene_logs_permissions_idx` covers live
Genesis scenes, followed by an `active_pointers_entity_id_idx` probe per match.
Superseded Genesis rows leave the index. One statement counts and pages the
matches by `deployed_at DESC, scene_id` with `LIMIT/OFFSET`.
The function reads raw JSON text so even a stored JSON `\u0000` cannot fail an
insert through an index. Expected traffic is low; measured dataset sizes,
query timings and index write costs must be recorded in the task report.

Genesis collaborator grants also require the configured LAND index to be
complete and fresh. One shared SQL predicate checks the entire scene footprint
for both request authorization and listing, before counting and pagination.
For each parcel, indexed token-history probes select the latest LAND transfer,
estate membership change, and current estate's transfer by block/log position.
Missing ownership or required event timestamps fail closed. Approval and
operator changes do not change this ownership boundary. Transfer away and back
still revokes the old deployment's grant.

The predicate uses the fresh published fold while unrelated events are being
ingested. A relevant LAND transfer, estate membership event or current estate
transfer beyond that published block denies the affected footprint until
publication; approval churn and unrelated parcels do not cause a global
collaborator outage. Publication timestamps and ordinary LAND readiness use
the database clock; the status response observes the cursor and that clock
together. Both
request and listing SQL gate the history function behind a `CASE` membership
check, so a wallet absent from `logsPermissions` does not pay for that probe.

Timestamp repair runs after live ingest and ownership publication, consuming
only the remaining request budget within a 30-second deadline and applying
multiple batches of up to 100 distinct blocks. Cancellation or the deadline
retains committed repair progress. Rejected or partial batches fall back to
counted single-header calls, with periodic batch probes so a transient fault
does not disable batching until restart. An unresolved or hash-mismatched
block stops the live cursor at the preceding verified prefix. The repair due-set and updates are scoped to
the configured LAND/Estate registries. Unknown legacy timestamps keep the
collaborator readiness gate false until repair completes; ordinary ownership
publication remains independent. Verify upgrade lock behavior and backlog estimates before claiming readiness.

The immutable signed entity timestamp minus the protocol's 15-minute maximum
future skew must be strictly later than every relevant ownership event. No
request-time baseline, receipt timestamp, metadata refresh or peer replay
renews the grant. This conservative rule assumes source deployment validation
and ordinary agreement between source-node and chain clocks. A deployment
timestamped within 15 minutes after an ownership change cannot restore the
grant: another deployment with a later signed timestamp is required. Waiting
alone does not restore that old entity.
Unavailable LAND history affects this collaborator grant only; independent
owner, player and scene-server grants retain their own authorization rules.

Genesis scenes need a valid scope too. Use one permission core with explicit
owner/player/host operations. Guests get scoped storage capabilities, never SQL
access or other scenes' environment values. Mirroring cannot overwrite local
saves or grant host authority. Keep live CRDT/tick state in the scene runtime;
persist only what the restart contract requires. Test cross-scene/player
isolation, redeploy, revocation and restart.

Places owns one active identity row per world scene, explicitly bound to its
current `(world_name, entity_id)` deployment. Worlds calls the Places identity
writer in the deployment transaction under the world row lock; storage and
scene discovery resolve that same binding. Catalogue refreshes never mint or
retire identities. Inactive rows retain the UUID but clear the deployment link,
so retained storage does not become reachable from a replacement scene. No
foreign key ties retained identities to replaceable deployment rows; the
transactional writer and partial unique deployment index enforce the binding.
The unique partial `(world_name, entity_id) WHERE active` index enforces one
active place per deployment and serves exact deployment lookups. Its leading
`world_name` column bounds overlap probes and retirement sweeps to the active
scenes in that world (expected up to a few hundred).
World quotas and usage exclude recorded retired identities, and exclude the
parked legacy `world` bucket once that world has an active identity; unknown
other legacy scopes still count. Storage rows remain retained and inaccessible
through new scene scopes. Verify migration and recovery limits and measure quota-query costs before acceptance.

The world discovery listing remains one aggregate per world. Scene listings and
place detail read the shared identities joined to active deployments; neither
adds scene rows to the aggregate catalogue. Upstream mirrored catalogue IDs
remain external IDs, never storage authority. The old
`worlds.world_scene_identity` table is a frozen upgrade source only: creating
the new Places relation copies its UUIDs and retained records during object
creation. This is once per creation, not once for the lifetime of a database:
never drop/recreate the authoritative relation to recover it, because that
would replay the stale legacy snapshot. Existing storage keys are not rewritten,
and migration `0010` stays byte-for-byte stable.

**SQL-level dependency:** Places discovery joins `worlds.world_scenes` and
`worlds.worlds`; Places identity retirement probes `worlds.world_scenes` for
the exact current deployment. Worlds owns those source rows and invokes the
Places identity writer within the same transaction. These cross-schema reads
are part of the shared database contract, independent of Rust crate imports.
The Rust module-DAG check does not inspect SQL and cannot prove these data
dependencies absent. Keep column/visibility and lifecycle changes coordinated
across both domains; no reverse Rust crate dependency or internal HTTP call is
introduced.

Preserve the reviewed method, path, scope and query authorization contract for scene-host storage requests. Handle security acceptance through the private review workflow.

Storage value reads use PostgreSQL by default on every process. The optional
`STORAGE_CACHE_ENABLED=true` memo is only suitable for one storage-serving
process: invalidation is local, and even a primary can return stale values or
environment secrets after a replica writes. `FLEET_JOBS_ENABLED` does not make
that cache coherent. Explicit caching accepts a freshness window of
`STORAGE_CACHE_TTL_SECONDS` and is unsuitable for immediate cross-process revocation.
`STORAGE_CACHE_ENABLED` accepts only `true` or `false`; numeric values such as
`1` and `0` are rejected at startup.

Permission-source outages deliberately return 503 instead of upstream's 401,
without granting access. Transient permission/RPC outages and export database
setup failures (connection acquisition, snapshot preparation or their deadlines)
carry `Retry-After: 1`. Export setup failures occur before acceptance, so they do
not consume the scene cooldown. The header is a minimum retry delay, not a promise
that capacity has recovered; clients should use backoff for repeated failures.
Export concurrency caps continue to bound pending attempts. Invalid export
timeout configuration is rejected at startup; a defensive out-of-range deadline
failure carries no retry hint. Likewise,
missing credentials (including `ENCRYPTION_KEY`) and unsupported EIP-1654
validation require configuration and carry no timed retry instruction. This is
an intentional wire divergence, not a claim of identical error responses.

A storage export (`POST /world-storage/exports`) holds one pooled connection
while reading and serializing world rows and then player rows, never env. The
read-only transaction ends before the final gzip trailer is sent; cancellation,
the export deadline and the raw-size cap bound interrupted producers. `EXPORT_MAX_CONCURRENT_PER_POD` (5) bounds them per
`serve` process and must stay well under `PG_POOL_SIZE` (60): at most a
twelfth of the pool by default, so the request handlers keep the rest. The
caps are per process, as upstream's are per pod.

`export::produce` is rare and user-initiated. Its two row streams use the
namespace primary-key prefix in key order: `world_storage`, then
`player_storage` by `player_address, key`, in one read-only repeatable-read
transaction. A connection is acquired before response headers (unavailable
capacity returns 503), and the producer owns both it and its concurrency slot.
One deadline bounds acquisition, SQL and channel backpressure; dropping the
body cancels the producer. Completion uses a separate channel, so a failure or
panic cannot masquerade as successful HTTP EOF. Transaction-local statement
and idle-in-transaction timeouts match the export budget and reset before pool
reuse. The configured timeout is 1–3600 seconds, defaulting to 120 seconds;
deadline construction also rejects an unrepresentable duration. Scene cooldown
begins only after database setup succeeds, with an atomic admission recheck:
a pre-response 503 releases its reservation without consuming the cooldown.
When cooldown is enabled, only one pending admission per scene can prepare a
database transaction, even if the configured per-scene concurrency cap is higher.
Disabling cooldown preserves concurrent admission up to the configured caps.
Optional selections are
`key = ANY` / `player_address = ANY` filters on that prefix. Not measured:
a scene at its quota (10 MB world plus 1 MB per player).

Scene-host storage requests sign the pathname, allowing `Storage.getValues`
listing queries. Verification still binds the method and declared storage mount;
the unsigned-query trade-off requires security acceptance.

Comms (`services/catalyst/comms`) uses the worlds/content identities and world
permission decisions through shared queries and local calls. World permissions
keep one authoritative representation; social keeps message/call membership
and privacy; comms enforces them and owns its connection/assignment lifecycle.
Persist ACLs, bans and assignment/fencing state needed for recovery under
their owning domains; live connections, presence and media follow the
transport's lifecycle. Presence is not proof of permission. Resolve room
identity from the same active deployment for both clients and authoritative
hosts. Permission revocation must also take effect in the running room, not
merely change a row. Preserve host identity and publish/subscribe grant
distinctions. Test shared-room joins, denied joins, revocation and reconnect
against the real transport.

World allow-lists admit explicit access wallets and members of listed active,
unsuspended communities who are not banned. Admission probes
`worlds.world_access_communities` with `social.community_members` and
`social.community_bans`; their primary keys cover the world/member lookups.
`world_access_communities_community_idx` finds affected worlds by community
for leave, kick, ban, suspension, deletion and mirror-deactivation cleanup.
Expected scale is thousands of grants and tens of room joins per second;
these are estimates, not measured throughput.

## Queries, ingest and derived state

For Shop: keep explicit asset/chain/order identities, separate accounting
facts from rebuildable projections, bound reconciliation, use exact currency
units, reserve atomically with external work outside the transaction. The
MANA-only LAND/resale rule holds through authorization and settlement; a
stored client-supplied sale category never establishes eligibility.

- Share the pool and use qualified SQL joins. Return the fields the consumer
  needs; avoid per-result round trips. Preserve API ordering, collation,
  pagination and empty/error distinctions when moving Rust work into SQL.
- Keep the bounded `UNNEST` ingest and deterministic pointer locks. Bound bytes
  and transaction duration as well as entity count; measure pool and lock
  waits under reads. Advance durable progress only after data or its retry
  record is durable; keep duplicate/out-of-order/resume tests.
- Poll indexed, bounded due sets for retries, mirrors and notification
  delivery. Keep exhausted-row cleanup and no-auth behavior explicit. With
  multiple workers, use a short atomic claim with lease/recovery semantics; do
  network work outside the transaction; preserve idempotency on redelivery.
- Give every cached or materialized result a source, freshness budget,
  invalidation and rebuild path. Notifications may wake a reader; durable
  delivery needs a transactional record and a resumable consumer. Keep
  triggers small and count their cost in ingest measurements.
- Each index names its query or integrity constraint in the SQL comment beside
  it. Match filter, sort, expression and keyset tie-breaker; check the actual
  prepared plan. Measure storage/WAL/update cost before adding or removing an
  index; a tiny lab run with zero scans is not evidence to remove one.
  Partial-index predicates match the query; current time belongs in the
  due-work query, never in a moving `now()` index predicate.
- Prefer stable keyset pagination for new feeds and mirror scans; keep
  required offset APIs. Define tie-breaking, replay windows and deletion
  handling; a cursor alone does not give snapshot consistency.

## Measurement and acceptance

Use read-only PostgreSQL catalogue and statistics queries through psql for a
baseline (for example `pg_stat_user_tables` and `pg_stat_user_indexes`). Capture
a before/after pair over the same workload window; counters are cumulative and
row counts are estimates. For a
query/layout change record revision, PostgreSQL version, hardware, dataset
size/distribution, concurrency, cache state, query/parameters, repeated
p50/p95/p99, throughput and resource cost. Use `EXPLAIN (ANALYZE, BUFFERS,
WAL, SETTINGS)`; `ANALYZE` executes, so measure writes on a disposable
database. Buffer hits are not CPU cache hits. Before accepting a slice, prove
wire, authorization, concurrency and recovery behavior and state the
improvement and trade-off; smoke-scale bootstrap runs are not benchmarks.
Record missing measurements explicitly.

## Bootstrap, schema versions and local writes

`catalyst schema apply` bootstraps `catalyst` from the binary's rendering of
`schemas/`; it never replays third-party schema histories or dumps. Startup
verifies schema identity and version and fails clearly on a mismatch; it never
resets persistent data. Drop and re-apply disposable development databases. A
database holding local authority changes only through a data-safe release
(record the data-safe release procedure and evidence in the task report): additive steps under
`schemas/migrations/` applied once by `catalyst schema migrate`; anything that
drops, renames or retypes is its own reviewed release under `schemas/releases/`.

The Shop schema requires schema version 2. For a database stamped version 1,
stop the node and verify a backup restore, then apply the reviewed release
(it keeps column, index and row shapes and moves Shop relations to `shop`):

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f schemas/releases/v2-shop-schema.sql
catalyst schema migrate
```

`db::bootstrap` refuses version 1, and `migrate` alone would create an empty
`shop` beside `marketplace`. A version change is never rollback-compatible
(`services/catalyst/tests/schema_upgrade_from_closure_base.rs` exercises the upgrade; deployment restore and rollback evidence remains a separate acceptance gate).

Peer sync is not a backup of local state. Keep local write paths disabled until a backup restore of the node's database, blobs and credentials has been demonstrated. Record the restore and rollback evidence in the task report before enabling writes. Development and commit builds sharing a database must agree on its schema; do not reset or silently migrate shared state to fit a working tree.
