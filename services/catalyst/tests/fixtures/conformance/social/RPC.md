# social-service-ea RPC behaviour: upstream specs and Catalyst tests

Reference: `decentraland/social-service-ea` at `fb3dcb80f8cac53485696a5f7c94b720b20ae6c9`. Local:
`services/catalyst/social/src/rpc` (the `/social-rpc` socket), `services/catalyst/shared/src/rpc` (handshake,
transport, dcl-rpc) and the voice runtime in `services/catalyst/comms/src/gatekeeper/voice`.

The handshake's header semantics come from `@dcl/crypto-middleware` 6.3.0, the version the reference's `yarn.lock`
resolves for its direct dependency (`yarn.lock:1020-1023`; not the transitive 4.1.1). Source: the published
tarball `https://registry.yarnpkg.com/@dcl/crypto-middleware/-/crypto-middleware-6.3.0.tgz`, fetched read-only on
2026-10-28, sha512 `HUwaPBuXjf3xSGIcnwm52NIZchaPg0rDe/bsjlYdo4ZpjuTlTeQ8m7fjTT3V14/pRTa55T404eHGZURU5jJDog==`
(equal to the `yarn.lock` integrity) and sha1 `855174cab339df1d62e288d2b533fdef6f3ea4d3` (the resolved URL's
fragment), extracted with `tar` and never run. Citations below are lines of its `package/dist/verify.js`. The
same functions read the same in `decentraland/core-libs` at tag `@dcl/crypto-middleware@6.3.0`
(`libs/crypto-middleware/src/verify.ts`).

Scope: the upstream unit specs of the RPC surface at the pin, 50 files and 811 cases. A case is a line that calls
`it(` or `test(`; the 10 `it.each`/`test.each` tables (2 in `friendships`, 6 in `settings.utils`, 2 in
`notifications`) do not match that and are not in the count, though their behaviour is mapped below. This prints
`50 files, 811 cases` from a checkout of the reference repository:

```sh
git ls-tree -r --name-only fb3dcb8 test/unit \
  | grep -E '^test/unit/(controllers/handlers/ws/ws-handler|adapters/rpc-server|adapters/rpc-server/services/[^/]+|logic/(blocks|friends-logic|friends|friendships|settings/settings|settings/settings\.utils|voice-logic|community-voice|community-voice-cache|community-voice-polling|updates|ws-pool-component|peers-stats|profiles|notifications))\.spec\.ts$' \
  | while read -r f; do git show "fb3dcb8:$f" | grep -cE '\b(it|test)\('; done \
  | awk '{ n++; s += $1 } END { print n " files, " s " cases" }'
```

The files, with their case counts:

| spec file at `fb3dcb8` | cases |
| --- | --- |
| `test/unit/controllers/handlers/ws/ws-handler.spec.ts` | 43 |
| `test/unit/adapters/rpc-server.spec.ts` | 15 |
| `test/unit/adapters/rpc-server/services/accept-private-voice-chat.spec.ts` | 4 |
| `test/unit/adapters/rpc-server/services/block-user.spec.ts` | 4 |
| `test/unit/adapters/rpc-server/services/demote-speaker-in-community-voice-chat.spec.ts` | 37 |
| `test/unit/adapters/rpc-server/services/end-community-voice-chat.spec.ts` | 7 |
| `test/unit/adapters/rpc-server/services/end-private-voice-chat.spec.ts` | 3 |
| `test/unit/adapters/rpc-server/services/get-blocked-users.spec.ts` | 3 |
| `test/unit/adapters/rpc-server/services/get-blocking-status.spec.ts` | 5 |
| `test/unit/adapters/rpc-server/services/get-friends.spec.ts` | 3 |
| `test/unit/adapters/rpc-server/services/get-friendship-status.spec.ts` | 8 |
| `test/unit/adapters/rpc-server/services/get-incoming-private-voice-chat-requests.spec.ts` | 3 |
| `test/unit/adapters/rpc-server/services/get-mutual-friends.spec.ts` | 5 |
| `test/unit/adapters/rpc-server/services/get-pending-friendship-requests.spec.ts` | 5 |
| `test/unit/adapters/rpc-server/services/get-private-messages-settings.spec.ts` | 5 |
| `test/unit/adapters/rpc-server/services/get-sent-friendship-requests.spec.ts` | 5 |
| `test/unit/adapters/rpc-server/services/get-social-settings.spec.ts` | 3 |
| `test/unit/adapters/rpc-server/services/join-community-voice-chat.spec.ts` | 6 |
| `test/unit/adapters/rpc-server/services/kick-player-from-community-voice-chat.spec.ts` | 28 |
| `test/unit/adapters/rpc-server/services/mute-speaker-from-community-voice-chat.spec.ts` | 9 |
| `test/unit/adapters/rpc-server/services/promote-speaker-in-community-voice-chat.spec.ts` | 38 |
| `test/unit/adapters/rpc-server/services/reject-private-voice-chat.spec.ts` | 3 |
| `test/unit/adapters/rpc-server/services/reject-speak-request-in-community-voice-chat.spec.ts` | 33 |
| `test/unit/adapters/rpc-server/services/request-to-speak-in-community-voice-chat.spec.ts` | 11 |
| `test/unit/adapters/rpc-server/services/start-community-voice-chat.spec.ts` | 5 |
| `test/unit/adapters/rpc-server/services/start-private-voice-chat.spec.ts` | 7 |
| `test/unit/adapters/rpc-server/services/subscribe-to-block-updates.spec.ts` | 6 |
| `test/unit/adapters/rpc-server/services/subscribe-to-community-member-connectivity-updates.spec.ts` | 7 |
| `test/unit/adapters/rpc-server/services/subscribe-to-community-voice-chat-updates.spec.ts` | 11 |
| `test/unit/adapters/rpc-server/services/subscribe-to-friend-connectivity-updates.spec.ts` | 11 |
| `test/unit/adapters/rpc-server/services/subscribe-to-friendship-updates.spec.ts` | 6 |
| `test/unit/adapters/rpc-server/services/subscribe-to-private-voice-chat-updates.spec.ts` | 8 |
| `test/unit/adapters/rpc-server/services/unblock-user.spec.ts` | 5 |
| `test/unit/adapters/rpc-server/services/upsert-friendship.spec.ts` | 7 |
| `test/unit/adapters/rpc-server/services/upsert-social-settings.spec.ts` | 15 |
| `test/unit/logic/blocks.spec.ts` | 3 |
| `test/unit/logic/community-voice-cache.spec.ts` | 18 |
| `test/unit/logic/community-voice-polling.spec.ts` | 8 |
| `test/unit/logic/community-voice.spec.ts` | 88 |
| `test/unit/logic/friends-logic.spec.ts` | 83 |
| `test/unit/logic/friends.spec.ts` | 2 |
| `test/unit/logic/friendships.spec.ts` | 39 |
| `test/unit/logic/notifications.spec.ts` | 6 |
| `test/unit/logic/peers-stats.spec.ts` | 8 |
| `test/unit/logic/profiles.spec.ts` | 16 |
| `test/unit/logic/settings/settings.spec.ts` | 4 |
| `test/unit/logic/settings/settings.utils.spec.ts` | 10 |
| `test/unit/logic/updates.spec.ts` | 94 |
| `test/unit/logic/voice-logic.spec.ts` | 42 |
| `test/unit/logic/ws-pool-component.spec.ts` | 16 |
| total: 50 files | 811 |

Selection rule for `test/unit/logic/` (34 specs at the pin): the specs of the logic the RPC procedures and the
socket run on (friends, blocks, settings, voice, the update fan-out, the connection pool, the online peers, profile
parsing and the friendship notifications), 15 of them. The other 19 are not RPC behaviour:
- the communities HTTP API on the social-api host (`ENDPOINTS.md`): `communities`, `community-bans`,
  `community-compliance-validator`, `community-fields-validator`, `community-image-signature`, `community-members`,
  `community-owners`, `community-places`, `community-posts`, `community-requests`, `community-roles` (the voice
  gates read roles from the database in `src/logic/community-voice/validation.ts`, not through it),
  `community-thumbnails` and `community/utils`;
- SQS consumers and scheduled jobs, not the socket: `community-broadcaster`, `community-events`, `community-ranking`;
- `referral` (HTTP and SQS; an intentional gap in `docs/compatibility.md`) and `user-mutes` (HTTP routes);
- `queries`: it asserts the text of generated SQL, with no observable counterpart.

There is no `test/unit/controllers/handlers/rpc/` at this revision. The cases are grouped below by the behaviour a
client observes.

Status:
- **covered**: a local test asserts it.
- **ported**: a test added for this table asserts it (`rpc::tests::<file>::<test>`).
- **differs**: Catalyst behaves otherwise, including where it has no counterpart. Each names the upstream lines
  (paths in the reference repository, under `src/`) and the local lines (paths under `services/catalyst/`).
- **n/a**: the case checks an implementation detail with no observable counterpart (mock call counts, logging,
  Sentry, metrics, Redis or wiring).

Every difference is open: none is changed here. `docs/compatibility.md` summarises them.

Test files, relative to `services/catalyst/social/src/rpc/` unless a crate is named:
- `tests.rs` (T);
- `tests/socket.rs` (S), `tests/friends.rs` (F), `tests/settings.rs` (ST), `tests/private_voice.rs` (PV),
  `tests/community_voice.rs` (CV), `tests/failures.rs` (FA), `tests/voice_runtime.rs` (VR),
  `tests/connectivity.rs` (CN), `tests/upstream_reads.rs` (UR) and `tests/voice_bugs.rs` (VB, the accepted call
  through the real voice runtime);
- `service/domain.rs`, `service/helpers.rs`, `service/subscriptions.rs`, `limits.rs`, `profiles.rs`, `pubsub.rs`,
  `context.rs` (their `tests` modules);
- `shared/src/rpc/handshake.rs` (HS);
- `services/catalyst/social/src/communities/tests.rs` (CT).

The database tests skip without `CATALYST_TEST_DATABASE_URL`.

Differences that apply throughout:
- **Error messages.** An `internalServerError` carries `"internal error"` (local `social/src/rpc/service/helpers.rs:149-154`)
  where upstream carries the error's message (upstream `src/controllers/handlers/rpc/upsert-friendship.ts:69-71`).
  The pending and sent request lists send an empty one on both sides (upstream
  `src/controllers/handlers/rpc/get-pending-friendship-requests.ts:45-46`; local `social/src/rpc/service/domain.rs:245`).
  A database or voice-runtime error raised with `?` (the private and community voice procedures, for example local
  `social/src/rpc/service/private_voice.rs:49`) is a dcl-rpc remote error, code 500, whose message is exactly
  `Internal Server Error` for every procedure; the detail is logged, never sent (local
  `social/src/rpc/service/helpers.rs:33`, `:44-52`; FA `an_internal_refusal_carries_no_database_text`). Upstream
  answers the oneof's `internalServerError` there with the error's message (upstream
  `src/controllers/handlers/rpc/end-private-voice-chat.ts:42-46`): the carrier differs, an open difference, since
  moving to the oneof would change the wire.
- **Profiles.** No procedure answers `profileNotFound` on either side at this revision. Upstream drops a listed
  request whose counterparty has no parseable profile (upstream `src/logic/friends/friendships.ts:254-258`) and
  answers an upsert `internalServerError` "Profile not found for address …" after the write and the update
  (upstream `src/logic/friends/component.ts:354-360`); block and unblock answer an address-only profile on both
  sides. Catalyst answers a placeholder profile everywhere (local `social/src/rpc/profiles.rs:254-269`) and the
  upsert `accepted` (local `social/src/rpc/service/friends.rs:282-291`).
- **Stream closed.** Upstream ends a second subscription of one event on one connection with a duplicate-subscription
  notice (upstream `src/logic/updates.ts:463`, `:476-477`). The generated proto has no `stream_closed` field (local
  `social/src/rpc/proto/generated/decentraland.social_service.v2.rs:188`) and a second subscription opens another
  stream (local `social/src/rpc/service/helpers.rs:201-228`).

Production capture: the eight read procedures against `wss://rpc-social-service-ea.decentraland.org`
(2026-10-28 09:53 UTC, `social-rpc/_socket/reads.json`, 10 calls of a wallet created for the run, the status reads
on the zero address and a second address created for the run). The local answers are byte for byte production's
(UR `the_reads_answer_as_production_does`); the capture itself is the ignored `capture` test there, run only with
`CATALYST_SOCIAL_RPC_CAPTURE=1`.

## ws-handler.spec.ts (43)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| A valid chain authenticates and SERVER_READY follows | successful authentication | T `ws_handshake_valid_chain_receives_server_ready`; HS `verify_handshake_accepts_valid_chain` | covered |
| Any verification failure closes 3003 "Unauthorized" | authentication failure; middleware rejects the credentials | T `ws_handshake_garbage_or_scene_signed_frame_gets_close_3003`; S `an_expired_chain_closes_unauthorized` | covered, ported |
| A malformed first frame (`""`, not JSON, `null`, number, string, `[]`, numeric timestamp, object chain link) closes 3003 without verification | the `describe.each` of 8 frames | S `a_malformed_first_frame_closes_unauthorized`; HS `verify_handshake_rejects_malformed_envelope` | ported |
| A non-string field outside `x-identity-*` is left to verification | leave it to the middleware | S `a_field_outside_the_identity_headers_does_not_stop_the_handshake` | ported |
| An `x-identity-*` field (any case) that is neither a string nor an array of strings is refused before verification, metadata included (#501); an array reads as its first element, an empty one as absent (`firstOf`, `dist/verify.js:21-23`) | header values must be strings | HS `an_identity_field_that_is_not_a_string_is_refused_up_front`, `an_array_of_strings_reads_as_its_first_element`; S `a_malformed_first_frame_closes_unauthorized`, `an_array_of_strings_authenticates` | ported |
| A chain link present at index 10 is refused as too long, whatever its value | (`verify()` tests the raw value: an empty array is truthy) | — | differs: an empty array there reads as absent and is ignored. `@dcl/crypto-middleware` `dist/verify.js:87-89`; local `shared/src/rpc/handshake.rs:113` |
| The identity headers are read under their exact lowercase names; a re-cased duplicate is type-checked, then never read | (`verify()` reads `headers['x-identity-timestamp']`, `dist/verify.js:72`, `:321-322`) | HS `a_re_cased_identity_header_is_never_read`; S `a_re_cased_identity_header_is_never_read_on_the_socket` | covered |
| An absent `x-identity-metadata` reads as `{}` and the payload ends `:` | (`createPayload` joins `undefined` as empty) | — | differs: Catalyst signs over `{}` when it is absent. `@dcl/crypto-middleware` `dist/verify.js:173`, `:218`; local `shared/src/rpc/handshake.rs:224` |
| The scene signer is refused (re-cased value, re-spelled key); `{}`, `dcl:explorer` and absent metadata pass | the 7 metadata cases | HS `scene_signer_gate_matches_upstream_reject_if_signer`, `a_scene_signed_frame_is_refused_before_the_signature_is_checked` | covered |
| No auth frame within the window closes the socket | timeout for non-authenticated connections | T `ws_handshake_timeout_closes_connection` | covered; differs: 3003 "Unauthorized" where upstream sends 3004 "Authorization timeout" (no upstream spec pins the code). Upstream `src/controllers/handlers/uws/ws-handler.ts:371-376`; local `social/src/rpc/ws.rs:100-102`, `:70-75` |
| A successful auth clears the timeout | clear timeout when authenticated | S `an_authenticated_socket_outlives_the_handshake_timeout` | ported |
| A frame sent while auth is in progress gets `{"error":"Authentication already in progress, please try again later"}` | reject messages during authentication | — | differs: no answer; the handshake reads one frame, and a second one waits, then reaches the RPC server (binary) or is dropped (text). Upstream `src/controllers/handlers/uws/ws-handler.ts:414-421`; local `social/src/rpc/ws.rs:100`, `shared/src/rpc/ws.rs:52-57` |
| The catalyst being unreachable during verification closes 3003 | (the 503 describe) | S `an_unreachable_contract_validator_closes_unauthorized` | ported |
| Closing an authenticated socket detaches the user and frees its slot | cleanup authenticated connection | S `closing_an_authenticated_socket_releases_its_connection`; `context.rs` `presence_counts_sockets_per_address` | ported |
| The RPC layer closing the transport ends the socket with 1011 "RPC transport closed" | detach the user and end the socket | — | differs: the close frame carries no code. Upstream `src/controllers/handlers/uws/ws-handler.ts:210-229`; local `shared/src/rpc/ws.rs:140-142`, `:202` |
| A socket closed during verification is never attached | abort user attachment (3 cases) | — | differs: the user is attached and then torn down. Upstream `src/controllers/handlers/uws/ws-handler.ts:175-181`, `:192-199`; local `social/src/rpc/ws.rs:87-90`, `social/src/rpc/mod.rs:218-238` |
| After auth, binary frames reach the RPC server | process the message | T `two_explorers_exchange_a_friend_request_over_the_socket` | covered |
| Upgrade state, pool registration, disconnected flags, drain, emitter throws, send failures, Sentry and metrics | 19 cases | — | n/a |

## rpc-server.spec.ts (15)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| Procedures run as the wallet the socket authenticated | attach a user | `context.rs` `presence_counts_sockets_per_address`; T `two_explorers_exchange_a_friend_request_over_the_socket` | covered |
| Two sockets of one address share its events; an unknown detach is harmless | shared subscriber; detach a non-existent user | `pubsub.rs` `publish_is_shared_by_every_receiver`; `context.rs` `presence_counts_sockets_per_address` | covered |
| Different addresses have separate channels | separate subscribers | `pubsub.rs` `each_address_has_its_own_channel` | ported |
| A detached address's channel goes | remove subscriber on detach (2) | `pubsub.rs` `a_channel_without_subscribers_is_forgotten_on_the_next_publish` | covered |
| The private call ends when the address's last socket detaches, not before | 3 cases | `context.rs` `presence_counts_sockets_per_address` (the predicate); PV `a_caller_that_disconnects_ends_the_call_for_the_callee` (the end) | covered, ported |
| Start without creators, service registration, Redis channels, emitter internals | 6 cases | — | n/a |

## Friendships

### logic/friendships.spec.ts (39) and logic/friends.spec.ts (2)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| The transition table; the acting-user rules | `isFriendshipActionValid`, `isUserActionValid`, `validateNewFriendshipAction` | `domain.rs` `full_transition_table_is_enforced`, `no_prior_action_only_admits_request_and_block`, `self_request_is_rejected_even_without_prior_action`, `acting_user_rules_still_apply_after_transition_check` | covered |
| Only accept activates the friendship | `getNewFriendshipStatus` | T `two_explorers_exchange_a_friend_request_over_the_socket`; F `cancel_reject_and_delete_reach_the_other_party` | covered, ported |
| The status of each last action and direction | `getFriendshipRequestStatus` (9) | `domain.rs` `action_mapping_covers_every_action_and_direction`, `block_fallback_only_when_no_friendship_action` | covered |
| Each emitted update names who acted | `parseEmittedUpdateToFriendshipUpdate` (7) | T `two_explorers_exchange_a_friend_request_over_the_socket`; F `cancel_reject_and_delete_reach_the_other_party`, `blocking_a_friend_ends_the_friendship_and_tells_them` | covered, ported |
| Connectivity updates parse ONLINE and OFFLINE | `parseEmittedUpdateToFriendConnectivityUpdate` | `subscriptions.rs` `replay_tests` | covered |
| A target is lowercased | `parseUpsertFriendshipRequest` | T `two_explorers_exchange_a_friend_request_over_the_socket` | covered |
| A request without a message stores none | `parseUpsertFriendshipRequest` | — | differs for an explicit `""` only: upstream stores no message, Catalyst stores and forwards `""` (an absent message is absent on both sides). Upstream `src/logic/friends/friendships.ts:128`; local `social/src/rpc/db.rs:558` |
| Profile mapping; the address is lowercased | `parseProfileToFriend`, `parseProfilesToFriends` | `profiles.rs` `profiles_come_from_the_active_profile_entity` | covered; differs: the picture is the node's face URL, and the caller's casing is kept. Upstream `src/logic/profiles.ts:35`, `:45-49`; local `social/src/rpc/profiles.rs:257`, `:159` |
| Requests whose counterparty has no (parseable) profile are dropped | 5 cases | `profiles.rs` `without_a_pool_every_address_is_a_placeholder_in_input_order` | differs: a placeholder profile. Upstream `src/logic/friends/friendships.ts:254-266`; local `social/src/rpc/service/domain.rs:257-260`, `social/src/rpc/profiles.rs:263-269` |

### logic/friends-logic.spec.ts (83)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| Friends with total and pagination | `getFriendsProfiles` (4) | T `apply_without_a_known_row_reuses_the_reversed_pair`; `helpers.rs` `friends_and_mutual_allow_up_to_1000_and_do_not_share_the_100_cap` | covered |
| Mutual friends | 6 cases | F `mutual_friends_answer_the_shared_friends_and_an_empty_page_for_a_bad_user` | ported |
| Pending and sent requests with totals | 6 cases | T `pending_requests_list_the_latest_request_with_its_message` | covered |
| The rate limits: pair budget symmetric, block budget directional | 6 cases | `limits.rs` `actor_budget_spans_targets`, `pair_budget_is_symmetric_for_friendship_actions`, `symmetric_and_directional_buckets_never_merge`, `block_budget_is_directional_so_the_counterparty_cannot_spend_it`; F `friendship_mutations_past_the_pair_budget_are_refused_without_a_write` | covered, ported |
| Blocked users: total is the full count | 3 cases | T `get_blocked_users_pages_and_counts_the_full_set` | covered |
| Friendship status: the latest action | 3 cases | T `friendship_probe_reports_last_action_and_both_block_directions`, `friendship_action_outranks_a_block_row` | covered |
| Upsert on a blocked pair writes nothing | 1 case | F `a_blocked_pair_cannot_change_its_friendship` | ported |
| Legal transitions write, publish and answer | 4×4 cases | T `two_explorers_exchange_a_friend_request_over_the_socket`; F `cancel_reject_and_delete_reach_the_other_party` | covered, ported |
| Illegal transitions write nothing | 7×2 cases | `domain.rs` (decision); F `an_illegal_transition_writes_nothing` | covered, ported |
| Blocking a friend ends the friendship and publishes both updates; unblocking records a delete | 8 cases | F `blocking_a_friend_ends_the_friendship_and_tells_them` | ported |
| Blocking a stranger publishes only the block update | 3 cases | — | differs: a `FriendshipUpdate::Block` is published too. Upstream `src/logic/friends/component.ts:133`, `:144`; local `social/src/rpc/service/blocks.rs:86-97` |
| Unblocking a stranger publishes only the block update | 3 cases | F `unblocking_a_stranger_only_lifts_the_block` | ported |
| A checksummed block target reaches the lowercase subscriber | 1 case | F `blocking_a_friend_ends_the_friendship_and_tells_them` | ported |
| A missing sender or receiver profile fails the upsert | 2 cases | — | differs: `accepted` with a placeholder, where upstream answers `internalServerError` after the write and the update. Upstream `src/logic/friends/component.ts:354-360`; local `social/src/rpc/service/friends.rs:265-274`, `:282-291` |
| A request or accept emits a friendship notification | 2 cases | — | differs: no notification is sent. Upstream `src/logic/friends/component.ts:370-381`, `src/logic/notifications.ts:78-79`; local `social/src/rpc/service/friends.rs:277-280` |
| A failed block write answers an error and tells no one | 1 case | FA `a_failed_database_call_answers_as_upstream_does` | ported |
| A block whose friendship write fails is rolled back with it | (one transaction) | — | differs: the friendship write is a separate statement; its failure is logged and the block stands. Upstream `src/logic/friends/component.ts:129-141`; local `social/src/rpc/service/blocks.rs:54-66` |
| Profile-registry counts, limiter backend, query-error propagation | 12 cases | — | n/a |

### rpc-server/services: friends

| spec | behaviour | local test | status |
| --- | --- | --- | --- |
| get-friends (3) | Profiles, total, page; a failure answers the empty page | T `two_explorers_exchange_a_friend_request_over_the_socket`; `helpers.rs` `page_of_matches_upstream_get_page`; FA `a_failed_database_call_answers_as_upstream_does`; UR | covered, ported |
| get-mutual-friends (5) | Shared friends; a missing or invalid user answers the empty page; a failure too | F `mutual_friends_answer_the_shared_friends_and_an_empty_page_for_a_bad_user`; FA | ported |
| get-pending-friendship-requests (5), get-sent-friendship-requests (5) | Requests with id, profile, date and message; default 100, cap 200; a failure answers `internalServerError` | T `pending_requests_list_the_latest_request_with_its_message`; `helpers.rs` `friendship_requests_default_100_max_200`, `page_of_matches_upstream_get_page`; FA; UR | covered, ported; differs: an absent message is omitted, not `""`. Upstream `src/logic/friends/friendships.ts:242`; local `social/src/rpc/service/domain.rs:262` |
| get-friendship-status (8) | The status; a missing user and an invalid one are named; a lone block | T `two_explorers_exchange_a_friend_request_over_the_socket`; F `friendship_status_names_a_missing_or_invalid_user_and_reads_a_lone_block`; UR | covered, ported; differs: an unknown stored action reads as none or blocked, not UNRECOGNIZED. Upstream `src/logic/friends/friendships.ts:230-231`; local `social/src/rpc/service/domain.rs:110`, `social/src/rpc/service/friends.rs:129-135` |
| upsert-friendship (7) | Accepted with the receiver's profile; illegal transitions; a blocked pair | T `two_explorers_exchange_a_friend_request_over_the_socket`; F `a_blocked_pair_cannot_change_its_friendship`, `an_illegal_transition_writes_nothing` | covered, ported |
| upsert-friendship | An invalid address answers `invalidRequest` | — | differs: no address check; the target is written. Upstream `src/controllers/handlers/rpc/upsert-friendship.ts:31-33`; local `social/src/rpc/service/friends.rs:194`, `:243-252` |
| upsert-friendship | A request to yourself answers `invalidFriendshipAction` "You cannot send a friendship request to yourself" | — | differs: `invalidRequest` "invalid target user", for every action and for a missing user. Upstream `src/controllers/handlers/rpc/upsert-friendship.ts:27-29`; local `social/src/rpc/service/friends.rs:194-199` |
| upsert-friendship | An unknown action answers `invalidRequest` "Unknown message" | — | differs: the message is "missing action". Upstream `src/controllers/handlers/rpc/upsert-friendship.ts:22-24`; local `social/src/rpc/service/friends.rs:185-189` |
| subscribe-to-friendship-updates (6) | The other party's updates with the sender's profile | T `two_explorers_exchange_a_friend_request_over_the_socket` | covered; stream errors and cleanup n/a; stream closed differs (throughout) |
| subscribe-to-friend-connectivity-updates (11) | Live updates; an initial snapshot of the online friends that a failure does not end | T `two_explorers_exchange_a_friend_request_over_the_socket`; `subscriptions.rs` `replay_tests` | covered; differs: the snapshot replays every friend with its status, and a failed or unacknowledged snapshot ends the stream. Upstream `src/controllers/handlers/rpc/subscribe-to-friend-connectivity-updates.ts:35-42`, `src/logic/updates.ts:519-523`; local `social/src/rpc/service/subscriptions.rs:44-84`; stream closed differs (throughout) |
| subscribe-to-community-member-connectivity-updates (7) | Member updates passed through, the subscriber's own included | CN `a_member_update_reaches_the_subscriber_only_when_current_and_co_member`; `subscriptions.rs` `replay_tests` | ported; differs: each update is re-checked at delivery (the member must be a co-member and the status current), and an initial replay of the co-members comes first. Upstream `src/controllers/handlers/rpc/subscribe-to-community-member-connectivity-updates.ts:33-46`; local `social/src/rpc/service/subscriptions.rs:177-227`, `:241`, `social/src/rpc/context.rs:249-283` |

## Blocks

### logic/blocks.spec.ts (3) and rpc-server/services: blocks

| spec | behaviour | local test | status |
| --- | --- | --- | --- |
| blocks (3) | Blocked profiles with `blockedAt`; the update names the blocker | `profiles.rs` `without_a_pool_every_address_is_a_placeholder_in_input_order`; F `blocking_a_friend_ends_the_friendship_and_tells_them` | covered, ported |
| block-user (4) | Ok with the profile and `blockedAt`; yourself is invalid | F `blocking_a_friend_ends_the_friendship_and_tells_them`, `blocking_yourself_is_an_invalid_request` | ported; differs: an invalid address is blocked, not refused; the self message is "cannot block yourself" (upstream "Cannot block yourself") and a missing user's "missing user". Upstream `src/controllers/handlers/rpc/block-user.ts:20-26`; local `social/src/rpc/service/blocks.rs:22-35`, `:52` |
| unblock-user (5) | Ok with the profile, no `blockedAt`; a failure answers `internalServerError` | F `blocking_a_friend_ends_the_friendship_and_tells_them`, `unblocking_a_stranger_only_lifts_the_block`; FA | ported; differs: no guard for yourself or an invalid address. Upstream `src/controllers/handlers/rpc/unblock-user.ts:20-27`; local `social/src/rpc/service/blocks.rs:119-128`, `:144` |
| get-blocked-users (3) | Profiles with `blockedAt`, the full total; a failure answers the empty page | T `get_blocked_users_pages_and_counts_the_full_set`; FA; UR | covered, ported |
| get-blocking-status (5) | Each direction alone, both, none; a failure answers both empty | T `block_and_unblock_return_the_latest_action_in_one_statement`; F `blocking_status_reads_each_direction_alone`; FA; UR | covered, ported |
| subscribe-to-block-updates (6) | The blocked party receives `{address: blocker, isBlocked}` | F `blocking_a_friend_ends_the_friendship_and_tells_them`, `unblocking_a_stranger_only_lifts_the_block` | ported (the published update); stream closed differs (throughout) |

## Settings

### logic/settings (14) and rpc-server/services: settings

| spec | behaviour | local test | status |
| --- | --- | --- | --- |
| settings (4) | A stored row, the defaults of an absent one, mixed, an empty input | T `private_messages_settings_keeps_input_order_and_schema_defaults`; ST `settings_read_their_defaults_and_round_trip_every_value` | covered, ported |
| settings.utils (10) | Every value maps both ways; a partial update writes only its fields | ST `settings_read_their_defaults_and_round_trip_every_value` | ported |
| settings.utils | An unknown value is refused | — | differs: prost reads it as the first (default) value and that is stored. Upstream `src/logic/settings/utils.ts:116-119`; local `social/src/rpc/service/settings.rs:43-51`, `social/src/rpc/service/domain.rs:142-148` |
| settings.utils | Private-message settings per address with friendship | ST `private_message_settings_read_per_address_with_friendship` | ported |
| get-social-settings (3) | Stored values; the defaults of an absent row, nothing inserted | ST `settings_read_their_defaults_and_round_trip_every_value`; UR | ported |
| get-private-messages-settings (5) | Privacy and friendship per address, in order; an empty list; more than 50 addresses | ST `private_message_settings_read_per_address_with_friendship`; UR | ported |
| upsert-social-settings (15) | One or several fields, the merged row back | ST `settings_read_their_defaults_and_round_trip_every_value` | ported |
| upsert-social-settings | An empty payload answers `invalidRequest` "At least one setting to update must be provided" | — | differs: no check; for a wallet without a row it creates one with the column default `only_friends`. Upstream `src/controllers/handlers/rpc/upsert-social-settings.ts:30-41`; local `social/src/rpc/service/settings.rs:37-72`, `social/src/rpc/db.rs:759`, `:33` |
| upsert-social-settings | An unknown privacy value answers `invalidRequest` | — | differs (as settings.utils). Upstream `src/controllers/handlers/rpc/upsert-social-settings.ts:83-91`; local `social/src/rpc/service/settings.rs:43-45` |
| upsert-social-settings | The gatekeeper's copy follows the privacy, fail-closed (10 cases) | — | n/a: one table; the private-messages token reads it when minted |

## Private voice

### logic/voice-logic.spec.ts (42) and rpc-server/services: private voice

| spec | behaviour | local test | status |
| --- | --- | --- | --- |
| voice-logic | Calling yourself is refused before any read, in any letter case | T `self_call_is_forbidden_before_any_read`; PV `a_start_follows_the_callers_privacy_and_refuses_yourself` | covered, ported |
| voice-logic | A blocked pair is refused and writes no call | T `blocked_pair_cannot_start_call_and_friends_can`; PV `a_start_follows_the_callers_privacy_and_refuses_yourself` | covered, ported |
| voice-logic | The callee's and the caller's `only_friends` refuse a stranger, admit a friend | T `blocked_pair_cannot_start_call_and_friends_can`; PV `a_start_follows_the_callers_privacy_and_refuses_yourself` | covered, ported |
| voice-logic | Either party busy is a conflict | T `blocked_pair_cannot_start_call_and_friends_can`, `live_call_still_blocks_symmetrically`, `expired_prior_call_does_not_block_new_one`; VB `an_old_accepted_call_keeps_its_parties_busy` | covered |
| voice-logic, start-private-voice-chat | Either party in a voice chat (private or community) is a conflict naming it | PV `either_party_already_in_a_voice_room_is_a_conflict_naming_it` | ported; differs: the community check comes after the self-call and the block, not before. Upstream `src/controllers/handlers/rpc/start-private-voice-chat.ts:32-48` before `src/logic/voice/voice.ts:36-45`; local `social/src/rpc/service/private_voice.rs:37`, `:50`, `:55` |
| voice-logic | The callee is rung with the call id | T `blocked_pair_cannot_start_call_and_friends_can` | covered |
| voice-logic | A competing reservation is a conflict without ringing | comms `voice::db` `pending_calls_serialize_shared_participants_and_return_deleted_rows`; VR `a_lost_reservation_is_busy_and_rings_no_one` | covered, ported |
| voice-logic | Accept: an unknown call is not found; only the callee accepts; each party gets its own grant | PV `an_unknown_call_is_not_found`, `each_party_gets_its_own_grant_and_the_callee_sees_the_incoming_call`; T `blocked_pair_cannot_start_call_and_friends_can` | covered, ported |
| voice-logic | Accept: a credentials failure is an error with no ACCEPTED | VR `a_failed_private_grant_is_an_internal_error_without_accepted` | ported |
| voice-logic | Accept deletes the pending call | VB `an_accepted_call_is_never_swept_as_expired`, `an_old_accepted_call_keeps_its_parties_busy`, `an_old_accepted_call_does_not_arm_the_sweep` | ported; differs: the call row is kept while the call lasts (its participant rows mark it accepted), so the incoming request still returns it; it never expires (the sweep, the start's reclaim and the sweep scheduler skip it) and keeps both parties busy whatever its age. Upstream `src/logic/voice/voice.ts:120-121`; local `comms/src/gatekeeper/voice/db.rs:381`, `:262`, `:278`, `:308`, `social/src/rpc/db.rs:219-220`, `:974-975` |
| voice-logic | Reject: an unknown call or a second reject is not found; the caller hears REJECTED | PV `an_unknown_call_is_not_found`; T `reject_and_disconnect_delete_through_voice_owner_before_signalling` | covered, ported |
| voice-logic | Reject by someone other than the callee is not found | PV `only_the_callee_rejects_and_only_a_party_ends` | ported; an accepted call is not found either, as upstream deletes the pending call on accept. Upstream `src/logic/voice/voice.ts:156-157`; local `social/src/rpc/service/private_voice.rs:246`, `comms/src/gatekeeper/voice/db.rs:332-336` |
| voice-logic | End: an unknown call is not found; the caller ends | PV `an_unknown_call_is_not_found`; T `blocked_pair_cannot_start_call_and_friends_can` | covered, ported |
| voice-logic | End by a non-party is not found; only the other party hears ENDED | PV `only_the_callee_rejects_and_only_a_party_ends`; VB `a_stranger_cannot_end_an_accepted_call` | ported; differs: both parties hear ENDED. Upstream `src/logic/voice/voice.ts:189-191`, `:203-204`; local `social/src/rpc/service/private_voice.rs:318`, `social/src/rpc/context.rs:491-497` |
| voice-logic | End deletes the LiveKit room | VB `ending_an_accepted_call_deletes_its_room`, `a_failed_room_delete_still_removes_the_call` | ported: an accepted call ends through the runtime's end path, which deletes the row while membership is still checkable, then the room. Upstream `src/logic/voice/voice.ts:217`; local `social/src/rpc/service/private_voice.rs:300`, `comms/src/gatekeeper/voice/mod.rs:285-303` |
| voice-logic | End: a runtime failure is an error | VB `a_failed_room_delete_still_removes_the_call`; FA `an_internal_refusal_carries_no_database_text` | differs: a failed room delete is logged and the answer is ok (the call row is already gone); a database failure is the remote error 500 (throughout). Upstream `src/logic/voice/voice.ts:217`, `src/controllers/handlers/rpc/end-private-voice-chat.ts:42-46`; local `comms/src/gatekeeper/voice/mod.rs:300-302`, `social/src/rpc/service/private_voice.rs:300` |
| voice-logic | Incoming: none is not found; a pending call names its caller | PV `each_party_gets_its_own_grant_and_the_callee_sees_the_incoming_call` | ported |
| voice-logic | Disconnect: the other party hears ENDED; no call does nothing | T `reject_and_disconnect_delete_through_voice_owner_before_signalling`; PV `a_caller_that_disconnects_ends_the_call_for_the_callee`; VB `ending_an_accepted_call_deletes_its_room` (the room) | covered, ported; differs: the party that dropped hears ENDED too. Upstream `src/logic/voice/voice.ts:263-269`; local `social/src/rpc/context.rs:446-473`, `:491-497` |
| voice-logic | Expiry pages through batches and tells both parties | T `sweep_reclaims_the_same_rows_the_busy_check_skips`, `sweep_tick_issues_no_delete_until_a_call_is_due`; PV `the_expiry_sweep_pages_through_and_tells_both_parties` | covered, ported |
| voice-logic | Gatekeeper fallbacks and delete races | 7 cases | n/a: one table, atomic deletes |
| start-private-voice-chat (7) | Ok, refused, busy, in a voice chat, unknown error; the messages | T `blocked_pair_cannot_start_call_and_friends_can`, `a_missing_voice_runtime_refuses_the_call_explicitly`; PV `a_refused_start_says_why`, `either_party_already_in_a_voice_room_is_a_conflict_naming_it`; VR `a_lost_reservation_is_busy_and_rings_no_one` | covered, ported; differs: a self-call and a blocked pair get their own messages, not the not-friends one. Upstream `src/logic/voice/voice.ts:36-45`, `src/logic/voice/errors.ts:1-5`; local `social/src/rpc/service/private_voice.rs:37-53` |
| accept-, reject-, end-private-voice-chat (10) | Ok with the call id; not found; refused | PV, T (above) | covered, ported; differs: the not-found messages are "call not found" and "no incoming call", not upstream's sentences naming the id or address. Upstream `src/logic/voice/errors.ts:19-29`; local `social/src/rpc/service/private_voice.rs:156`, `:355` |
| get-incoming-private-voice-chat-requests (3) | Ok, not found, internal | PV `each_party_gets_its_own_grant_and_the_callee_sees_the_incoming_call`; FA | ported |
| subscribe-to-private-voice-chat-updates (8) | Who receives each status | T `blocked_pair_cannot_start_call_and_friends_can`, `reject_and_disconnect_delete_through_voice_owner_before_signalling`; PV | covered, ported; differs: every update carries caller and callee, where upstream sets only the caller of REQUESTED. Upstream `src/controllers/handlers/rpc/subscribe-to-private-voice-chat-updates.ts:36-41`; local `social/src/rpc/context.rs:483-488`; stream closed differs (throughout) |

## Community voice

### logic/community-voice.spec.ts (88), community-voice-cache (18), community-voice-polling (8) and rpc-server/services: community voice

| spec | behaviour | local test | status |
| --- | --- | --- | --- |
| community-voice | The owner or a moderator starts; members hear STARTED; a member or non-member cannot | T `community_voice_start_join_and_ban_semantics`; CV `moderators_start_and_end_and_members_cannot` | covered, ported |
| community-voice | The owner or a moderator ends; members hear ENDED; a member cannot | T `a_former_owner_cannot_start_or_end_voice_chat_in_a_deleted_community`; CV `moderators_start_and_end_and_members_cannot` | covered, ported |
| community-voice | Join: a private member joins; a non-member of a private community and a banned wallet cannot | T `community_voice_start_join_and_ban_semantics` | covered |
| community-voice | Join: a public non-member joins | T `community_voice_start_join_and_ban_semantics` asserts the opposite | differs: membership is required. Upstream `src/logic/community-voice/validation.ts:49-53`; local `social/src/rpc/service/community_voice.rs:111-118` |
| community-voice | Mute: who may mute whom; the refused never reach the runtime; a checksummed target arrives lowercased | `domain.rs` `owner_protection_only_bites_a_non_owner_targeting_the_owner`, `the_moderator_gate_reads_both_role_spellings_and_refuses_members`; CV `who_may_mute_whom` | covered, ported |
| community-voice | Self-mute is free; self-unmute needs the audience | CV `who_may_mute_whom` | ported |
| community-voice | Raising a hand needs the audience; lowering never does | CV `who_may_raise_a_hand` | ported |
| community-voice | The active list hides an unlisted room from non-members | CT `voice_status_derives_from_the_presence_rows` (listed), `an_unlisted_room_is_listed_only_for_members` | covered, ported |
| promote-, demote-, reject-speak-request-, kick- (136) | A moderator or the owner acts on the audience, a moderator never on the owner, a member or guest never; a private target must be a member | T `community_voice_start_join_and_ban_semantics` (kick); CV `who_may_moderate_the_room` | covered, ported |
| promote-, demote- | A banned target is refused | — | differs: a public community lets it through (a ban deletes the member row and the target gate reads no ban). Upstream `src/logic/community-voice/validation.ts:207-209`; local `social/src/rpc/service/domain.rs:409`, `:428` |
| demote- | Demoting yourself needs no moderator | — | differs: the moderator gate applies. Upstream `src/controllers/handlers/rpc/demote-speaker-in-community-voice-chat.ts:46`, `:63`; local `social/src/rpc/service/domain.rs:452-453` |
| start-, end-, promote-, demote-, reject-, kick-, muting another | A banned actor whose role row survived is refused | — | differs: these gates read only the member row (join, raising a hand and self-unmute read bans). Upstream `src/logic/community-voice/validation.ts:92-95`, `:141-144`; local `social/src/rpc/service/domain.rs:345-346`, `:364-373` |
| all | An empty community id or target answers `invalidRequest`; a missing community answers `invalidRequest` (promote, demote, reject), not found (join, raise a hand, self-unmute) or forbidden (start, end) | — | differs: a missing community or an unparsable id is forbidden, but ok when lowering a hand or self-muting; an empty target in a public community passes the gate and the runtime's failure is logged with an ok answer. Upstream `src/controllers/handlers/rpc/promote-speaker-in-community-voice-chat.ts:34-43`, `:57-59`, `src/logic/community-voice/validation.ts:41-42`; local `social/src/rpc/service/domain.rs:373`, `social/src/rpc/service/community_voice.rs:57-59`, `:160-187`, `:412-437` |
| start-, end-, join-, raise a hand, self-unmute | No active room answers not found; an active room answers conflict on start | — | differs: no room-live check; start mints again. Upstream `src/logic/community-voice/community-voice.ts:116-119`, `:309-312`, `src/logic/community-voice/validation.ts:41-42`; local `social/src/rpc/service/community_voice.rs:44-75`, `:111-140`, `social/src/rpc/service/domain.rs:385` |
| start-, join- | A runtime failure is an internal error | VR `a_failed_community_grant_is_an_internal_error` | ported |
| promote-, demote-, reject-, kick-, mute-, request-to-speak-, end- | A runtime error answers an error (not found or internal) | — | differs: logged, the answer is ok. Upstream `src/controllers/handlers/rpc/promote-speaker-in-community-voice-chat.ts:78`, `:147`; local `social/src/rpc/service/community_voice.rs:17-20` |
| community-voice | A ban landing during a join refuses it and evicts the seat | — | differs: the ban is read once, before the grant. Upstream `src/logic/community-voice/community-voice.ts:279-290`; local `social/src/rpc/service/community_voice.rs:111-121`, `:125-150` |
| community-voice | STARTED carries positions, worlds and image, and reaches every subscriber of a public listed community | — | differs: empty lists, no image, members only. Upstream `src/logic/community-voice/community-voice.ts:208`, `src/logic/updates.ts:401-402`; local `social/src/rpc/service/domain.rs:312-327` |
| community-voice | The profile reaches the room metadata | — | differs: none. Upstream `src/logic/community-voice/community-voice.ts:124`; local `comms/src/gatekeeper/voice/rooms.rs:356` |
| community-voice-polling (8) | A room ending out of band reaches subscribers as ENDED | comms `gatekeeper::tests::a_community_room_lives_while_a_moderator_is_present_and_sweeps_otherwise` | differs: it writes notifications, not a stream update. Upstream `src/logic/community-voice/community-voice-polling.ts:118-120`; local `comms/src/gatekeeper/voice/mod.rs:1183-1205`, `social/src/comms_directory.rs:39-47` |
| community-voice-cache (18) | The Redis cache | — | n/a: room state is derived from `social.community_voice_chat_users` |
| subscribe-to-community-voice-chat-updates (11) | Updates in order with their fields | `service/helpers.rs` `pubsub_routing_tests::one_publish_reaches_only_the_matching_stream_in_order` | covered (routing); stream closed differs (throughout) |

## The update fan-out and the logic behind every answer

### logic/updates.spec.ts (94)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| A friendship update reaches the other party; with no subscriber it goes nowhere | 3 | T `two_explorers_exchange_a_friend_request_over_the_socket`; F `cancel_reject_and_delete_reach_the_other_party`; `pubsub.rs` `a_channel_without_subscribers_is_forgotten_on_the_next_publish` | covered; the unparseable-message case n/a (events are typed and in process) |
| Accepting a friendship tells each party's open connectivity stream that the other is ONLINE, when online | 7 | — | differs: no connectivity update follows an accept; the new friend appears in a new stream's replay or on the next presence change. Upstream `src/logic/updates.ts:107-139`; local `social/src/rpc/service/friends.rs:275-280` |
| A friend's connectivity reaches each online friend; none when no friend is online | 4 | T `two_explorers_exchange_a_friend_request_over_the_socket`, `connectivity_friend_pages_and_delivery_predicate_revoke_blocks_in_both_directions`; `topology.rs` `bounded_page_progress_survives_retry_rounds_without_restarting_first_page` | covered; differs: a failed recipient read is retried from its cursor, where upstream logs and drops the update. Upstream `src/logic/updates.ts:145-158`; local `social/src/rpc/topology.rs:40-42` |
| A member's status reaches its online co-members in each of its communities, in batches | 6 | CN `a_members_status_reaches_its_co_members_in_each_community`; T `connectivity_replay_members_pages_exclude_self_and_cover_multiple_communities` | covered, ported; differs: a failed read is retried (as above). Upstream `src/logic/updates.ts:165-183`; local `social/src/rpc/context.rs:409-435` |
| A join, leave, kick or ban reaches the community's online members and the affected member | 7 | — | differs: no stream update follows a membership change, and the delivery check would refuse one to a wallet that is no longer a member. Upstream `src/logic/updates.ts:262-297`; local `social/src/rpc/context.rs:266-270`, `social/src/rpc/db.rs:1113-1119` |
| A block update reaches the blocked wallet | 3 | F `blocking_a_friend_ends_the_friendship_and_tells_them`, `unblocking_a_stranger_only_lifts_the_block` | covered |
| Private voice: REQUESTED to the callee, ACCEPTED and REJECTED to the caller, ENDED and EXPIRED to both | 14 | T `blocked_pair_cannot_start_call_and_friends_can`, `reject_and_disconnect_delete_through_voice_owner_before_signalling`; PV `each_party_gets_its_own_grant_and_the_callee_sees_the_incoming_call`, `a_caller_that_disconnects_ends_the_call_for_the_callee`, `the_expiry_sweep_pages_through_and_tells_both_parties` | covered; the payload differs as recorded under private voice; missing addresses, an unknown status and logging n/a |
| Community voice STARTED: every subscriber for a public listed community, members otherwise, never the starter | 10 | CV `moderators_start_and_end_and_members_cannot`; T `community_voice_start_join_and_ban_semantics` | covered, ported; differs: members only, as recorded under community voice. Upstream `src/logic/updates.ts:343-374`, `:401-402`; local `social/src/rpc/service/domain.rs:312-327` |
| Community voice ENDED reaches the audience its start had; a failed member read still announces a public listed start | 11 | CV `moderators_start_and_end_and_members_cannot` | covered (members), ported (a wallet with no role hears nothing); differs: a non-member told of a public room is not told it ended, and a failed member read tells no one, at start (a dcl-rpc remote error) and at end. Upstream `src/logic/updates.ts:343-389`; local `social/src/rpc/service/domain.rs:283-289`, `social/src/rpc/service/community_voice.rs:44-47` |
| Community voice: a missing community at start, logging | 3 | T `a_former_owner_cannot_start_or_end_voice_chat_in_a_deleted_community` | covered (the refusal); logging n/a |
| A second subscription of one stream on one connection ends with the stream-closed notice | 6 | — | differs (throughout); its metric and log cases n/a |
| One address on two connections: both receive | 2 | `pubsub.rs` `publish_is_shared_by_every_receiver` | covered |
| The initial snapshot comes before live updates: an update published after subscribing follows the replay; a failed snapshot continues live | 3 | T `two_explorers_exchange_a_friend_request_over_the_socket`; CN `a_live_update_published_after_subscribing_follows_the_snapshot`; `subscriptions.rs` `replay_tests` | covered, ported; differs: a failed snapshot ends the stream, as recorded under the friend-connectivity row |
| A stream without a connection id or emitter | 2 | — | n/a: an unauthenticated transport is refused before any stream (`social/src/rpc/service/mod.rs:39-44`) |
| The live loop yields the parsed updates its filter accepts, and skips one whose profile is not found | 10 | `service/helpers.rs` `pubsub_routing_tests::one_publish_reaches_only_the_matching_stream_in_order` | covered; differs: an update whose profile is not found is delivered with a placeholder (throughout); registration and error-path cases n/a |
| Deleting a community tells its online members it went OFFLINE | 3 | — | differs: no stream update, and the delivery check requires an active community. Upstream `src/logic/updates.ts:308-322`; local `social/src/rpc/db.rs:1119` |

### logic/ws-pool-component.spec.ts (16)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| Each open socket counts; closing one frees its slot | 4 | S `closing_an_authenticated_socket_releases_its_connection`, `each_socket_counts_and_closing_one_frees_its_slot`, `an_authenticated_socket_outlives_the_handshake_timeout` | covered, ported |
| A socket counts from the moment it opens | 1 | — | differs: only an authenticated socket counts. Upstream `src/controllers/handlers/uws/ws-handler.ts:363`; local `social/src/rpc/mod.rs:219` |
| A full pool refuses a new socket with 1013; a freed slot is taken again | 3 | S `a_full_pool_closes_a_new_socket_until_a_slot_frees` | ported; differs: the reason reads "Server is at capacity; try again later" (upstream "Unable to acquire connection"), and the check runs before the handshake while the count follows it, so concurrent handshakes can pass a full pool. Upstream `src/logic/ws-pool/component.ts:26-29`, `src/controllers/handlers/uws/ws-handler.ts:407`; local `social/src/rpc/ws.rs:55-65`, `social/src/rpc/mod.rs:219` |
| Shutdown ends each socket with 1001 "Server shutting down" | 3 | — | differs: nothing sends 1001; the server job stops. Upstream `src/logic/ws-pool/component.ts:59-72`; local `social/src/rpc/mod.rs:176-181` |
| Duration metrics, unknown connections, the rejection metric, connection ids | 5 | — | n/a |

### logic/peers-stats.spec.ts (8)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| The online peers are the deduplicated union of every source; none from empty sources | 3 | comms `topology.rs` `complete_replacement_invalid_retention_and_quiet_expiry` | covered; differs: one source (the Pulse island frames), not archipelago and worlds stats, and every address is lowercased with anything that is not a wallet dropped. Upstream `src/logic/peers-stats/peers-stats.ts:12-14`; local `comms/src/topology.rs:56-62` |
| A failing source is skipped; all failing gives none | 4 | comms `topology.rs` `complete_replacement_invalid_retention_and_quiet_expiry` | covered; differs: a frame that does not decode keeps the previous set until its receipt expires, and an expired receipt empties it. Upstream `src/logic/peers-stats/peers-stats.ts:12-14`; local `comms/src/topology.rs:55`, `:89-97` |
| Every source is asked | 1 | — | n/a |

### logic/profiles.spec.ts (16)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| The first avatar is the profile | 1 | `profiles.rs` `the_first_avatar_names_the_profile_and_falls_back_to_unclaimed_name` | ported |
| No avatar, or neither a name nor an unclaimed name, means no profile | 4 | `profiles.rs` `without_a_pool_every_address_is_a_placeholder_in_input_order` | differs: a placeholder (throughout) |
| The name, else `unclaimedName` | 2 | `profiles.rs` `profiles_come_from_the_active_profile_entity`, `the_first_avatar_names_the_profile_and_falls_back_to_unclaimed_name` | covered, ported; differs: an empty `name` beside an `unclaimedName` is a placeholder. Upstream `src/logic/profiles.ts:19-21`; local `social/src/rpc/profiles.rs:61-62`, `:155-158` |
| The user id is required and lowercased | 3 | `profiles.rs` `profiles_come_from_the_active_profile_entity` | differs: the address is the caller's, as recorded under friendships. Upstream `src/logic/profiles.ts:33-35`; local `social/src/rpc/profiles.rs:257` |
| The picture is `face256`, required | 3 | `profiles.rs` `profiles_come_from_the_active_profile_entity` | covered; differs: the node's face URL, as recorded under friendships |
| A full profile carries the name color only when present | 1 | `profiles.rs` `profiles_come_from_the_active_profile_entity` | covered |
| An absent `hasClaimedName` is false | 1 | `profiles.rs` `profiles_come_from_the_active_profile_entity` | ported |
| The minimal profile kept in the cache | 1 | — | n/a |

### logic/notifications.spec.ts (6, and 2 `it.each` tables)

| behaviour | upstream cases | local test | status |
| --- | --- | --- | --- |
| A request or an accept notifies; cancel, reject and delete do not | the 2 tables | — | differs: nothing notifies, as recorded under friends-logic. Upstream `src/logic/notifications.ts:78-79`; local `social/src/rpc/service/friends.rs:277-280` |
| The event names both parties, and the message when there is one | 3 | — | differs: no event is sent. Upstream `src/logic/notifications.ts:28-60`; local `social/src/rpc/service/friends.rs:277-280` |
| An unknown action is an error | 1 | — | n/a: the action is a closed enum (`social/src/rpc/service/domain.rs:21-28`) |
| The SNS publish retries | 2 | — | n/a |
