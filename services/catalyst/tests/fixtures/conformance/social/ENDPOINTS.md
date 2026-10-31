# Social family: endpoint enumeration

Baseline = the production `*.decentraland.org` services; candidate = a Catalyst node
(`serve`), which mounts each service under the configured gateway prefix (`/social-api`, `/comms-gatekeeper`,
`/notifications`, `/badges`, `/profile-images`, `/camera-reel-service`).
The rpc-social-service-ea host is mounted at `/social-rpc`.
Captured by `tests/conformance/capture/social.mjs`; the request log is
`MANIFEST.md` (rendered from `manifest.json`), the fixtures sit in one
directory per service with the raw response (`<name>.raw.json` +
`<name>.raw.body`) beside each replay-format `<name>.json`.

Sources: social-api and rpc-social-service-ea are read from a fresh clone of
`github.com/decentraland/social-service-ea` at the pinned revision below (committed
2026-10-21; production's rpc-host `/status` reported the same `commitHash` on
2026-10-26). The other services were read at the revision each checkout had on
2026-09-25 (not pinned by local checkout; the SHA is what
`git -c safe.directory='*' -C <repo> show HEAD:<path>` resolved to):

| service | source |
| --- | --- |
| social-api, rpc-social-service-ea | `github.com/decentraland/social-service-ea` @ `fb3dcb80f8cac53485696a5f7c94b720b20ae6c9`: `src/controllers/routes/http.routes.ts` (social-api, the API http-server), `src/controllers/routes/uws.routes.ts` and `src/controllers/handlers/uws/{status,privacy,ws}-handler.ts` (rpc host, the uWS server), `src/components.ts` (both servers and the http-server status checks), `docs/openapi.yaml` |
| comms-gatekeeper | `github.com-decentraland/comms-gatekeeper` @ `105df71d2ff434e6d9a0c3385494dd7367158abd`: `src/controllers/routes.ts`, `src/logic/utils.ts` (metadata keys), `docs/openapi.yaml` |
| notifications | `github.com-decentraland/notifications-workers` @ `0f113fba0bfc33856b8e184474b99e9c28046370`: `inbox/src/controllers/routes.ts`, `docs/openapi.yaml` |
| badges | no pinned upstream checkout recorded; `src/social/badges/mod.rs` is a local route inventory, not external evidence |
| profile-images | `github.com-decentraland/profile-images` @ `e75cc9a6bfa7d21831632a7fa432e369270d218e` (`src/controllers/routes.ts`: the processor's `/status` and the bearer-gated `/schedule-processing`; the image reads are the bucket behind the edge) |
| camera-reel | `github.com-decentraland/camera-reel-service` @ `69947bc697ee712c1b4cb4560ca5307773c14157`: `src/api.rs`, `src/api/{get,upload,delete,update,docs}.rs`, `src/lib.rs` (`/health`) |
| request shapes | `catalyrst/crates/catalyrst-conformance/fixtures/explorer-boot/corpus.json` (the explorer's boot traffic: `HEAD /comms-gatekeeper/status`, `POST /get-scene-adapter`, `GET /users/{address}/bans`, `GET /social-api/v2/communities?search=&onlyMemberOf=true&offset=-100&limit=100`, `GET /v1/community-voice-chats/active`, `GET /v1/mutes?limit=100&offset=0`, `GET /scene-admin`, `GET /scene-bans`, `GET /categories`, `GET /users/{address}/{badges,preview}`, `GET /entities/{cid}/face.png`, `GET /api/users/{address}`, `GET /api/places/{uuid}/images`), and `catalyrst/crates/catalyrst-conformance/fixtures/notifications/*.json` |

Capturable = an unauthenticated GET/HEAD/OPTIONS, or a POST whose body only
reads, or a signed read the run's throwaway key can issue. "signed" means
ADR-44 signed fetch; the fixture then records the throwaway wallet's auth
chain (timestamp-bound, so a replay must re-sign) and the baseline answer is
the authenticated shape for a wallet nothing is stored under. Everything
that writes (POST/PUT/PATCH/DELETE with effect), needs a service bearer
token or an operator role, is listed as not capturable.

## social-api.decentraland.org (Catalyst: `/social-api`)

| method | path | params | capturable | fixtures |
| --- | --- | --- | --- | --- |
| GET | `/v1/communities` | `limit` 1-100, `offset` <= 100000, `search`, `minimal` (needs auth + search >= 3), `onlyMemberOf`, `onlyWithActiveVoiceChat`, `roles[]`; optional signed fetch | yes | `communities-list`, `-page2`, `-search`, `-only-voice-chat`, `-limit-zero` (200: limit 0 falls back to the default), `-limit-not-a-number` (200), `-401-minimal-unauth`, `-minimal-signed`, `-origin` (with an `Origin`: `access-control-allow-origin: *`, `vary: accept-encoding`) |
| POST | `/v1/communities` | multipart create | no: write | |
| GET | `/v1/communities/{id}` | optional signed fetch | yes | `community-by-id`, `-404-unknown`, `-bad-id` (400 "Invalid identifier") |
| PUT/PATCH/DELETE | `/v1/communities/{id}` | | no: write | |
| GET | `/v1/communities/{id}/members` | `limit`, `offset`, `onlyOnline`; optional signed fetch | yes | `community-members`, `-only-online`, `-404-unknown` |
| POST | `/v1/communities/{id}/members` | join | no: write | |
| DELETE/PATCH | `/v1/communities/{id}/members/{memberAddress}` | leave, role | no: write | |
| GET | `/v1/communities/{id}/bans` | signed, moderator | signed shape only | `community-bans-unauth` (400 "Invalid Auth Chain"), `-signed-not-moderator` (401) |
| POST/DELETE | `/v1/communities/{id}/members/{memberAddress}/bans` | | no: write | |
| GET | `/v1/communities/{id}/places` | `limit`, `offset`; optional signed fetch | yes | `community-places` |
| POST | `/v1/communities/{id}/places`, DELETE `.../places/{placeId}` | | no: write | |
| GET | `/v1/communities/{id}/posts` | `limit`, `offset`; optional signed fetch | yes | `community-posts` |
| POST | `/v1/communities/{id}/posts`; DELETE `.../posts/{postId}`; POST/DELETE `.../posts/{postId}/like` | | no: write | |
| GET | `/v1/communities/{id}/requests` | signed, owner/moderator | signed shape only | `community-requests-unauth`, `-signed-not-owner` (401) |
| POST | `/v1/communities/{id}/requests`; PATCH `.../requests/{requestId}` | | no: write | |
| GET | `/v1/communities/{address}/managed` | bearer `API_ADMIN_TOKEN`; registered only when the token is configured | error shape only | `managed-communities-401-no-bearer` |
| GET | `/v1/members/{address}/communities` | `limit`, `offset`; optional signed fetch | yes | `member-communities`, `-bad-address` (200 empty page) |
| POST | `/v1/members/{address}/communities` | community ids in the body; bearer `API_ADMIN_TOKEN`, registered only when the token is configured | no: bearer | |
| GET | `/v1/members/{address}/requests` | signed, own address | yes (signed) | `member-requests-unauth`, `-signed` |
| GET | `/v1/members/{address}/invites` | signed | yes (signed) | `member-invites-unauth`, `-signed-self` (400 "Users cannot invite themselves"), `-signed-other` (200 `{"data":[]}`) |
| GET | `/v1/referral-progress` | signed | yes (signed) | `referral-progress-unauth`, `-signed` |
| POST/PATCH | `/v1/referral-progress` | signed | no: write | |
| POST | `/v1/referral-email` | signed | no: write | |
| GET | `/v1/mutes` | `limit` (default 10), `offset`, `address`, `addresses[]`; signed | yes (signed) | `mutes-400-unauth` (400 "Missing authentication" shape), `mutes-signed` (the corpus form `limit=100&offset=0`) |
| POST/DELETE | `/v1/mutes` | `{"muted_address"}` body; signed | unsigned error shape only | `mutes-post-unauth`, `mutes-delete-unauth` (body `{}`: 400 "Invalid Auth Chain", the signed-fetch gate answers before the body is read) |
| GET | `/v1/community-voice-chats/active` | signed | yes (signed) | `voice-chats-active-unauth`, `-signed` |
| GET | `/v1/moderation/communities` | signed, global moderator | shape only | `moderation-communities-unauth`, `-signed-not-moderator` (403) |
| GET | `/v2/communities` | as v1, search honoured | yes | `communities-v2-list-search`, `-no-search` (200: search is not required unauthenticated) |
| GET | `/v2/communities/{id}` | | yes | `community-v2-by-id`, `-404-unknown` |
| GET | `/v2/communities/{id}/members` | | yes | `community-v2-members` |
| GET | `/v2/communities/{id}/bans` | signed | shape only | `community-bans-v2-unauth` |
| GET | `/v2/communities/{id}/requests` | signed | shape only | `community-requests-v2-unauth` |
| GET | `/v2/members/{address}/requests` | signed | shape only | `member-requests-v2-unauth` |
| GET | `/v2/communities/{id}/posts` | | yes | `community-posts-v2` |
| OPTIONS | any | CORS preflight | yes | `preflight-communities` |
| GET | `/health/live`, `/health/ready`, `/health/startup` | `@dcl/http-server` status checks (`src/components.ts`) | yes | `health-live` (200 `alive`, `text/plain`), `health-ready`, `health-startup` (200 `{"details":{"httpServer":{"status":"pass"}},"status":"pass"}`, `application/health+json`) |
| GET | unknown path | | yes | `unknown-path-404` |

Local routes with no production counterpart on this host, not captured:
`PATCH /v1/communities/{id}/posts/{postId}`, `/v1/friends`,
`/v1/friends/{peer}/messages`, `/v1/admin/communities[...]`,
`/social/communities/{id}/raw-thumbnail.png`, `/federation/communities/*`.

## rpc-social-service-ea.decentraland.org (Catalyst: `/social-rpc`)

The uWS server (`src/controllers/routes/uws.routes.ts`). Every HTTP answer
of a wrapped handler, `/health/live` and the catch-all carries
`Access-Control-Allow-Origin: *`.

| method | path | params | capturable | fixtures |
| --- | --- | --- | --- | --- |
| GET (WS upgrade) | `/` | dcl-rpc over WebSocket; the first frame is the signed auth chain (`ws-handler.ts`) | the eight reads only, by hand (`social/src/rpc/tests/upstream_reads.rs`) | `_socket/reads.json` (10 calls of a wallet created for the run; behaviour beyond it: `RPC.md`) |
| GET | `/status` | | yes | `status` (`{"version","currentTime","commitHash"}`, all three volatile) |
| ANY | `/health/live` | | yes | `health-live` (200 `alive`, no content-type) |
| GET | `/v1/users/{address}/privacy-settings` | address lower-cased | yes | `privacy-settings-unknown` (200 `{"private_messages_privacy":"all"}` for an address without a settings row), `-bad-address` (400 `{"error":"Invalid address"}`) |
| GET | `/metrics` | Prometheus | no: operator | |
| ANY | unknown path | | yes | `unknown-path-404` (404 `{"error":"Not Found"}`, JSON) |

Local routes with no production counterpart on this host, not captured:
`/social-rpc/info`, `/social-rpc/health`.

## comms-gatekeeper.decentraland.org (Catalyst: `/comms-gatekeeper`)

Metadata rules (`src/logic/utils.ts`): the comms handshake (`oldValidate`)
needs a top-level `realmName`; the scene routes need `signer`
`decentraland-kernel-scene` (or `dcl:authoritative-server` for
`/scene-admin`), `realm.{hostname,serverName}`, `sceneId`, `parcel`;
`/private-messages/token` needs `signer` `dcl:explorer`; the community voice
routes and `/users/{address}/voice-chat-status` take the social service's
bearer token; `/bans`, `/users/{address}/warnings` and the ban writes need
the moderator role.

| method | path | params | capturable | fixtures |
| --- | --- | --- | --- | --- |
| GET | `/ping` | | yes | `ping` |
| GET, HEAD | `/status` | | yes | `status`, `status-head` |
| POST | `/get-scene-adapter` | signed, explorer metadata | yes (signed read: mints a LiveKit token, redacted) | `get-scene-adapter-unauth`, `-signed-no-realm-name` (401), `-signed` (200 `{"adapter":"livekit:wss://...?access_token=<<REDACTED:jwt>>"}`) |
| POST | `/get-server-scene-adapter` | signed, authoritative server | error shape only | `get-server-scene-adapter-unauth` |
| GET | `/scene-participants` | `pointer`, `realm_name` (default `main` with a pointer) | yes | `scene-participants`, `-default-realm`, `-world`, `-400-no-params` |
| POST | `/mute` | | no: write | |
| GET | `/scene-admin` | `admin`; signed as scene | yes (signed) | `scene-admin-unauth` (400 `{"ok":false,"message":"Invalid Auth Chain"}`), `-signed-wrong-signer` (400 "Invalid metadata content"), `-signed-scene` (200: the place's admin list for the Genesis Plaza scene) |
| POST/DELETE | `/scene-admin` | | no: write | |
| GET | `/scene-bans` | `limit` (default 20), `offset`; signed as scene, admin | shape only | `scene-bans-unauth`, `-signed-wrong-signer`, `-signed-scene` (401 "User does not have permission to list scene bans") |
| GET | `/scene-bans/addresses` | signed as scene, admin | shape only | `scene-bans-addresses-unauth`, `-signed-scene` |
| POST/DELETE | `/scene-bans` | | no: write | |
| GET/POST/PUT/DELETE | `/scene-stream-access` | signed as scene, admin | GET shape only | `scene-stream-access-unauth` |
| POST | `/livekit-webhook` | LiveKit signature | no: write | |
| GET | `/private-messages/token` | signed as explorer | unauth shape only: the signed form mints a token for the wallet | `private-messages-token-unauth` |
| PATCH | `/users/{address}/private-messages-privacy` | bearer | no: write | |
| GET | `/users/{address}/voice-chat-status` | bearer | error shape only | `voice-chat-status-401-no-token` |
| POST | `/private-voice-chat`; DELETE `/private-voice-chat/{id}` | bearer | no: write | |
| GET | `/users/{userAddress}/community-voice-chat-status` | bearer | not captured (same 401 shape as voice-chat-status) | |
| POST | `/community-voice-chat`, `/community-voice-chat/status` | bearer | no: write / bearer | |
| GET | `/community-voice-chat/active` | bearer | error shape only | `community-voice-chat-active-401-no-token` |
| GET | `/community-voice-chat/{communityId}/status` | bearer | error shape only | `community-voice-chat-status-401-no-token` |
| POST/DELETE/PATCH | `/community-voice-chat/{communityId}[/users/{address}[/speak-request\|speaker\|mute]]` | bearer | no: write | |
| POST | `/cast/streamer-token`, `/cast/watcher-token`, `/cast/presentation-bot-token` | | no: write (mint) | |
| GET | `/cast/generate-stream-link` | signed as scene, admin | not captured: generates a key | |
| GET | `/cast/stream-info/{streamingKey}` | | yes | `cast-stream-info-invalid-key` (400) |
| GET | `/cast/presenters` | signed as scene | shape only | `cast-presenters-unauth` (400) |
| PUT/DELETE | `/cast/presenters/{participantIdentity}` | | no: write | |
| GET | `/users/{address}/bans` | | yes | `user-bans`, `-unknown-address`, `-bad-address` (200 `{"data":{"isBanned":false}}` for every input) |
| POST/DELETE | `/users/{address}/bans`; POST `/users/{address}/warnings` | signed, moderator | no: write | |
| GET | `/users/{address}/warnings` | signed, moderator | shape only | `user-warnings-401-unauth`, `-signed-not-moderator` |
| GET | `/bans` | signed, moderator | shape only | `bans-401-unauth` |
| GET | `/worlds/{worldName}/parcels/{baseParcel}/users/{address}/ban-status` | bearer | error shape only | `world-ban-status-no-token` (401 "Authorization header is missing") |
| OPTIONS | any | CORS preflight | yes | `preflight-get-scene-adapter` |
| GET | unknown path | | yes | `unknown-path-404` |

## notifications.decentraland.org (Catalyst: `/notifications`)

| method | path | params | capturable | fixtures |
| --- | --- | --- | --- | --- |
| GET | `/status` | | yes | `status` |
| GET | `/notifications` | `from` (ms), `limit` 1-50 (default 20), `onlyUnread` (presence); signed | yes (signed) | `notifications-unauth` (400, the body `auth-error-production.json` pins), `-signed`, `-signed-filters`, `-expired-timestamp` (401), `-bad-signature` (401) |
| POST | `/notifications` | API key | no: write | |
| PUT | `/notifications/read` | signed | no: write | |
| POST | `/notifications/email` | bearer | no: write | |
| GET | `/subscription` | signed | yes (signed) | `subscription-unauth`, `-signed` (the default document; `address` volatile) |
| PUT | `/subscription`, `/set-email`, `/confirm-email` | | no: write | |
| GET | `/subscription/opt-outs/{scope}/{scopeId}` | scope `community`; signed | yes (signed) | `opt-outs-community-unauth`, `-signed`, `opt-outs-bad-scope-signed` (400) |
| POST | `/subscription/opt-outs`; DELETE `/subscription/opt-outs/{scope}/{scopeId}` | | no: write | |
| GET | `/unsubscribe/{address}[/{notificationType}]` | signed URL from the e-mail | error shape only | `unsubscribe-address-no-signature` (401 "Invalid URL.") |
| OPTIONS | any | CORS preflight | yes | `preflight-notifications` |
| GET | unknown path | | yes | `unknown-path-404` |

## badges.decentraland.org (Catalyst: `/badges`)

| method | path | params | capturable | fixtures |
| --- | --- | --- | --- | --- |
| GET | `/categories` | | yes | `categories` (5 categories) |
| GET | `/users/{address}/badges` | | yes | `user-badges`, `-second-address`, `-unknown-address`, `-bad-address` (200 empty for a malformed address) |
| GET | `/users/{address}/preview` | | yes | `user-preview`, `-unknown-address` |
| GET | `/badges/{badgeId}/tiers` | | yes | `badge-tiers` (200 `{"data":{"tiers":[]}}` for a badge without tiers) |
| POST/DELETE | `/users/{address}/badges/{badgeId}` | admin token | no: write | |
| GET | badge art: `https://assets-cdn.decentraland.org/<badgeId>[/<tier>]/{2d,3d}/{normal,hrm,basecolor}.png` (the URLs the `assets` object carries) | | yes | `asset-2d`, `asset-3d`, `asset-404` |
| OPTIONS | any | CORS preflight | yes | `preflight-categories` |
| GET | unknown path | | yes | `unknown-path-404` |

## profile-images.decentraland.org (Catalyst: `/profile-images`)

Production answers every `/entities/{cid}/{face,body}.png` with a `301` to
`https://profile-images-bucket-43d0c58.s3.us-east-1.amazonaws.com/v1/entities/{cid}/{file}`
without checking the CID or the file name (the bucket then answers 200 or a
404 XML document); the explorer follows it. Both hops are captured.

| method | path | params | capturable | fixtures |
| --- | --- | --- | --- | --- |
| GET | `/entities/{cid}/face.png` | | yes | `face-profile-{0,1,2}` (301) + `-object` (200 image/png), `face-unknown-cid` + `-object` (404 XML), `face-bad-cid` (301) |
| GET | `/entities/{cid}/body.png` | | yes | `body-profile-{0,1,2}` + `-object`, `body-unknown-cid` |
| HEAD | `/entities/{cid}/face.png` | | yes | `face-head` |
| GET | `/entities/{cid}/other.png` | | yes | `unknown-file` (301 as well) |
| OPTIONS | `/entities/{cid}/face.png` | | yes | `preflight-face` (301, no CORS handling at the edge) |
| GET | `/` | | yes | `root-404` |
| GET | processor `/status`; POST `/schedule-processing` (bearer) | not on this host | no | |

The three profiles: the corpus CID
`bafkreickiq5m2aj4i4hi4emzcyaqvh2w2wdig325ienuqnkycz5xr5l66a` and the active
profile entities of two public community members (owner and moderator of
the largest public community), resolved through `POST
peer.decentraland.org/content/entities/active`.

## camera-reel-service.decentraland.org (Catalyst: `/camera-reel-service`)

| method | path | params | capturable | fixtures |
| --- | --- | --- | --- | --- |
| GET | `/health/live` | | yes | `health-live` |
| GET | `/api/users/{address}` | | yes | `user`, `user-unknown-address` |
| GET | `/api/users/{address}/images` | `offset`, `limit` (clamped to 100), `compact`; public photos only without the owner's signature | yes | `user-images`, `-compact-page2`, `-limit-over-max`, `-bad-limit` (400) |
| GET | `/api/images/{id}` | | yes | `image-redirect` (307 to the bucket), `image-file-redirect` (the `url` form `{id}-{ts}.jpg`) |
| GET | `/api/images/{id}/metadata` | | yes | `image-metadata`, `image-metadata-404` |
| POST | `/api/images`; DELETE `/api/images/{id}`; PATCH `/api/images/{id}/visibility` | signed | no: write | |
| GET | `/api/places/{placeId}/images` | `offset`, `limit` | yes | `place-images`, `-page2`, `-unknown-place` (200 empty) |
| POST | `/api/places/images` | `{placesIds:[...]}` + `offset`, `limit` | yes (read body) | `places-images-batch`, `-400-empty` |
| GET | `/api/wearables/{item}/images` | `<contract>-<itemId>`, `offset`, `limit` | yes | `wearable-images`, `-400-bad-item` |
| GET | `/api/docs/openapi.json` | | yes | `docs-openapi` |
| OPTIONS | any | CORS preflight | yes | `preflight-user` (200) |
| GET | unknown path | | yes | `unknown-path-404` (empty body) |

Legacy root paths (`/images`, `/users`, `/places/{id}/images`) are not
served on this host (README Coverage: "no Catalyst meaning").

## Counts

161 fixtures: social 53, social-rpc 5, gatekeeper 35, notifications 14,
badges 13, profile-images 20, camera-reel 21. 166 production requests plus 4
helper lookups on `peer.decentraland.org` across the six runs listed in
`MANIFEST.md`; the largest per-host total is social-api's 54.

Not captured and why: every write; token-minting reads under a real
identity (`/private-messages/token` signed, `/cast/generate-stream-link`);
bearer-token and moderator routes beyond their error shape (`POST
/v1/members/{address}/communities` included); the rpc host's WebSocket session beyond the reads in
`social-rpc/_socket/reads.json`; `GET /users/{userAddress}/community-voice-chat-status`
(same 401 shape as the captured voice-chat-status).

## Replay of social-api and social-rpc (2026-10-29 04:17 UTC)

`tests/conformance.rs` against revision `fa8f9acc` with the communities
mirror on production and `COMMUNITIES_ADMIN_TOKEN` set (setup and every row:
`tests/conformance/REPORT.md`, "Node setup" and "social"): social-api 32 of
53 identical and 21 different, none genuine (`communities-list-only-voice-chat`
and `community-v2-by-id` differ only in the keys `docs/compatibility.md`
records as social-api extensions; the rest is the live listing, profiles not
synced, the mirror's empty member, post and place lists, and two messages that
name the replay's own signer); social-rpc 5 of 5 identical. The member, post
and place lists of the captured community, which the mirror does not copy, are
compared in process against seeded rows
(`social/src/communities/tests/fixture_shapes.rs`, where only the recorded
extension keys still differ).

`communities-list-limit-zero` cannot show a page count: `data.pages` and
`data.total` are volatile for the listing fixtures. The 2026-09-25 replay
against a Catalyst node recorded `pages` 0 vs 2 for it (limit 0 then paged by
zero). `limit=0` now falls back to the default 100, as the capture answers
(`limit` 100, `pages` 2): `social/src/communities/http.rs`, tests
`offset_is_capped_and_ordinary_values_pass_through` (the limit) and
`paginated_page_is_one_based` (the page count).

## Replay of the other services (2026-09-25 19:31-19:40 UTC)

`catalyrst-conformance-replay` (old harness) against the `v2-target` debug
binary built 2026-09-25 18:21 UTC, `serve` on database `catalyst_conf_social`
and port 5461 with the environment (bounded outfits/emote sync from
`peer.decentraland.org`, every mirror's first pass complete within 60 s,
`LIVEKIT_ALLOW_DEV_CREDS=true`), each service replayed with the candidate
`http://127.0.0.1:5461<prefix>`:

| service | passed | expected failures | divergences to look at |
| --- | --- | --- | --- |
| gatekeeper | 10/35 | 6 signed fixtures "Expired signature"; `scene-participants` 404 (no Pulse/archipelago room state) | unauthenticated scene routes answer `{"error":"Invalid Auth Chain"}` where production answers 400 `{"ok":false,"message":"Invalid Auth Chain"}`; `get-scene-adapter-unauth` 400 vs 401; bearer routes (`voice-chat-status`, `community-voice-chat/*`, `world ban-status`, `private-messages/token`) 503 vs 401/400 (Catalyst has no social-service token configured); `/cast/*` and `/scene-stream-access` 404 (G18 gap, unimplemented at the recorded revision); `ping` body 22 B vs `pong`; `user-bans-bad-address` 400 vs 200; preflight allow-origin reflected vs `*` |
| notifications | 1/14 | 4 signed fixtures 401 | `/status` 404 (route unimplemented at the recorded revision); `/unsubscribe/{address}` 404 (unimplemented at the recorded revision); `?limit=5&onlyUnread&from=0` 400 (valueless `onlyUnread` rejected); opt-outs unknown scope 404 vs 400; every auth error body differs ("Unauthenticated: Expired signature" / "Invalid Auth Chain" vs the ADR-44 message pair); preflight 200 with `*` vs 204 with the requested headers |
| badges | 5/13 | user badges achieved 0 vs 3/5 (empty tables); `badge-tiers` 404 vs 200 empty | `/categories` lists 3 categories, production 5 (Explorer, Collector, Creator, Socializer, Builder); asset URLs: production `https://assets-cdn.decentraland.org/<id>/2d/normal.png`, Catalyst serves `/badges/assets/{path}` under `DEFAULT_ASSET_BASE_URL` `https://badges.decentraland.org`; preflight 200 `*` vs 204 |
| profile-images | 1/20 | the 19 image fixtures: production is a 301 to the bucket, Catalyst renders in process and answered 404 with no profile entities synced (`SYNC_ENTITY_TYPES=outfits,emote`) | structural: an edge redirect versus a direct `image/png`; `face-bad-cid` 400 vs 301; preflight 204 with every method vs the edge's 301 |
| camera-reel | 12/21 | `image-*` 404 (photo not in the local table) | `/api/wearables/{item}/images` 404 (camera-reel-service #62 unimplemented at the recorded revision); preflight `OPTIONS /api/users/{address}` 405 (no CORS layer under `/camera-reel-service`); `docs-openapi` 33 schema differences; `unknown-path-404` body "Not Found" vs empty; `user-images-bad-limit` message text |

Logs: `replay-<service>.log` in the run's scratchpad; the node and its
database were stopped and dropped after the run.
