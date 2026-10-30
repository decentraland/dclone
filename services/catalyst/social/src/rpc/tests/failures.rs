//! What each call answers when its database call fails, as upstream's
//! `rpc-server/services` specs (`decentraland/social-service-ea` at
//! `fb3dcb8`) assert for a rejected component: the empty page for the
//! friend and block lists, `internalServerError` for the request lists, the
//! incoming call and a block or unblock (which tells no one), and both empty
//! lists for the blocking status. The pool points at a closed port, so
//! every query fails without a database.

use super::*;
use crate::rpc::drpc::rpc_protocol::RemoteError;
use crate::rpc::proto::errors::InternalServerError;

fn unreachable_ctx() -> Context {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(250))
        .connect_lazy("postgres://postgres@127.0.0.1:1/unreachable")
        .unwrap();
    test_ctx(Db::new(pool.clone()), Arc::new(FakeVoiceRooms::new(pool)))
}

const PAGE: Option<Pagination> = Some(Pagination {
    limit: 10,
    offset: 0,
});

fn empty_page() -> Option<PaginatedResponse> {
    Some(PaginatedResponse { total: 0, page: 1 })
}

#[tokio::test]
async fn a_failed_database_call_answers_as_upstream_does() {
    let ctx = unreachable_ctx();
    let me = addr("fail1");
    let other = addr("fail2");
    let svc = SocialServiceImpl;

    let friends = svc
        .get_friends(
            GetFriendsPayload { pagination: PAGE },
            procedure_ctx(&ctx, &me, 61),
        )
        .await
        .unwrap();
    assert!(friends.friends.is_empty());
    assert_eq!(friends.pagination_data, empty_page());

    let mutual = svc
        .get_mutual_friends(
            GetMutualFriendsPayload {
                user: Some(User {
                    address: other.clone(),
                }),
                pagination: PAGE,
            },
            procedure_ctx(&ctx, &me, 62),
        )
        .await
        .unwrap();
    assert!(mutual.friends.is_empty());
    assert_eq!(mutual.pagination_data, empty_page());

    for incoming in [true, false] {
        let payload = GetFriendshipRequestsPayload { pagination: PAGE };
        let pctx = procedure_ctx(&ctx, &me, 63);
        let answer = if incoming {
            svc.get_pending_friendship_requests(payload, pctx).await
        } else {
            svc.get_sent_friendship_requests(payload, pctx).await
        }
        .unwrap();
        assert_eq!(
            answer.response,
            Some(
                paginated_friendship_requests_response::Response::InternalServerError(
                    InternalServerError { message: None }
                )
            ),
            "incoming {incoming}"
        );
        assert_eq!(answer.pagination_data, None);
    }

    let blocked = svc
        .get_blocked_users(
            GetBlockedUsersPayload { pagination: PAGE },
            procedure_ctx(&ctx, &me, 64),
        )
        .await
        .unwrap();
    assert!(blocked.profiles.is_empty());
    assert_eq!(blocked.pagination_data, empty_page());

    let status = svc
        .get_blocking_status(procedure_ctx(&ctx, &me, 65))
        .await
        .unwrap();
    assert!(status.blocked_users.is_empty() && status.blocked_by_users.is_empty());

    let mut to_other = ctx.pubsub().subscribe(&other);
    let user = || {
        Some(User {
            address: other.clone(),
        })
    };
    let block = svc
        .block_user(
            BlockUserPayload { user: user() },
            procedure_ctx(&ctx, &me, 67),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            block.response,
            Some(block_user_response::Response::InternalServerError(_))
        ),
        "{block:?}"
    );
    let unblock = svc
        .unblock_user(
            UnblockUserPayload { user: user() },
            procedure_ctx(&ctx, &me, 68),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            unblock.response,
            Some(unblock_user_response::Response::InternalServerError(_))
        ),
        "{unblock:?}"
    );
    assert!(
        to_other.try_recv().is_err(),
        "a failed write told the other party"
    );

    let incoming = svc
        .get_incoming_private_voice_chat_request(procedure_ctx(&ctx, &me, 66))
        .await
        .unwrap();
    assert!(
        matches!(
            incoming.response,
            Some(
                get_incoming_private_voice_chat_request_response::Response::InternalServerError(_)
            )
        ),
        "{incoming:?}"
    );
}

/// A failure a procedure refuses with (a dcl-rpc RemoteError 500) says only
/// `Internal Server Error`, for every procedure: the database's text goes to
/// the log, never to the client.
#[tokio::test]
async fn an_internal_refusal_carries_no_database_text() {
    let ctx = unreachable_ctx();
    let me = addr("fail3");
    let svc = SocialServiceImpl;
    let end = svc
        .end_private_voice_chat(
            EndPrivateVoiceChatPayload {
                call_id: Uuid::new_v4().to_string(),
            },
            procedure_ctx(&ctx, &me, 69),
        )
        .await
        .unwrap_err();
    let community = svc
        .start_community_voice_chat(
            StartCommunityVoiceChatPayload {
                community_id: Uuid::new_v4().to_string(),
            },
            procedure_ctx(&ctx, &me, 70),
        )
        .await
        .unwrap_err();
    for (name, refusal) in [("end", end), ("community", community)] {
        let remote = RemoteError::from(refusal);
        assert_eq!(remote.error_code, 500, "{name}");
        assert_eq!(remote.error_message, "Internal Server Error", "{name}");
    }
}
