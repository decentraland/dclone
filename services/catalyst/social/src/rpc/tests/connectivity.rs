//! The live half of `SubscribeToCommunityMemberConnectivityUpdates` over the
//! socket, against upstream's `subscribe-to-community-member-connectivity-updates`
//! spec (`decentraland/social-service-ea` at `fb3dcb8`). Catalyst passes the
//! subscriber's own update, as upstream does. Recorded difference (RPC.md,
//! friends services, "subscribe-to-community-member-connectivity-updates (7)"):
//! upstream passes every routed update through
//! (`src/controllers/handlers/rpc/subscribe-to-community-member-connectivity-updates.ts:41`,
//! `shouldHandleUpdate: () => true`); Catalyst drops at delivery an update whose
//! member is not a co-member or whose status is not the current presence.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_update_reaches_the_subscriber_only_when_current_and_co_member() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    // The server runs the private-voice expiry sweep against the scratch pool.
    let _turn = VOICE_TABLE.lock().await;
    let server = spawn_server(Some(pool.clone()), 5).await;
    let (me, _client, social) = explorer_as(&server, &PrivateKeySigner::random()).await;
    let community = seed_community(&pool, true, &me, "owner").await;
    let stranger = addr("cmc1");
    let mut updates = subscribed(
        &server,
        &me,
        social.subscribe_to_community_member_connectivity_updates(),
    )
    .await;
    let update = |member: &str, status: ConnectivityStatus| {
        SocialEvent::CommunityMember(CommunityMemberConnectivityUpdate {
            community_id: community.to_string(),
            member: Some(User {
                address: member.to_string(),
            }),
            status: status as i32,
        })
    };
    let pubsub = server.state.ctx.pubsub();
    // No presence was received, so ONLINE is not the current status: dropped.
    pubsub.publish(&me, update(&me, ConnectivityStatus::Online));
    // Not a member of the community: dropped.
    pubsub.publish(&me, update(&stranger, ConnectivityStatus::Offline));
    // The subscriber's own, current: delivered.
    pubsub.publish(&me, update(&me, ConnectivityStatus::Offline));
    let got = next_update(&mut updates).await;
    assert_eq!(got.member.map(|m| m.address), Some(me.clone()));
    assert_eq!(got.status, ConnectivityStatus::Offline as i32);
    drop_community(&pool, community).await;
    server.jobs.shutdown(Duration::from_secs(2)).await;
}

/// Upstream's `updates.spec.ts` "online members across communities"
/// (`src/logic/updates.ts:165-183`): a member's new status reaches its
/// co-members in each of its communities, and no one outside them.
#[tokio::test]
async fn a_members_status_reaches_its_co_members_in_each_community() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let ctx = test_ctx(
        Db::new(pool.clone()),
        Arc::new(FakeVoiceRooms::new(pool.clone())),
    );
    let (x, first_peer, second_peer, stranger) =
        (addr("cmf1"), addr("cmf2"), addr("cmf3"), addr("cmf4"));
    let first = seed_community(&pool, true, &x, "member").await;
    let second = seed_community(&pool, true, &x, "member").await;
    for (community, peer) in [(first, &first_peer), (second, &second_peer)] {
        sqlx::query("INSERT INTO social.community_members (community_id, member_address, role) VALUES ($1, $2, 'member')")
            .bind(community)
            .bind(peer)
            .execute(&pool)
            .await
            .unwrap();
    }
    ctx.topology()
        .receive(&pulse_frame(vec![x.clone()]))
        .unwrap();
    let generation = ctx.topology().snapshot().generation;
    let mut receivers: Vec<_> = [&first_peer, &second_peer, &stranger]
        .iter()
        .map(|a| ctx.pubsub().subscribe(a))
        .collect();
    let mut cursor = crate::rpc::context::ConnectivityCursor::default();
    for _ in 0..4 {
        let (next, done) = ctx
            .fan_topology_page(&x, true, generation, cursor)
            .await
            .unwrap();
        cursor = next;
        if done {
            break;
        }
    }
    let heard = |rx: &mut tokio::sync::broadcast::Receiver<Arc<SocialEvent>>| {
        std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|e| match e.as_ref() {
                SocialEvent::TopologyCommunityMember(u, _) => Some((
                    u.community_id.clone(),
                    u.member.as_ref().map(|m| m.address.clone()),
                    u.status,
                )),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let online = ConnectivityStatus::Online as i32;
    assert_eq!(
        heard(&mut receivers[0]),
        [(first.to_string(), Some(x.clone()), online)]
    );
    assert_eq!(
        heard(&mut receivers[1]),
        [(second.to_string(), Some(x.clone()), online)]
    );
    assert!(heard(&mut receivers[2]).is_empty());
    drop_community(&pool, first).await;
    drop_community(&pool, second).await;
}

/// Upstream's `updates.spec.ts` "queues live updates emitted during the
/// snapshot" (`src/logic/updates.ts:513-524`): an update published after
/// subscribing follows the replay. The replay is not held, so this does not
/// show that an update published during the snapshot is queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_update_published_after_subscribing_follows_the_snapshot() {
    let Some(pool) = scratch_pool().await else {
        return;
    };
    let _turn = VOICE_TABLE.lock().await;
    let server = spawn_server(Some(pool.clone()), 5).await;
    let db = Db::new(pool.clone());
    let (alice, _alice_client, alice_social) =
        explorer_as(&server, &PrivateKeySigner::random()).await;
    let (bob, _bob_client, _bob_social) = explorer_as(&server, &PrivateKeySigner::random()).await;
    cleanup_pair(&db, &alice, &bob).await;
    db.apply_friendship_action(&alice, &bob, "accept", true, None, None)
        .await
        .unwrap();
    server
        .state
        .ctx
        .topology()
        .receive(&pulse_frame(vec![alice.clone(), bob.clone()]))
        .unwrap();
    let mut updates = subscribed(
        &server,
        &alice,
        alice_social.subscribe_to_friend_connectivity_updates(),
    )
    .await;
    // Marked by its profile name, apart from the replay and from Pulse's own fan-out.
    let mut marked = crate::rpc::profiles::friend_profile(&bob, None);
    marked.name = "published after subscribing".into();
    server.state.ctx.pubsub().publish(
        &alice,
        SocialEvent::FriendConnectivity(FriendConnectivityUpdate {
            friend: Some(marked),
            status: ConnectivityStatus::Online as i32,
        }),
    );
    let first = next_update(&mut updates).await.friend.unwrap();
    assert_eq!(first.address, bob);
    assert_ne!(
        first.name, "published after subscribing",
        "the replay comes first"
    );
    let mut seen = false;
    for _ in 0..3 {
        let friend = next_update(&mut updates).await.friend.unwrap();
        if friend.name == "published after subscribing" {
            seen = true;
            break;
        }
    }
    assert!(seen, "the update published after subscribing was lost");
    cleanup_pair(&db, &alice, &bob).await;
    server.jobs.shutdown(Duration::from_secs(2)).await;
}
