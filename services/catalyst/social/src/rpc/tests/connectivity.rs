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
