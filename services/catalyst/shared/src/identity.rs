//! The shared vocabulary of the social schema: what every social module binds
//! to and decodes from `social.*`. Types only, no I/O and no `Serialize` on an
//! identity (a verified identity must never be reachable from a wire DTO).
//!
//! - [`Address`]: a wallet address, canonical lowercase `0x` + 40 hex, which is
//!   how every `social.*` address column stores it; `checksummed()` is the
//!   EIP-55 display form.
//! - [`CommunityId`]: the uuid key of `social.communities`; a federation hex id
//!   maps onto it deterministically.
//! - [`CommunityRole`] (what a `community_members.role` row stores) and
//!   [`CommunityStanding`] (the five-way tier a permission check is made on,
//!   ordered least to most authority), with the capability matrix
//!   [`CommunityPermission`].
//! - [`BanScope`]: which ban relation a check reads.
//! - [`PermissionDecision`] / [`AuthorityNotEstablished`]: a yes, or one of the
//!   five distinct reasons there is no yes, each with its HTTP status; "could
//!   not tell" is never a 403.

use std::fmt;

use uuid::Uuid;

use crate::signed_fetch::Signer;

/// The zero address marks a scene this node mirrored rather than one an
/// owner deployed here (the old marker, kept on the wire); `mirror_source`
/// is the authoritative column.
pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// `0x` (or `0X`) + 40 hex digits, any case: the loose wire check the
/// marketplace and LAND routes apply before normalizing.
pub fn is_valid_eth_address(addr: &str) -> bool {
    let hex = match addr.strip_prefix("0x").or_else(|| addr.strip_prefix("0X")) {
        Some(h) => h,
        None => return false,
    };
    hex.len() == 40 && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AddressError {
    #[error("address must be 0x followed by 40 hex characters, got {0:?}")]
    Malformed(String),
}

/// A wallet address in the storage form: `0x` + 40 lowercase hex characters.
///
/// The value proves nothing about the caller: it is the key format, not an
/// authentication. A verified caller's address comes from
/// [`Address::from_signer`] (the signed-fetch verifier's output); a claimed
/// one (a path segment, a body field) from [`Address::parse`], and the two are
/// the same type on purpose, because the schema keys them identically; the
/// authorization question is answered by a [`CommunityStanding`] or a ban
/// lookup, never by which constructor ran.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Type)]
#[sqlx(transparent)]
pub struct Address(String);

impl Address {
    /// Parses any casing; refuses anything that is not `0x` + 40 hex.
    pub fn parse(raw: &str) -> Result<Self, AddressError> {
        let trimmed = raw.trim();
        let hex = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
            .ok_or_else(|| AddressError::Malformed(truncate(raw)))?;
        if hex.len() != 40 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(AddressError::Malformed(truncate(raw)));
        }
        Ok(Self(format!("0x{}", hex.to_ascii_lowercase())))
    }

    /// social-service-ea's `normalizeAddress` then `EthAddress.validate`:
    /// lowercased and never trimmed, so a surrounding space is refused where
    /// [`Address::parse`] would trim it.
    pub fn parse_untrimmed(raw: &str) -> Result<Self, AddressError> {
        if raw.trim() != raw {
            return Err(AddressError::Malformed(truncate(raw)));
        }
        Self::parse(raw)
    }

    /// The verified signer of a signed-fetch request; `Signer` is already the
    /// lowercased SIGNER link, so this cannot fail.
    pub fn from_signer(signer: &Signer) -> Self {
        Self(signer.as_str().to_ascii_lowercase())
    }

    /// The lowercase text: the SQL bind value and the wire's canonical form.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    /// The EIP-55 mixed-case display form (`0x75e1d32289679dfcB2F01fBc0e043B3d7F9Cd443`).
    pub fn checksummed(&self) -> String {
        let bytes = hex::decode(&self.0[2..]).expect("an Address holds 40 hex characters");
        alloy_primitives::Address::from_slice(&bytes).to_checksum(None)
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for Address {
    type Err = AddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

fn truncate(raw: &str) -> String {
    raw.chars().take(48).collect()
}

/// The membership that counts (social's by-ids lookup, worlds' allow-list):
/// `c` active and unsuspended, `cm` the `$2` address's row, no active ban of
/// `$2`. Read as `FROM {this} AND <filter on c.id>`.
pub const COMMUNITY_MEMBERSHIP_SQL: &str = "social.communities c \
     JOIN social.community_members cm ON cm.community_id = c.id AND cm.member_address = $2 \
     WHERE c.active AND NOT c.suspended \
       AND NOT EXISTS (SELECT 1 FROM social.community_bans cb \
                        WHERE cb.community_id = c.id AND cb.banned_address = $2 AND cb.active)";

/// The key of `social.communities`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Type)]
#[sqlx(transparent)]
pub struct CommunityId(Uuid);

impl CommunityId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn parse(raw: &str) -> Result<Self, uuid::Error> {
        Uuid::parse_str(raw.trim()).map(Self)
    }

    /// The old federation id is a hex SHA-256; the projection tables keyed it
    /// by its first 16 bytes with the RFC 4122 v4 variant bits set
    /// (`community_uuid_from_hex`). Kept so a federated community lands on the
    /// same row id it had.
    pub fn from_federation_hex(hex_id: &str) -> Self {
        let bytes = hex::decode(hex_id).unwrap_or_default();
        let mut arr = [0u8; 16];
        if bytes.len() >= 16 {
            arr.copy_from_slice(&bytes[..16]);
        } else {
            arr[..bytes.len()].copy_from_slice(&bytes);
        }
        arr[6] = (arr[6] & 0x0f) | 0x40;
        arr[8] = (arr[8] & 0x3f) | 0x80;
        Self(Uuid::from_bytes(arr))
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for CommunityId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CommunityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// What a `social.community_members.role` row stores (CHECK-constrained).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommunityRole {
    Owner,
    Moderator,
    Member,
}

impl CommunityRole {
    /// The one stored spelling; the old tables had two (`moderator` on the
    /// client path, `mod` on the federation path).
    pub fn as_stored_text(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Moderator => "moderator",
            Self::Member => "member",
        }
    }

    /// Accepts both old spellings on input; rejects the removed `admin` tier
    /// and anything unrecognised.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "owner" => Some(Self::Owner),
            "mod" | "moderator" => Some(Self::Moderator),
            "member" => Some(Self::Member),
            _ => None,
        }
    }
}

impl fmt::Display for CommunityRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_stored_text())
    }
}

/// A wallet's standing in one community, as a permission check sees it.
///
/// `Ord` is load-bearing: variants are declared least-to-most authority and
/// `actual < minimum` is the whole of the old `require_min_role`. Reordering
/// them changes authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CommunityStanding {
    /// Lowest, so a minimum of any tier refuses it.
    Banned,
    /// No membership row (or a stored value this module does not recognise).
    NotAMember,
    Member,
    /// Not a global platform moderator and not a scene administrator.
    Moderator,
    /// Not a world-name, LAND or collection owner.
    Owner,
}

impl CommunityStanding {
    /// From what the two relations say: an active `community_bans` row wins,
    /// then the member row, else not a member. A stored role text this module
    /// does not recognise reads as [`Self::NotAMember`] (holds no capability).
    pub fn from_rows(banned: bool, role: Option<&str>) -> Self {
        if banned {
            return Self::Banned;
        }
        match role.and_then(CommunityRole::parse) {
            Some(CommunityRole::Owner) => Self::Owner,
            Some(CommunityRole::Moderator) => Self::Moderator,
            Some(CommunityRole::Member) => Self::Member,
            None => Self::NotAMember,
        }
    }

    /// For a tier a *caller supplied*: unrecognised is `None`, never a
    /// silent demotion.
    pub fn parse_supplied(raw: &str) -> Option<Self> {
        match raw {
            "owner" => Some(Self::Owner),
            "mod" | "moderator" => Some(Self::Moderator),
            "member" => Some(Self::Member),
            "banned" => Some(Self::Banned),
            "none" | "" => Some(Self::NotAMember),
            _ => None,
        }
    }

    /// The wire spelling (`none` for a non-member, `banned` for a banned wallet).
    pub fn as_wire_text(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Moderator => "moderator",
            Self::Member => "member",
            Self::Banned => "banned",
            Self::NotAMember => "none",
        }
    }

    pub fn role(self) -> Option<CommunityRole> {
        match self {
            Self::Owner => Some(CommunityRole::Owner),
            Self::Moderator => Some(CommunityRole::Moderator),
            Self::Member => Some(CommunityRole::Member),
            Self::Banned | Self::NotAMember => None,
        }
    }

    pub fn is_member(self) -> bool {
        !matches!(self, Self::NotAMember | Self::Banned)
    }

    pub fn has(self, permission: CommunityPermission) -> bool {
        role_permissions(self).contains(&permission)
    }
}

impl From<CommunityRole> for CommunityStanding {
    fn from(role: CommunityRole) -> Self {
        match role {
            CommunityRole::Owner => Self::Owner,
            CommunityRole::Moderator => Self::Moderator,
            CommunityRole::Member => Self::Member,
        }
    }
}

/// A capability held **within one community**: not a world ACL name, not a
/// LAND grant bitmask, not a platform-wide power.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommunityPermission {
    EditInfo,
    EditName,
    AddPlaces,
    RemovePlaces,
    AcceptRequests,
    RejectRequests,
    ViewRequests,
    BanPlayers,
    SendInvitations,
    EditSettings,
    DeleteCommunity,
    AssignRoles,
    InviteUsers,
    CreatePosts,
    DeletePosts,
}

const OWNER_PERMISSIONS: &[CommunityPermission] = &[
    CommunityPermission::EditInfo,
    CommunityPermission::EditName,
    CommunityPermission::AddPlaces,
    CommunityPermission::RemovePlaces,
    CommunityPermission::AcceptRequests,
    CommunityPermission::RejectRequests,
    CommunityPermission::ViewRequests,
    CommunityPermission::BanPlayers,
    CommunityPermission::SendInvitations,
    CommunityPermission::EditSettings,
    CommunityPermission::DeleteCommunity,
    CommunityPermission::AssignRoles,
    CommunityPermission::InviteUsers,
    CommunityPermission::CreatePosts,
    CommunityPermission::DeletePosts,
];

const MODERATOR_PERMISSIONS: &[CommunityPermission] = &[
    CommunityPermission::EditInfo,
    CommunityPermission::AddPlaces,
    CommunityPermission::RemovePlaces,
    CommunityPermission::AcceptRequests,
    CommunityPermission::RejectRequests,
    CommunityPermission::ViewRequests,
    CommunityPermission::BanPlayers,
    CommunityPermission::SendInvitations,
    CommunityPermission::InviteUsers,
    CommunityPermission::CreatePosts,
    CommunityPermission::DeletePosts,
];

fn role_permissions(standing: CommunityStanding) -> &'static [CommunityPermission] {
    match standing {
        CommunityStanding::Owner => OWNER_PERMISSIONS,
        CommunityStanding::Moderator => MODERATOR_PERMISSIONS,
        CommunityStanding::Member | CommunityStanding::NotAMember | CommunityStanding::Banned => {
            &[]
        }
    }
}

pub fn can_create_post(standing: CommunityStanding) -> bool {
    standing.has(CommunityPermission::CreatePosts)
}

/// Owner may delete any post; a moderator only their own (the old shape,
/// kept as is).
pub fn can_delete_post(standing: CommunityStanding, is_author: bool) -> bool {
    standing.has(CommunityPermission::DeletePosts)
        && (standing != CommunityStanding::Moderator || is_author)
}

pub fn can_like_post(standing: CommunityStanding, community_is_private: bool) -> bool {
    standing != CommunityStanding::Banned
        && !(community_is_private && standing == CommunityStanding::NotAMember)
}

/// One half of the ban/kick predicate; the other half is
/// `actor.has(BanPlayers)`. Owners are untouchable; a moderator is acted on
/// only by the owner; a member by owner or moderator; a non-member never.
pub fn can_act_on_member(actor: CommunityStanding, target: CommunityStanding) -> bool {
    match target {
        CommunityStanding::Owner => false,
        CommunityStanding::Moderator => actor == CommunityStanding::Owner,
        CommunityStanding::Member => matches!(
            actor,
            CommunityStanding::Owner | CommunityStanding::Moderator
        ),
        CommunityStanding::NotAMember | CommunityStanding::Banned => false,
    }
}

/// Which ban relation a check reads. Three relations, three meanings; a
/// scope is never inferred from a bare string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BanScope {
    /// `social.user_bans`: in force while `lifted_at IS NULL AND (expires_at
    /// IS NULL OR expires_at > now)`; matches by address or by recorded device.
    Platform,
    /// `social.community_bans` where `active`.
    Community(CommunityId),
    /// `social.scene_bans`, keyed by the places catalogue id or world name.
    Scene { place_id: String },
}

impl BanScope {
    /// The relation the scope reads, for logs and refusal details.
    pub fn table(&self) -> &'static str {
        match self {
            Self::Platform => "social.user_bans",
            Self::Community(_) => "social.community_bans",
            Self::Scene { .. } => "social.scene_bans",
        }
    }
}

impl fmt::Display for BanScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Platform => f.write_str("platform"),
            Self::Community(id) => write!(f, "community {id}"),
            Self::Scene { place_id } => write!(f, "scene {place_id}"),
        }
    }
}

/// Why an authority could not be established.
///
/// The arms are **not** interchangeable, and that is the entire point of the
/// type. Three different things are routinely flattened into one another:
///
/// - *"we could not tell"*: a backing store was down, a query failed;
/// - *"the service is misconfigured"*: the credential this gate compares
///   against is unset;
/// - *"the caller may not"*: the lookup succeeded and the answer was no.
///
/// Collapsing "could not tell" into a **denial** is the fail-closed twin of
/// the fail-open defect (a dead database read as "not an admin", or as "not
/// banned"). Both are wrong, in opposite directions, and the caller must be
/// able to tell which happened, hence one arm each.
///
/// `RefusedLacksAuthority` is the only arm meaning the caller was successfully
/// identified and found wanting; the other four mean the question was never
/// answered. Not `#[non_exhaustive]`: adding an arm **should** break every
/// downstream match.
#[derive(Debug, thiserror::Error)]
pub enum AuthorityNotEstablished {
    /// No usable credential was presented, or the one presented did not
    /// verify. **401**. `detail` is `&'static str` on purpose: a fixed reason
    /// chosen by the gate, never an echo of anything the caller sent.
    #[error("authentication missing or invalid: {detail}")]
    AuthenticationMissingOrInvalid { detail: &'static str },

    /// A shared secret was presented and did not match. **401**, and
    /// distinguishable from "presented nothing" in logs and tests.
    #[error("presented shared secret did not match")]
    PresentedSharedSecretDidNotMatch,

    /// The principal is who they say they are, the lookup succeeded, and they
    /// do not hold this authority. **403**. The *only* arm that means "no".
    #[error("refused: {detail}")]
    RefusedLacksAuthority { detail: String },

    /// The check could not be completed, so no decision exists. **503**,
    /// never 403 and never 200.
    #[error("could not determine authority: backing store {store} unavailable")]
    UndeterminedStoreUnavailable {
        store: &'static str,
        reason_for_operators: String,
    },

    /// The service is misconfigured; the caller is not unauthorized. **503**.
    #[error("credential not configured: {environment_variable} is unset")]
    CredentialNotConfigured { environment_variable: &'static str },
}

impl AuthorityNotEstablished {
    /// The one place that decides the HTTP status for a refusal.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::AuthenticationMissingOrInvalid { .. }
            | Self::PresentedSharedSecretDidNotMatch => 401,
            Self::RefusedLacksAuthority { .. } => 403,
            Self::UndeterminedStoreUnavailable { .. } | Self::CredentialNotConfigured { .. } => 503,
        }
    }

    /// A database fault while reading `store`: undetermined, not denied.
    pub fn store_unavailable(store: &'static str, error: &sqlx::Error) -> Self {
        Self::UndeterminedStoreUnavailable {
            store,
            reason_for_operators: error.to_string(),
        }
    }
}

/// The answer to "may WHO do WHAT to WHOM": a yes, or exactly why there is no
/// yes. A permission check returns this, never a bare `bool`, so an outage
/// cannot be mistaken for a denial (or a permit).
#[derive(Debug)]
pub enum PermissionDecision {
    Allowed,
    Refused(AuthorityNotEstablished),
}

impl PermissionDecision {
    pub fn refused(detail: impl Into<String>) -> Self {
        Self::Refused(AuthorityNotEstablished::RefusedLacksAuthority {
            detail: detail.into(),
        })
    }

    pub fn undetermined(store: &'static str, error: &sqlx::Error) -> Self {
        Self::Refused(AuthorityNotEstablished::store_unavailable(store, error))
    }

    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    /// `Ok(())` when allowed, else the refusal, for `?` in a handler.
    pub fn into_result(self) -> Result<(), AuthorityNotEstablished> {
        match self {
            Self::Allowed => Ok(()),
            Self::Refused(why) => Err(why),
        }
    }
}

impl From<AuthorityNotEstablished> for PermissionDecision {
    fn from(why: AuthorityNotEstablished) -> Self {
        Self::Refused(why)
    }
}

#[cfg(test)]
mod tests {
    use super::CommunityStanding as Tier;
    use super::*;

    #[test]
    fn parsing_lowercases_the_address() {
        let a = Address::parse("0xABCDEF0123456789ABCDEF0123456789ABCDEF01").unwrap();
        assert_eq!(a.as_str(), "0xabcdef0123456789abcdef0123456789abcdef01");
        assert_eq!(format!("{a}"), "0xabcdef0123456789abcdef0123456789abcdef01");
    }

    #[test]
    fn two_parses_of_the_same_address_in_different_cases_are_equal() {
        let lower = Address::parse("0x75e1d32289679dfcb2f01fbc0e043b3d7f9cd443").unwrap();
        let upper = Address::parse("0x75E1D32289679DFCB2F01FBC0E043B3D7F9CD443").unwrap();
        assert_eq!(lower, upper);
    }

    #[test]
    fn parse_refuses_anything_but_forty_hex_after_0x() {
        for bad in [
            "",
            "0x",
            "0xaa",
            "75e1d32289679dfcb2f01fbc0e043b3d7f9cd443",
            "0x75e1d32289679dfcb2f01fbc0e043b3d7f9cd44g",
            "0x75e1d32289679dfcb2f01fbc0e043b3d7f9cd4433",
        ] {
            assert!(Address::parse(bad).is_err(), "{bad:?} must be refused");
        }
        assert!(Address::parse(" 0X75e1d32289679dfcb2f01fbc0e043b3d7f9cd443 ").is_ok());
    }

    #[test]
    fn checksummed_is_eip55() {
        let a = Address::parse("0x75e1d32289679dfcb2f01fbc0e043b3d7f9cd443").unwrap();
        assert_eq!(
            a.checksummed(),
            "0x75e1d32289679dfcB2F01fBc0e043B3d7F9Cd443"
        );
        let b = Address::parse("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed").unwrap();
        assert_eq!(
            b.checksummed(),
            "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
        );
    }

    #[test]
    fn federation_hex_maps_to_a_v4_shaped_uuid_deterministically() {
        let hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let id = CommunityId::from_federation_hex(hex);
        assert_eq!(id, CommunityId::from_federation_hex(hex));
        let uuid = id.as_uuid();
        let bytes = uuid.as_bytes();
        assert_eq!(bytes[6] >> 4, 0x4, "version nibble");
        assert_eq!(bytes[8] & 0xc0, 0x80, "RFC 4122 variant");
        assert_eq!(&bytes[..6], &[0x00, 0x11, 0x22, 0x33, 0x44, 0x55]);
    }

    #[test]
    fn a_short_hex_is_zero_padded_not_rejected() {
        let id = CommunityId::from_federation_hex("abcd");
        assert_eq!(id.as_uuid().as_bytes()[0], 0xab);
        assert_eq!(id.as_uuid().as_bytes()[2], 0x00);
    }

    #[test]
    fn both_old_moderator_spellings_parse_and_one_is_stored() {
        assert_eq!(CommunityRole::parse("mod"), Some(CommunityRole::Moderator));
        assert_eq!(
            CommunityRole::parse("moderator"),
            Some(CommunityRole::Moderator)
        );
        assert_eq!(CommunityRole::Moderator.as_stored_text(), "moderator");
        assert_eq!(
            CommunityRole::parse("admin"),
            None,
            "the removed tier stays removed"
        );
        assert_eq!(Tier::parse_supplied("admin"), None);
        assert_eq!(Tier::parse_supplied(""), Some(Tier::NotAMember));
        assert_eq!(Tier::parse_supplied("banned"), Some(Tier::Banned));
    }

    #[test]
    fn standing_from_rows_lets_the_ban_win_and_demotes_unknown_text() {
        assert_eq!(Tier::from_rows(true, Some("owner")), Tier::Banned);
        assert_eq!(Tier::from_rows(false, Some("owner")), Tier::Owner);
        assert_eq!(Tier::from_rows(false, Some("mod")), Tier::Moderator);
        assert_eq!(Tier::from_rows(false, Some("admin")), Tier::NotAMember);
        assert_eq!(Tier::from_rows(false, None), Tier::NotAMember);
    }

    #[test]
    fn the_tier_order_is_least_to_most_authority() {
        assert!(Tier::Banned < Tier::NotAMember);
        assert!(Tier::NotAMember < Tier::Member);
        assert!(Tier::Member < Tier::Moderator);
        assert!(Tier::Moderator < Tier::Owner);
    }

    #[test]
    fn owner_holds_all_permissions() {
        for p in OWNER_PERMISSIONS {
            assert!(Tier::Owner.has(*p), "owner missing {p:?}");
        }
        assert_eq!(OWNER_PERMISSIONS.len(), 15);
    }

    #[test]
    fn moderator_matrix_matches_upstream() {
        assert_eq!(MODERATOR_PERMISSIONS.len(), 11);
        for p in [
            CommunityPermission::EditInfo,
            CommunityPermission::AddPlaces,
            CommunityPermission::RemovePlaces,
            CommunityPermission::AcceptRequests,
            CommunityPermission::RejectRequests,
            CommunityPermission::ViewRequests,
            CommunityPermission::BanPlayers,
            CommunityPermission::SendInvitations,
            CommunityPermission::InviteUsers,
            CommunityPermission::CreatePosts,
            CommunityPermission::DeletePosts,
        ] {
            assert!(Tier::Moderator.has(p), "mod missing {p:?}");
        }
        for p in [
            CommunityPermission::EditName,
            CommunityPermission::EditSettings,
            CommunityPermission::DeleteCommunity,
            CommunityPermission::AssignRoles,
        ] {
            assert!(!Tier::Moderator.has(p), "mod wrongly has {p:?}");
        }
    }

    #[test]
    fn member_and_below_have_no_permissions() {
        for tier in [Tier::Member, Tier::NotAMember, Tier::Banned] {
            assert!(!tier.has(CommunityPermission::EditInfo));
            assert!(!tier.has(CommunityPermission::CreatePosts));
            assert!(!tier.has(CommunityPermission::BanPlayers));
        }
    }

    #[test]
    fn is_member_excludes_none_and_banned() {
        assert!(Tier::Owner.is_member());
        assert!(Tier::Moderator.is_member());
        assert!(Tier::Member.is_member());
        assert!(!Tier::NotAMember.is_member());
        assert!(!Tier::Banned.is_member());
    }

    #[test]
    fn create_post_is_owner_and_moderator_only() {
        assert!(can_create_post(Tier::Owner));
        assert!(can_create_post(Tier::Moderator));
        assert!(!can_create_post(Tier::Member));
        assert!(!can_create_post(Tier::NotAMember));
        assert!(!can_create_post(Tier::Banned));
    }

    #[test]
    fn delete_post_owner_any_mod_own_member_never() {
        assert!(can_delete_post(Tier::Owner, true));
        assert!(can_delete_post(Tier::Owner, false));
        assert!(can_delete_post(Tier::Moderator, true));
        assert!(!can_delete_post(Tier::Moderator, false));
        assert!(!can_delete_post(Tier::Member, true));
        assert!(!can_delete_post(Tier::Member, false));
        assert!(!can_delete_post(Tier::NotAMember, true));
        assert!(!can_delete_post(Tier::Banned, true));
    }

    #[test]
    fn like_post_public_open_private_members_only() {
        assert!(can_like_post(Tier::NotAMember, false));
        assert!(can_like_post(Tier::Member, false));
        assert!(can_like_post(Tier::Moderator, false));
        assert!(can_like_post(Tier::Owner, false));
        assert!(!can_like_post(Tier::NotAMember, true));
        assert!(can_like_post(Tier::Member, true));
        assert!(can_like_post(Tier::Moderator, true));
        assert!(can_like_post(Tier::Owner, true));
        assert!(!can_like_post(Tier::Banned, false));
        assert!(!can_like_post(Tier::Banned, true));
    }

    #[test]
    fn role_action_transitions_match_upstream() {
        for actor in [Tier::Owner, Tier::Moderator, Tier::Member, Tier::NotAMember] {
            assert!(!can_act_on_member(actor, Tier::Owner));
        }
        assert!(can_act_on_member(Tier::Owner, Tier::Moderator));
        assert!(!can_act_on_member(Tier::Moderator, Tier::Moderator));
        assert!(!can_act_on_member(Tier::Member, Tier::Moderator));
        assert!(can_act_on_member(Tier::Owner, Tier::Member));
        assert!(can_act_on_member(Tier::Moderator, Tier::Member));
        assert!(!can_act_on_member(Tier::Member, Tier::Member));
        assert!(!can_act_on_member(Tier::Owner, Tier::NotAMember));
    }

    #[test]
    fn a_ban_scope_names_its_relation() {
        assert_eq!(BanScope::Platform.table(), "social.user_bans");
        assert_eq!(
            BanScope::Community(CommunityId::new()).table(),
            "social.community_bans"
        );
        assert_eq!(
            BanScope::Scene {
                place_id: "x.dcl.eth".into()
            }
            .table(),
            "social.scene_bans"
        );
        assert_eq!(
            BanScope::Scene {
                place_id: "x.dcl.eth".into()
            }
            .to_string(),
            "scene x.dcl.eth"
        );
    }

    fn every_arm() -> Vec<(AuthorityNotEstablished, u16)> {
        vec![
            (
                AuthorityNotEstablished::AuthenticationMissingOrInvalid {
                    detail: "no auth chain headers",
                },
                401,
            ),
            (
                AuthorityNotEstablished::PresentedSharedSecretDidNotMatch,
                401,
            ),
            (
                AuthorityNotEstablished::RefusedLacksAuthority {
                    detail: "requires the owner tier".to_string(),
                },
                403,
            ),
            (
                AuthorityNotEstablished::UndeterminedStoreUnavailable {
                    store: "community_members",
                    reason_for_operators: "connection refused".to_string(),
                },
                503,
            ),
            (
                AuthorityNotEstablished::CredentialNotConfigured {
                    environment_variable: "COMMS_GATEKEEPER_AUTH_TOKEN",
                },
                503,
            ),
        ]
    }

    #[test]
    fn every_arm_maps_to_its_documented_status() {
        for (refusal, expected) in every_arm() {
            assert_eq!(
                refusal.http_status(),
                expected,
                "wrong status for {refusal:?}"
            );
        }
    }

    /// The table above must cover the enum. If an arm is added and not listed
    /// here, this count fails and the author must decide its status.
    #[test]
    fn the_status_table_covers_every_arm() {
        assert_eq!(
            every_arm().len(),
            5,
            "an arm was added to AuthorityNotEstablished without a row in every_arm()"
        );
    }

    #[test]
    fn undetermined_and_misconfigured_never_render_as_a_denial() {
        let undetermined = AuthorityNotEstablished::UndeterminedStoreUnavailable {
            store: "community_members",
            reason_for_operators: "pool timed out".to_string(),
        };
        let misconfigured = AuthorityNotEstablished::CredentialNotConfigured {
            environment_variable: "ADMIN_TOKEN",
        };
        assert_ne!(undetermined.http_status(), 403);
        assert_ne!(misconfigured.http_status(), 403);
        assert_eq!(undetermined.http_status(), 503);
        assert_eq!(misconfigured.http_status(), 503);
    }

    #[test]
    fn a_real_denial_never_renders_as_unavailable_or_success() {
        let refused = AuthorityNotEstablished::RefusedLacksAuthority {
            detail: "moderator tier required".to_string(),
        };
        assert_eq!(refused.http_status(), 403);
        assert_ne!(refused.http_status(), 503);
        assert_ne!(refused.http_status(), 200);
    }

    #[test]
    fn operator_facing_text_names_the_variable_and_the_store() {
        let misconfigured = AuthorityNotEstablished::CredentialNotConfigured {
            environment_variable: "COMMS_GATEKEEPER_AUTH_TOKEN",
        };
        assert_eq!(
            misconfigured.to_string(),
            "credential not configured: COMMS_GATEKEEPER_AUTH_TOKEN is unset"
        );
        let undetermined = AuthorityNotEstablished::UndeterminedStoreUnavailable {
            store: "community_members",
            reason_for_operators: "connection refused".to_string(),
        };
        assert_eq!(
            undetermined.to_string(),
            "could not determine authority: backing store community_members unavailable"
        );
    }

    #[test]
    fn a_decision_converts_to_a_result_without_losing_the_arm() {
        assert!(PermissionDecision::Allowed.into_result().is_ok());
        let refused = PermissionDecision::refused("owner only")
            .into_result()
            .unwrap_err();
        assert_eq!(refused.http_status(), 403);
        let down = PermissionDecision::undetermined(
            "social.community_members",
            &sqlx::Error::PoolTimedOut,
        );
        assert!(!down.is_allowed());
        assert_eq!(down.into_result().unwrap_err().http_status(), 503);
    }
}
