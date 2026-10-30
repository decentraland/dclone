import type { BlogBlock, BlogPost, BlogPostCard } from "@/core/lib/content/blog";
import type { Place } from "../catalyst/schema";
import { playUrl } from "../catalyst/bases";
import { eventOccurrence, eventCoords, formatEventWhen, type Event } from "../catalyst/places/events";
import type { ShopPageData } from "../shop/pages";
import type { ProfileVM } from "../catalyst/social/profile";
import type { CommunityDetail } from "../catalyst/social/communities";
import { heroEventCards, hotspotCards, ritualCards, type HomeContent } from "../catalyst/landings/home";

export function wantsMarkdown(request: Request): boolean {
  let url: URL;
  try {
    url = new URL(request.url);
  } catch {
    return false;
  }

  const fmt = url.searchParams.get("format");
  if (fmt && fmt.toLowerCase() === "md") return true;

  if (url.pathname.toLowerCase().endsWith(".md")) return true;

  const accept = request.headers.get("accept") ?? "";
  return /(^|[\s,])text\/(x-)?markdown\b/i.test(accept);
}

export function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

type MarkdownResponseInit = {
  status?: number;
  cacheControl?: string;
  headers?: HeadersInit;
};

export function markdownResponse(md: string, opts: MarkdownResponseInit = {}): Response {
  const headers = new Headers(opts.headers);
  headers.set("Content-Type", "text/markdown; charset=utf-8");
  headers.set("x-markdown-tokens", String(estimateTokens(md)));
  const vary = headers.get("Vary");
  headers.set("Vary", vary ? `${vary}, Accept` : "Accept");
  if (opts.cacheControl) headers.set("Cache-Control", opts.cacheControl);
  return new Response(md, { status: opts.status ?? 200, headers });
}

const HEADING = ["", "# ", "## ", "### ", "#### ", "##### ", "###### "] as const;

function heading(level: number, text: string): string {
  const prefix = HEADING[Math.min(Math.max(level, 1), 6)];
  return `${prefix}${clean(text)}`;
}

function bulletList(items: string[]): string {
  return items
    .map((i) => clean(i))
    .filter((i) => i.length > 0)
    .map((i) => `- ${i}`)
    .join("\n");
}

function clean(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}

function joinBlocks(blocks: Array<string | null | undefined>): string {
  return blocks
    .filter((b): b is string => typeof b === "string" && b.trim().length > 0)
    .join("\n\n")
    .trim();
}

type ElementLike = { type?: unknown; props?: { href?: unknown; children?: unknown } };

function isElementLike(node: unknown): node is ElementLike {
  return typeof node === "object" && node !== null && "props" in node;
}

export function reactNodeToMarkdown(node: unknown): string {
  if (node == null || typeof node === "boolean") return "";
  if (typeof node === "string") return node;
  if (typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(reactNodeToMarkdown).join("");

  if (isElementLike(node)) {
    const inner = reactNodeToMarkdown(node.props?.children);
    const type = node.type;
    if (type === "a" && typeof node.props?.href === "string") {
      return `[${inner}](${node.props.href})`;
    }
    if (type === "strong" || type === "b") return `**${inner}**`;
    if (type === "em" || type === "i") return `*${inner}*`;
    return inner;
  }

  return "";
}

function blogBlockToMarkdown(block: BlogBlock): string {
  switch (block.type) {
    case "h2":
      return heading(2, block.content);
    case "h3":
      return heading(3, block.content);
    case "h4":
      return heading(4, block.content);
    case "quote":
      return `> ${clean(block.content)}`;
    case "ul":
      return bulletList(block.items);
    case "p":
      return clean(block.content);
  }
}

export function blogPostToMarkdown(post: BlogPost): string {
  const meta = [post.category?.title, post.publishedDate, post.author?.title]
    .filter((v): v is string => typeof v === "string" && v.trim().length > 0)
    .join(" \u{B7} ");

  return joinBlocks([
    heading(1, post.title),
    post.description ? clean(post.description) : "",
    meta ? `_${meta}_` : "",
    ...(post.body ?? []).map(blogBlockToMarkdown),
  ]);
}

export function blogIndexToMarkdown(posts: BlogPostCard[]): string {
  const list = posts ?? [];
  const entries = list.map((p) =>
    joinBlocks([
      heading(2, p.title),
      p.description ? clean(p.description) : "",
      joinInline([p.category?.title, p.publishedDate]),
      `[Read more](/blog/${p.slug})`,
    ]),
  );

  return joinBlocks([
    heading(1, "Decentraland Blog"),
    `${list.length} ${list.length === 1 ? "post" : "posts"}.`,
    ...entries,
  ]);
}

function joinInline(parts: Array<string | null | undefined>): string {
  const joined = parts
    .filter((v): v is string => typeof v === "string" && v.trim().length > 0)
    .map(clean)
    .join(" \u{B7} ");
  return joined ? `_${joined}_` : "";
}

type AgentLegalBlock =
  | string
  | {
      type?: string;
      id?: string;
      heading?: string;
      content?: unknown;
      items?: unknown[];
    };

type AgentLegalSection = {
  id?: string;
  heading: string;
  body: AgentLegalBlock[];
};

export type AgentLegalDoc = {
  title: string;
  intro?: string;
  sections: AgentLegalSection[];
};

function legalBlockToMarkdown(block: AgentLegalBlock): string {
  if (typeof block === "string") return clean(block);

  const type = block.type;
  if (type === "h3") return heading(3, reactNodeToMarkdown(block.content ?? block.heading));
  if (type === "h4") return heading(4, reactNodeToMarkdown(block.content ?? block.heading));
  if (type === "ul") {
    return bulletList((block.items ?? []).map(reactNodeToMarkdown));
  }
  const text = clean(reactNodeToMarkdown(block.content ?? block.heading ?? ""));
  return text;
}

export function legalDocToMarkdown(doc: AgentLegalDoc): string {
  const sections = (doc.sections ?? []).map((section) =>
    joinBlocks([
      heading(2, section.heading),
      ...(section.body ?? []).map(legalBlockToMarkdown),
    ]),
  );

  return joinBlocks([
    heading(1, doc.title),
    doc.intro ? clean(doc.intro) : "",
    ...sections,
  ]);
}

function placeCreator(place: Place): string {
  return (place.contact_name || place.owner || "Decentraland").trim() || "Decentraland";
}

function placeApproval(place: Place): string | null {
  return typeof place.like_rate === "number"
    ? `${Math.round(place.like_rate * 100)}%`
    : null;
}

export function placeToMarkdown(place: Place): string {
  const facts: Array<[string, string | number | null | undefined]> = [
    ["Type", place.world ? `World${place.world_name ? ` (${place.world_name})` : ""}` : "Genesis City"],
    ["Coordinates", place.base_position],
    ["Parcels", (place.positions ?? []).length || null],
    ["Categories", (place.categories ?? []).length ? place.categories.join(", ") : null],
    ["Creator", placeCreator(place)],
    ["Visitors", place.user_count],
    ["Approval", placeApproval(place)],
    ["Likes", place.likes],
    ["Favorites", place.favorites],
    ["Updated", place.updated_at],
  ];

  const bullets = facts
    .filter(([, v]) => v !== null && v !== undefined && v !== "")
    .map(([k, v]) => `- ${k}: ${clean(String(v))}`)
    .join("\n");

  return joinBlocks([
    heading(1, place.title || "Untitled Place"),
    place.description ? clean(place.description) : "",
    bullets,
  ]);
}

export function placesIndexToMarkdown(list: Place[]): string {
  const places = list ?? [];
  const entries = places.map((p) => {
    const facts = [
      p.base_position ? `Coordinates: ${clean(p.base_position)}` : null,
      p.world ? "World" : null,
      typeof p.user_count === "number" ? `Visitors: ${p.user_count}` : null,
      `Likes: ${p.likes ?? 0}`,
    ]
      .filter((v): v is string => typeof v === "string" && v.length > 0)
      .join(" \u{B7} ");

    return joinBlocks([
      heading(2, p.title || "Untitled Place"),
      p.description ? clean(p.description) : "",
      facts ? `_${facts}_` : "",
      `[View](/places/${encodeURIComponent(p.id)})`,
    ]);
  });

  return joinBlocks([
    heading(1, "Decentraland Places"),
    `${places.length} ${places.length === 1 ? "place" : "places"}.`,
    ...entries,
  ]);
}

function inlineParts(parts: Array<string | null | undefined>): string {
  return parts
    .filter((v): v is string => typeof v === "string" && v.trim().length > 0)
    .map(clean)
    .join(" \u{B7} ");
}

type AgentWhatsOnData = {
  live?: Event[];
  upcoming?: Event[];
  filter?: string;
  search?: string;
};

function eventLine(e: Event): string {
  const meta = inlineParts([
    e.live ? "LIVE" : formatEventWhen(eventOccurrence(e).start ?? null),
    e.recurrent ? (e.recurrent_frequency ? clean(e.recurrent_frequency) : "recurring") : null,
    eventCoords(e),
    e.user_name || e.scene_name || null,
    (e.total_attendees ?? 0) > 0 ? `${e.total_attendees} attending` : null,
  ]);
  const title = clean(e.name || "Untitled event");
  return `- [${title}](/whats-on/${encodeURIComponent(e.id)})${meta ? ` \u{2014} ${meta}` : ""}`;
}

export function whatsOnToMarkdown(data: AgentWhatsOnData): string {
  const live = data.live ?? [];
  const upcoming = data.upcoming ?? [];
  const note = inlineParts([
    data.filter ? `filter: ${data.filter}` : null,
    data.search ? `search: "${data.search}"` : null,
  ]);

  const blocks: string[] = [
    heading(1, "What's On in Decentraland"),
    `${live.length} live now, ${upcoming.length} upcoming.${note ? ` (${note})` : ""}`,
  ];
  if (live.length) blocks.push(heading(2, "Live now"), live.map(eventLine).join("\n"));
  if (upcoming.length) blocks.push(heading(2, "Upcoming"), upcoming.map(eventLine).join("\n"));
  if (!live.length && !upcoming.length) blocks.push("_No events right now. Check back soon._");

  return joinBlocks(blocks);
}

export function eventDetailToMarkdown(e: Event): string {
  const host = e.user_name || e.scene_name || "Decentraland";
  const jump = e.url || playUrl({ position: `${e.x ?? 0},${e.y ?? 0}` });

  const facts: Array<[string, string | number | null | undefined]> = [
    ["When", formatEventWhen(eventOccurrence(e).start ?? null)],
    ["Host", host],
    ["Location", eventCoords(e)],
    ["Recurring", e.recurrent ? (e.recurrent_frequency || "yes") : null],
    ["Attendees", (e.total_attendees ?? 0) > 0 ? e.total_attendees : null],
    ["Live", e.live ? "yes" : null],
  ];
  const bullets = facts
    .filter(([, v]) => v !== null && v !== undefined && v !== "")
    .map(([k, v]) => `- ${k}: ${clean(String(v))}`)
    .join("\n");

  return joinBlocks([
    heading(1, e.name || "Untitled event"),
    e.description ? clean(e.description) : "",
    bullets,
    `[Jump in](${jump})`,
  ]);
}

export function profileToMarkdown(p: ProfileVM): string {
  const facts: Array<[string, string | number | null | undefined]> = [
    ["Address", p.address],
    ["Claimed name", p.hasClaimedName ? "yes" : "no"],
    ["Mutual friends", p.mutualCount > 0 ? p.mutualCount : null],
  ];
  const bullets = facts
    .filter(([, v]) => v !== null && v !== undefined && v !== "")
    .map(([k, v]) => `- ${k}: ${clean(String(v))}`)
    .join("\n");

  const info = (p.info ?? []).filter((f) => f.value);
  const links = (p.links ?? []).filter((l) => l.url);

  const empty = !p.hasClaimedName && !p.bio && info.length === 0 && links.length === 0;
  if (empty) {
    return joinBlocks([
      heading(1, "Decentraland Profile"),
      "_No public Decentraland profile has been set for this address._",
      bullets,
    ]);
  }

  const infoBlock = info.length
    ? joinBlocks([heading(2, "Info"), info.map((f) => `- ${clean(f.label)}: ${clean(f.value)}`).join("\n")])
    : "";
  const linksBlock = links.length
    ? joinBlocks([heading(2, "Links"), links.map((l) => `- [${clean(l.title || l.url)}](${l.url})`).join("\n")])
    : "";

  return joinBlocks([
    heading(1, p.name || "Decentraland Profile"),
    p.bio ? clean(p.bio) : "",
    bullets,
    infoBlock,
    linksBlock,
  ]);
}

export function discoverToMarkdown(content: HomeContent): string {
  const events = heroEventCards(content) ?? [];
  const hotspots = hotspotCards(content) ?? [];
  const rituals = ritualCards(content) ?? [];

  const blocks: string[] = [
    heading(1, content.hero?.title || "Discover Decentraland"),
    content.hero?.subtitle ? clean(content.hero.subtitle) : "",
  ];
  if (events.length)
    blocks.push(
      heading(2, "Events"),
      events.map((e) => `- ${inlineParts([clean(e.title), e.cat, e.when, e.live ? "LIVE" : null])}`).join("\n"),
    );
  if (hotspots.length)
    blocks.push(
      heading(2, "Hotspots"),
      hotspots
        .map((h) => `- ${h.href ? `[${clean(h.title)}](${h.href})` : clean(h.title)}${typeof h.online === "number" ? ` \u{2014} ${h.online} online` : ""}`)
        .join("\n"),
    );
  if (rituals.length)
    blocks.push(
      heading(2, "Rituals"),
      rituals
        .map((r) => `- ${r.href ? `[${clean(r.title)}](${r.href})` : clean(r.title)}${r.day ? ` \u{2014} ${clean(r.day)}` : ""}`)
        .join("\n"),
    );
  return joinBlocks(blocks);
}

type ShopFacts = {
  name?: string;
  category?: string | null;
  rarity?: string | null;
  creator?: string | null;
  network?: string | null;
  isSmart?: boolean;
  utility?: string | null;
  available?: number | null;
  owner?: string;
  issuedId?: string;
  contractAddress?: string;
};

/**
 * A Shop page (ui3/src/shop) from its SSR head and the public facts its loader read. Prices are left
 * out: what an item costs, and in which currency, is the server's purchase offer, which the page shows.
 */
export function shopPageToMarkdown(data: ShopPageData): string {
  const read = (name: string) => data.queries.find(([key]) => key[0] === name)?.[1] as ShopFacts | null | undefined;
  const meta = read("item-meta");
  const token = read("token");
  const collection = read("collection-meta");
  const facts: Array<[string, string | number | null | undefined]> = [
    ["Collection", collection?.name ? `${collection.name}${collection.contractAddress ? ` (${collection.contractAddress})` : ""}` : null],
    ["Creator", meta?.creator ?? collection?.creator],
    ["Category", token?.category ?? meta?.category],
    ["Rarity", token?.rarity ?? meta?.rarity],
    ["Network", token?.network ?? meta?.network],
    ["Smart wearable", meta?.isSmart ? "yes" : null],
    ["Utility", meta?.utility],
    ["Available to mint", meta?.available],
    ["Issue", token?.issuedId ? `#${token.issuedId}` : null],
    ["Owner", token?.owner],
  ];
  const bullets = facts
    .filter(([, v]) => v !== null && v !== undefined && v !== "")
    .map(([k, v]) => `- ${k}: ${clean(String(v))}`)
    .join("\n");
  return joinBlocks([
    heading(1, data.seo.title),
    data.seo.description ? clean(data.seo.description) : "",
    bullets,
    data.seo.canonical ? `Shop page: ${data.seo.canonical}` : "",
  ]);
}

export function communityToMarkdown(detail: CommunityDetail): string {
  const c = detail.community;
  const facts: Array<[string, string | number | null | undefined]> = [
    ["Owner", c.ownerName || c.ownerAddress],
    ["Privacy", c.privacy],
    ["Members", c.membersCount],
  ];
  const bullets = facts
    .filter(([, v]) => v !== null && v !== undefined && v !== "")
    .map(([k, v]) => `- ${k}: ${clean(String(v))}`)
    .join("\n");

  const members = detail.members ?? [];
  const membersBlock = members.length
    ? joinBlocks([
        heading(2, "Members"),
        members
          .slice(0, 50)
          .map((m) => `- ${clean(m.name || m.memberAddress)}${m.role && m.role !== "none" ? ` (${m.role})` : ""}`)
          .join("\n"),
      ])
    : "";
  const events = detail.events ?? [];
  const eventsBlock = events.length
    ? joinBlocks([
        heading(2, "Events"),
        events
          .map((e) => `- ${inlineParts([clean(e.name), e.timeLabel, e.creatorName ? `by ${e.creatorName}` : null])}`)
          .join("\n"),
      ])
    : "";

  return joinBlocks([
    heading(1, c.name || "Community"),
    c.description ? clean(c.description) : "",
    bullets,
    membersBlock,
    eventsBlock,
  ]);
}

export type AgentMarkdownKey =
  | "blogIndex"
  | "blogPost"
  | "legalDoc"
  | "placesIndex"
  | "placeDetail"
  | "whatsOn"
  | "eventDetail"
  | "shopPage"
  | "profileDetail"
  | "discover"
  | "communityDetail";

export type AgentMarkdownHandle = { agentMarkdown: AgentMarkdownKey };
