import { useEffect, useRef } from "react";
import { useNavigate, useSearchParams } from "react-router";

import LdWhatsOnPage, { type LdWhatsOnEventLink } from "@ui/landings/pages/LdWhatsOnPage";
import FreshnessNotice from "@ui/components/FreshnessNotice";
import { olderCopy, type CopyStamp } from "@ui/data/copyAsOf";

import {
  effectiveStartAt,
  fetchEvents,
  groupEventsByDay,
  toLiveNowCard,
  toUpcomingCard,
  type DayEvent,
  type Event,
} from "@/data/lib/catalyst/places/events";
import { pageLoader } from "@/core/lib/telemetry/page-loader";
import { type PageDefaults } from "@/core/lib/telemetry/page-loader";
import { track } from "@/core/lib/telemetry/track";

import type { Route } from "./+types/whats-on";
import type { AgentMarkdownHandle } from "@/data/lib/agent/markdown";
import type { StoryId } from "@/core/lib/telemetry/story-id";

export const handle = { agentMarkdown: "whatsOn" } satisfies AgentMarkdownHandle;

const STORY: StoryId = "admin/whats-on";
const LIVE_LIMIT = 8;
const UPCOMING_LIMIT = 24;
const FETCH_LIMIT = 100;

const FILTERS = [
  { id: "", label: "All" },
  { id: "today", label: "Today" },
  { id: "week", label: "This week" },
  { id: "recurring", label: "Recurring" },
] as const;
type FilterId = (typeof FILTERS)[number]["id"];

function applyFilter(events: Event[], filter: FilterId, now: Date): Event[] {
  if (!filter) return events;
  const dayStart = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const dayMs = 86_400_000;
  return events.filter((e) => {
    if (filter === "recurring") return e.recurrent === true;
    const iso = effectiveStartAt(e);
    if (!iso) return false;
    const t = new Date(iso).getTime();
    if (Number.isNaN(t)) return false;
    if (filter === "today") return t >= dayStart && t < dayStart + dayMs;
    return t >= dayStart && t < dayStart + 7 * dayMs;
  });
}

const FALLBACK: PageDefaults = {
  variant: "live_feed",
};

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const rawFilter = url.searchParams.get("filter")?.trim() ?? "";
  const filter: FilterId = (FILTERS.some((f) => f.id === rawFilter) ? rawFilter : "") as FilterId;
  const search = url.searchParams.get("search")?.trim() ?? "";

  const [{ sid, wrap }, liveRes, activeRes] = await Promise.all([
    pageLoader(request,  FALLBACK),
    fetchEvents({
      list: "live",
      search: search || undefined,
      limit: LIVE_LIMIT,
    }).catch(() => ({ data: [] as Event[], copy: null })),
    fetchEvents({
      list: "active",
      search: search || undefined,
      limit: FETCH_LIMIT,
    }).catch(() => ({ data: [] as Event[], copy: null })),
  ]);
  const live = liveRes.data;
  const active = activeRes.data;
  const copy = { stamp: olderCopy(liveRes.copy, activeRes.copy), now: Date.now() };

  const now = new Date();

  const sorted = [...active].sort((a, b) => {
    const ta = new Date(effectiveStartAt(a) ?? 8.64e15).getTime();
    const tb = new Date(effectiveStartAt(b) ?? 8.64e15).getTime();
    return ta - tb;
  });
  const upcoming = applyFilter(sorted, filter, now).slice(0, UPCOMING_LIMIT);

  const liveIds = new Set(live.map((e) => e.id));
  const { allDays, dayLabels } = groupEventsByDay(sorted, liveIds, 28, now);

  const payload = {
    sid,
    filter,
    search,
    live,
    upcoming,
    allDays,
    dayLabels,
    copy,
  };

  return wrap(payload);
}

export default function WhatsOn({ loaderData }: Route.ComponentProps) {
  const d = loaderData;

  return (
    <WhatsOnFeed
      sid={d.sid}
      filter={d.filter}
      search={d.search}
      live={d.live}
      upcoming={d.upcoming}
      allDays={d.allDays}
      dayLabels={d.dayLabels}
      copy={d.copy}
    />
  );
}

type FeedProps = {
  sid: string;
  filter: FilterId;
  search: string;
  live: Event[];
  upcoming: Event[];
  allDays: DayEvent[][];
  dayLabels: string[];
  /** The events copy's stamp, and the loader's clock. */
  copy: { stamp: CopyStamp | null; now: number };
};

function WhatsOnFeed({
  sid,
  filter,
  search,
  live,
  upcoming,
  allDays,
  dayLabels,
  copy,
}: FeedProps) {
  const [, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  useWhatsOnView(sid, live.length, upcoming.length, filter, search);

  const liveCards = live.map(toLiveNowCard);
  const upcomingCards = upcoming.map(toUpcomingCard);

  const eventLinks: LdWhatsOnEventLink[] = dedupeById([...live, ...upcoming]).map((e) => ({
    id: e.id,
    href: `/whats-on/${encodeURIComponent(e.id)}`,
    name: e.name ?? "Untitled event",
    live: e.live,
  }));

  function updateParam(key: string, value: string) {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value) next.set(key, value);
        else next.delete(key);
        return next;
      },
      { preventScrollReset: true },
    );
  }

  return (
    <LdWhatsOnPage
      liveNow={liveCards}
      upcoming={upcomingCards}
      allDays={allDays}
      dayLabels={dayLabels}
      filters={[...FILTERS]}
      activeFilter={filter}
      onFilterSelect={(id) => updateParam("filter", id)}
      eventLinks={eventLinks}
      onEventLinkClick={(link, e) => {
        e.preventDefault();
        track("lp_event_card_clicked", { event_id: link.id }, { sid, story: STORY });
        navigate(link.href);
      }}
      promoSlot={
        <FreshnessNotice copy={copy.stamp} now={copy.now} />
      }
    />
  );
}

function dedupeById(events: Event[]): Event[] {
  const seen = new Set<string>();
  const out: Event[] = [];
  for (const e of events) {
    if (seen.has(e.id)) continue;
    seen.add(e.id);
    out.push(e);
  }
  return out;
}

function useWhatsOnView(
  sid: string,
  liveCount: number,
  upcomingCount: number,
  filter: string,
  search: string,
) {
  const lastKey = useRef<string | null>(null);
  useEffect(() => {
    const key = `${filter}|${search}`;
    if (lastKey.current === key) return;
    lastKey.current = key;
    track(
      "lp_whatson_viewed",
      {
        live_count: liveCount,
        upcoming_count: upcomingCount,
        filter: filter || null,
        search: search || null,
      },
      { sid, story: STORY },
    );
  }, [sid, liveCount, upcomingCount, filter, search]);
}
