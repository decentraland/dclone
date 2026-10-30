import { useEffect, useRef, useState } from "react";
import type { ShouldRevalidateFunctionArgs } from "react-router";
import { Link, useNavigate, useNavigation, useSearchParams } from "react-router";

import ExploreChrome, { EXPLORE_TABS, PUBLIC_WORLD_TABS, type TabId } from "@ui/explorer/frames/ExploreChrome";
import PlaceCard from "@ui/components/PlaceCard";
import SearchField from "@ui/atoms/SearchField";
import FreshnessNotice from "@ui/components/FreshnessNotice";
import type { CopyStamp } from "@ui/data/copyAsOf";
import "@ui/explorer/pages/places.css";

import { mapPlace, type Place, type Category } from "@/data/lib/catalyst/places/index";
import { loadPlaces, loadCategories } from "@/data/lib/catalyst/places/index.server";
import { makeSearchCache } from "@/core/lib/router/client-cache";
import type { AgentMarkdownHandle } from "@/data/lib/agent/markdown";
import { type PageDefaults } from "@/core/lib/telemetry/page-loader";
import { pageLoader } from "@/core/lib/telemetry/page-loader";
import { track } from "@/core/lib/telemetry/track";

import type { Route } from "./+types/places";
import type { StoryId } from "@/core/lib/telemetry/story-id";

const STORY: StoryId = "misc/browse-places";

const PLACES_LIMIT = 60;

export function meta() {
  return [{ title: "Places | Decentraland" }, { name: "description", content: "Discover places in Decentraland, see who is online, and find your next virtual destination." }];
}

export const handle = { agentMarkdown: "placesIndex" } satisfies AgentMarkdownHandle;

const PLACES_TABS = EXPLORE_TABS.filter((tab) => PUBLIC_WORLD_TABS.has(tab.id));
const TAB_ROUTES: Partial<Record<TabId, string>> = {
  events: "/whats-on",
  places: "/places",
  communities: "/social",
  marketplace: "/shop",
  create: "/create",
};

const FALLBACK: PageDefaults = {
  variant: "live-signals",
};

export async function loader({ request }: Route.LoaderArgs) {
  const requestUrl = new URL(request.url);
  const category = requestUrl.searchParams.get("category")?.trim() ?? "";
  const search = requestUrl.searchParams.get("search")?.trim() ?? "";
  const rawOffset = Number.parseInt(requestUrl.searchParams.get("offset") ?? "0", 10);
  const offset = Number.isFinite(rawOffset) && rawOffset > 0 ? rawOffset : 0;

  const [{ sid, wrap }, placesRes, categories] = await Promise.all([
    pageLoader(request,  FALLBACK),
    loadPlaces({
      limit: PLACES_LIMIT,
      offset: offset || undefined,
      categories: category || undefined,
      search: search || undefined,
    }).catch(() => ({ data: [] as Place[], total: 0, copy: null })),
    loadCategories().catch(() => [] as Category[]),
  ]);

  return wrap({
    sid,
    category,
    search,
    offset,
    places: placesRes.data,
    total: placesRes.total,
    categories,
    layoutV2: false,
    copy: { stamp: placesRes.copy ?? null, now: Date.now() },
  });
}

type LoaderData = {
  sid: string;
  category: string;
  search: string;
  offset: number;
  places: Place[];
  total: number;
  categories: Category[];
  layoutV2: boolean;
  copy: CopyView;
};

/** The places copy's stamp, and the time the loader read it (one clock for SSR and hydration). */
type CopyView = { stamp: CopyStamp | null; now: number };

const cache = makeSearchCache<LoaderData>();

export async function clientLoader({
  request,
  serverLoader,
}: Route.ClientLoaderArgs) {
  const key = cache.key(request);
  const hit = cache.get(key);
  if (hit) return hit;
  const fresh = (await serverLoader()) as LoaderData;
  cache.set(key, fresh);
  return fresh;
}

export function shouldRevalidate({
  currentUrl,
  nextUrl,
  defaultShouldRevalidate,
}: ShouldRevalidateFunctionArgs) {
  if (
    currentUrl.pathname === nextUrl.pathname &&
    currentUrl.search === nextUrl.search
  ) {
    return false;
  }
  return defaultShouldRevalidate;
}

export default function Places({ loaderData }: Route.ComponentProps) {
  const d = loaderData;

  return (
    <PlacesExplorer
      sid={d.sid}
      activeCategory={d.category}
      search={d.search}
      offset={d.offset}
      places={d.places}
      total={d.total}
      categories={d.categories}
      layoutV2={d.layoutV2}
      copy={d.copy}
    />
  );
}

type ExplorerProps = {
  sid: string;
  activeCategory: string;
  search: string;
  offset: number;
  places: Place[];
  total: number;
  categories: Category[];
  layoutV2: boolean;
  copy: CopyView;
};

type Accumulated = { key: string; offset: number; rows: Place[] };

function accumulate(
  prev: Accumulated,
  key: string,
  offset: number,
  places: Place[],
): Accumulated {
  if (prev.key !== key || offset === 0) return { key, offset, rows: places };
  if (offset === prev.offset) return { key, offset, rows: places };
  if (offset > prev.offset) {
    return { key, offset, rows: [...prev.rows.slice(0, offset), ...places] };
  }
  return { key, offset, rows: prev.rows.slice(0, offset + places.length) };
}

function PlacesExplorer({
  sid,
  activeCategory,
  search,
  offset,
  places,
  total,
  categories,
  layoutV2,
  copy,
}: ExplorerProps) {
  const [, setSearchParams] = useSearchParams();
  const navigation = useNavigation();
  const navigate = useNavigate();

  const key = `${activeCategory}|${search}`;
  const [acc, setAcc] = useState<Accumulated>(() => ({
    key,
    offset,
    rows: places,
  }));

  useEffect(() => {
    setAcc((prev) => accumulate(prev, key, offset, places));
  }, [key, offset, places]);

  const rows = acc.key === key ? acc.rows : places;

  useExposure(sid, rows.length, activeCategory, search);

  const cards = rows.map(mapPlace);
  const canLoadMore = rows.length < total && places.length === PLACES_LIMIT;
  const loadingMore = navigation.state === "loading";

  function updateParam(paramKey: string, value: string) {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value) next.set(paramKey, value);
        else next.delete(paramKey);
        if (paramKey !== "offset") next.delete("offset");
        return next;
      },
      { preventScrollReset: true },
    );
  }

  function onCardClick(placeId: string) {
    track("place_card_clicked", { place_id: placeId }, { sid, story: STORY });
  }

  return (
    <ExploreChrome
      active="places"
      tabs={PLACES_TABS}
      onTab={(tab) => { const route = TAB_ROUTES[tab]; if (route) navigate(route); }}
      onClose={() => navigate("/play")}
    >
      <div className="pl" data-places-layout={layoutV2 ? "v2" : "v1"}>
        <div className="pl__head">
          <h1 className="pl__title">Places</h1>
          <div className="pl__actions">
            <div className="pl__search">
              <SearchField
                placeholder="Search"
                value={search}
                onChange={(v: string) => updateParam("search", v)}
              />
            </div>
          </div>
        </div>

        <FreshnessNotice copy={copy.stamp} now={copy.now} />

        <div className="pl__cats" role="tablist" aria-label="Place categories">
          <CategoryPill
            label="All"
            active={activeCategory === ""}
            onSelect={() => updateParam("category", "")}
          />
          {categories.map((c) => (
            <CategoryPill
              key={c.name}
              label={c.i18n?.en ?? c.name}
              active={activeCategory === c.name}
              onSelect={() => updateParam("category", c.name)}
            />
          ))}
        </div>

        {cards.length > 0 ? (
          <div className="pl__grid">
            {cards.map((card, i) => (
              <Link
                key={card.id}
                to={`/places/${encodeURIComponent(card.id)}`}
                onClick={() => onCardClick(card.id)}
                prefetch="intent"
                style={{ display: "block", textDecoration: "none", color: "inherit" }}
              >
                <PlaceCard
                  // The first two cards are visible on the mobile entry viewport.
                  priority={i < 2}
                  presentational
                  title={card.title}
                  image={card.image}
                  players={card.players}
                  rating={card.rating}
                  coords={card.coords}
                  live={card.live ? 1 : undefined}
                  featured={card.featured}
                  creator={card.creator}
                  hue={(i * 47) % 360}
                  to={undefined}
                />
              </Link>
            ))}
          </div>
        ) : (
          <p style={{ color: "rgba(255,255,255,0.7)", padding: "8px 0" }}>
            No places found.
          </p>
        )}

        {canLoadMore && (
          <div style={{ display: "flex", justifyContent: "center", padding: "24px 0" }}>
            <button
              type="button"
              className="pl__pill"
              disabled={loadingMore}
              onClick={() => updateParam("offset", String(rows.length))}
            >
              {loadingMore ? "Loading\u{2026}" : `Load more (${rows.length} of ${total})`}
            </button>
          </div>
        )}
      </div>
    </ExploreChrome>
  );
}

type CategoryPillProps = {
  label: string;
  active: boolean;
  onSelect: () => void;
};

function CategoryPill({ label, active, onSelect }: CategoryPillProps) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      className={"pl__pill" + (active ? " is-active" : "")}
      onClick={onSelect}
    >
      {label}
    </button>
  );
}

function useExposure(
  sid: string,
  count: number,
  category: string,
  search: string,
) {
  const lastKey = useRef<string | null>(null);
  useEffect(() => {
    const key = `${category}|${search}`;
    if (lastKey.current === key) return;
    lastKey.current = key;
    track(
      "place_list_viewed",
      { count, category: category || null, search: search || null },
      { sid, story: STORY },
    );
  }, [sid, count, category, search]);
}
