import { useNavigate } from "react-router";

import LdEventSchedulePage from "@ui/landings/pages/LdEventSchedulePage";

import {
  fetchSchedules,
  emptyDraft,
  scheduleToDraft,
  type Schedule,
  type ScheduleDraft,
} from "@/data/lib/catalyst/landings/schedules";
import {
  fetchEvents,
  effectiveStartAt,
  toLiveNowCard,
  groupEventsByDay,
  type Event,
  type LiveNowCard,
  type DayEvent,
} from "@/data/lib/catalyst/places/events";
import { type PageDefaults } from "@/core/lib/telemetry/page-loader";
import { pageLoader } from "@/core/lib/telemetry/page-loader";

import ScheduleWizard from "@/features/explorer/event-schedule/ScheduleWizard";

import type { Route } from "./+types/landings.event-schedule";
import type { StoryId } from "@/core/lib/telemetry/story-id";

const STORY: StoryId = "landings/event-schedule";

type ScheduleSource = "live" | "empty" | "error";

const FALLBACK: PageDefaults = {
  variant: "builder",
};

export async function loader({ request }: Route.LoaderArgs) {
  const url = new URL(request.url);
  const mode = url.searchParams.get("mode") === "edit" ? "edit" : "create";

  const editId = url.searchParams.get("id");

  const [{ sid, assignment, wrap }, { schedules, source }, liveEvents, activeEvents] =
    await Promise.all([
      pageLoader(request,  FALLBACK),
      fetchSchedules({ signal: request.signal })
        .then(({ data: live }) => ({
          schedules: live,
          source: (live.length > 0 ? "live" : "empty") as ScheduleSource,
        }))
        .catch(() => ({ schedules: [] as Schedule[], source: "error" as ScheduleSource })),
    fetchEvents({ list: "live", limit: 8 }, { signal: request.signal })
      .then((r) => r.data)
      .catch(() => [] as Event[]),
    fetchEvents({ list: "active", limit: 100 }, { signal: request.signal })
      .then((r) => r.data)
      .catch(() => [] as Event[]),
  ]);
  const now = new Date();
  const sortedActive = [...activeEvents].sort(
    (a, b) =>
      new Date(effectiveStartAt(a) ?? 8.64e15).getTime() -
      new Date(effectiveStartAt(b) ?? 8.64e15).getTime(),
  );
  const liveNow: LiveNowCard[] = liveEvents.map(toLiveNowCard);
  const { allDays, dayLabels } = groupEventsByDay(
    sortedActive,
    new Set(liveEvents.map((e) => e.id)),
    7,
    now,
  );

  const editTarget =
    mode === "edit"
      ? ((editId ? schedules.find((s) => s.id === editId) : schedules[0]) ?? null)
      : null;

  const payload = {
    sid,
    mode,
    source,
    schedules,
    liveNow,
    allDays,
    dayLabels,
    draft:
      mode === "edit" ? (editTarget ? scheduleToDraft(editTarget) : emptyDraft()) : null,
    scheduleId: editTarget?.id ?? null,
    assignment,
  };

  return wrap(payload);
}

type LoaderData = {
  sid: string;
  mode: "create" | "edit";
  source: ScheduleSource;
  schedules: Schedule[];
  liveNow: LiveNowCard[];
  allDays: DayEvent[][];
  dayLabels: string[];
  draft: ScheduleDraft | null;
  scheduleId: string | null;
  assignment: PageDefaults;
};

export default function EventScheduleRoute({ loaderData }: Route.ComponentProps) {
  const d = loaderData as LoaderData;
  const navigate = useNavigate();

  return (
    <LdEventSchedulePage
      mode={d.mode}
      onModeClick={(mode, e) => {
        e.preventDefault();
        navigate(`?mode=${mode}`);
      }}
    >
      <ScheduleWizard
        trackCtx={{
          sid: d.sid,
          story: STORY,
          variant: d.assignment.variant,
        }}
        schedules={d.schedules}
        source={d.source}
        liveNow={d.liveNow}
        allDays={d.allDays}
        dayLabels={d.dayLabels}
        scheduleId={d.scheduleId ?? undefined}
        draft={d.draft ?? undefined}
      />
    </LdEventSchedulePage>
  );
}
