import { beforeEach, describe, expect, it } from "vitest";
import { heroEventCards, parseHomeContent, withLiveEvents, type HomeContent } from "../landings/home";
import { eventDetailToMarkdown, whatsOnToMarkdown } from "../../agent/markdown";
import { eventOccurrence, effectiveStartAt, formatEventTime, formatEventWhen, toUpcomingCard, type Event } from "./events";

describe("when a recurring event has a series end months after its next occurrence", () => {
  let event: Event;
  let content: HomeContent;
  beforeEach(() => {
    content = parseHomeContent({ hero: { kicker: "", title: "", subtitle: "", downloads: null, downloadsLabel: "", cta: { desktop: { label: "", href: "/" }, epic: { label: "", href: "/" } }, platforms: [] }, rails: [{ id: "events", kind: "events", title: "", items: [] }], comeHangOut: { title: "", downloads: null, downloadsLabel: "" } });
    event = { id: "series", name: "Weekly fixture", image: null, image_vertical: null, description: null, start_at: "2099-01-01T18:00:00Z", finish_at: "2099-12-31T20:00:00Z", next_start_at: "2099-06-01T18:00:00Z", next_finish_at: null, duration: 7200000, all_day: false, x: 0, y: 0, position: null, coordinates: null, url: null, user_name: null, scene_name: null, estate_name: null, live: false, highlighted: false, trending: false, recurrent: true, recurrent_frequency: "WEEKLY", total_attendees: 0, place_id: null };
  });
  it("should export only the duration of the next occurrence", () => {
    expect(eventOccurrence(event)).toEqual({ start: "2099-06-01T18:00:00Z", finish: "2099-06-01T20:00:00.000Z" });
  });
  it("should use the same occurrence in upcoming cards and both agent markdown surfaces", () => {
    expect(effectiveStartAt(event)).toBe(event.next_start_at);
    expect(toUpcomingCard(event).time).toBe(formatEventTime(event.next_start_at));
    expect(heroEventCards(withLiveEvents(content, [event]))?.[0].when).toBe(formatEventTime(event.next_start_at));
    expect(eventDetailToMarkdown(event)).toContain(formatEventWhen(event.next_start_at));
    expect(whatsOnToMarkdown({ upcoming: [event] })).toContain(formatEventWhen(event.next_start_at));
  });
  describe("and a precise next finish is available", () => {
    beforeEach(() => { event.next_finish_at = "2099-06-01T19:30:00Z"; });
    it("should prefer the occurrence finish over calculated duration", () => {
      expect(eventOccurrence(event).finish).toBe("2099-06-01T19:30:00Z");
    });
  });
  describe("and legacy data omits occurrence duration", () => {
    beforeEach(() => { event.duration = null; });
    it("should omit an end rather than stretching a calendar entry across the series", () => {
      expect(eventOccurrence(event).finish).toBeUndefined();
    });
  });
  describe("and the event is not recurrent", () => {
    beforeEach(() => { event.recurrent = false; event.duration = null; event.start_at = "2099-06-01T18:00:00Z"; event.finish_at = "2099-06-01T20:00:00Z"; });
    it("should preserve the legacy finish fallback for a single event", () => {
      expect(eventOccurrence(event).finish).toBe("2099-06-01T20:00:00.000Z");
    });
  });
});
