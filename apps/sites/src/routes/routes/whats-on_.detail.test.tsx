import { renderToString } from "react-dom/server";
import { createMemoryRouter, RouterProvider } from "react-router";
import { beforeEach, describe, expect, it } from "vitest";
import { formatEventWhen, type Event } from "@/data/lib/catalyst/places/events";
import EventDetailRoute from "./whats-on_.$id";

describe("when the public event detail route renders a recurring event", () => {
  let event: Event;
  let html: string;
  beforeEach(() => {
    event = { id: "series", name: "Weekly fixture", image: null, image_vertical: null, description: null, start_at: "2099-01-01T18:00:00Z", finish_at: "2099-12-31T20:00:00Z", next_start_at: "2099-06-01T18:00:00Z", next_finish_at: null, duration: 7200000, all_day: false, x: 0, y: 0, position: null, coordinates: null, url: null, user_name: null, scene_name: null, estate_name: null, live: false, highlighted: false, trending: false, recurrent: true, recurrent_frequency: "WEEKLY", total_attendees: 0, place_id: null };
    const router = createMemoryRouter([{ id: "detail", path: "/whats-on/:id", Component: EventDetailRoute as never }], { initialEntries: ["/whats-on/series"], hydrationData: { loaderData: { detail: { id: "series", sid: "test", event, unavailable: false } } } });
    html = renderToString(<RouterProvider router={router} />);
  });
  it("should show the next occurrence rather than the series start", () => {
    expect(html).toContain(formatEventWhen(event.next_start_at));
    expect(html).not.toContain(formatEventWhen(event.start_at));
  });
  it("should retain reminder downloads alongside an honest RSVP navigation link", () => {
    expect(html).toContain('href="/landings/rsvp-event?event=series"');
    expect(html).toContain("Manage RSVP");
    expect(html).toContain("REMIND ME");
    expect(html).not.toContain('aria-pressed="false"');
  });
});
