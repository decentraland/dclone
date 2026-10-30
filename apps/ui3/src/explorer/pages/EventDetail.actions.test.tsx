import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import EventDetail from "./EventDetail";
import { ChromeNavContext, type ChromeNavigate } from "../../web/frames/chrome-nav";

describe("when a web event links to the RSVP flow", () => {
  let downloads: string[];
  beforeEach(() => {
    downloads = [];
    vi.useFakeTimers();
    vi.stubGlobal("Blob", class { constructor(parts: string[]) { downloads.push(parts.join("")); } });
    vi.stubGlobal("URL", { createObjectURL: vi.fn(() => "blob:calendar"), revokeObjectURL: vi.fn() });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    render(<EventDetail event={{ id: "recurring", title: "Weekly fixture", start: "2099-06-01T18:00:00Z", finish: "2099-06-01T20:00:00Z" }} rsvpHref="/landings/rsvp-event?event=recurring" />);
  });
  afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers(); });
  it("should expose RSVP navigation without claiming to know wallet attendance", () => {
    expect(screen.getByRole("link", { name: "Manage RSVP" })).toHaveAttribute("href", "/landings/rsvp-event?event=recurring");
    expect(screen.queryByRole("button", { name: /INTERESTED/ })).toBeNull();
    expect(screen.getByRole("link", { name: "Manage RSVP" })).not.toHaveAttribute("aria-pressed");
  });
  it("should retain both reminder and plain calendar downloads for one occurrence", () => {
    fireEvent.click(screen.getByRole("button", { name: "REMIND ME" }));
    fireEvent.click(screen.getByRole("button", { name: "Add to calendar" }));
    expect(downloads[0]).toContain("DTSTART:20990601T180000Z\r\nDTEND:20990601T200000Z");
    expect(downloads[0]).toContain("BEGIN:VALARM");
    expect(downloads[1]).not.toContain("BEGIN:VALARM");
  });
});

describe("when a caller supplies actual interested state", () => {
  beforeEach(() => { render(<EventDetail interested interestedCount={12} onToggleInterested={() => {}} />); });
  it("should show the interested toggle and preserve the independent reminder action", () => {
    expect(screen.getByRole("button", { name: /INTERESTED/ })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "REMIND ME" })).toBeTruthy();
  });
});


describe("when RSVP navigation is supplied by the host router bridge", () => {
  let navigate: ReturnType<typeof vi.fn<ChromeNavigate>>;
  let click: MouseEvent;
  beforeEach(() => {
    navigate = vi.fn<ChromeNavigate>();
    render(<ChromeNavContext.Provider value={{ navigate }}><EventDetail rsvpHref="/landings/rsvp-event?event=fixture" /></ChromeNavContext.Provider>);
    click = new MouseEvent("click", { bubbles: true, cancelable: true, button: 0 });
  });
  afterEach(() => vi.restoreAllMocks());
  it("should retain an anchor and route an ordinary click without a document reload", () => {
    screen.getByRole("link", { name: "Manage RSVP" }).dispatchEvent(click);
    expect(navigate).toHaveBeenCalledWith("/landings/rsvp-event?event=fixture");
    expect(click.defaultPrevented).toBe(true);
  });
  describe("and the click uses a browser modifier", () => {
    beforeEach(() => { click = new MouseEvent("click", { bubbles: true, cancelable: true, button: 0, ctrlKey: true }); });
    it("should preserve browser navigation without invoking the client router", () => {
      screen.getByRole("link", { name: "Manage RSVP" }).dispatchEvent(click);
      expect(navigate).not.toHaveBeenCalled();
      expect(click.defaultPrevented).toBe(false);
    });
  });
});
