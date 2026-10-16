import { describe, expect, it } from "vitest";
import { pageAt } from "./nav";

describe("pageAt", () => {
  it("names the page a path is on, as the sidebar marks it", () => {
    expect(pageAt("/")?.label).toBe("Overview");
    expect(pageAt("/config")?.label).toBe("Config");
    expect(pageAt("/requests/r_1")?.label).toBe("Requests");
    expect(pageAt("/settings")?.label).toBe("Settings");
    expect(pageAt("/nowhere")).toBeUndefined();
  });
});
