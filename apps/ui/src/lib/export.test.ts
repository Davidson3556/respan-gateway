import { describe, expect, it, vi } from "vitest";
import { setGatewayKey } from "@/lib/api";
import { downloadExport, exportPath } from "./export";

describe("exportPath", () => {
  it("carries only the filters that are set", () => {
    const filter = { status: "error", kind: "chat", provider: "all", model: "claude", client: "ci", q: "ignored" };
    expect(exportPath("csv", filter)).toBe(
      "/ui/api/requests/export?format=csv&kind=chat&status=error&model=claude&client=ci",
    );
  });

  it("asks for everything with no filters, and passes a since time", () => {
    expect(exportPath("jsonl", {})).toBe("/ui/api/requests/export?format=jsonl");
    expect(exportPath("jsonl", { status: "ok" }, "2026-01-01")).toBe(
      "/ui/api/requests/export?format=jsonl&status=ok&since=2026-01-01",
    );
  });
});

describe("downloadExport", () => {
  it("fetches with the gateway key and saves the body as a file", async () => {
    setGatewayKey("sk-gw");
    const fetchMock = vi.fn(async () => new Response("a,b\n1,2\n"));
    vi.stubGlobal("fetch", fetchMock);
    URL.createObjectURL = vi.fn(() => "blob:export");
    URL.revokeObjectURL = vi.fn();
    const clicked: string[] = [];
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (this: HTMLAnchorElement) {
      clicked.push(`${this.download} ${this.href}`);
    });

    await downloadExport("csv", { kind: "chat" });

    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/ui/api/requests/export?format=csv&kind=chat");
    expect(new Headers(init.headers).get("authorization")).toBe("Bearer sk-gw");
    expect(clicked).toEqual(["requests.csv blob:export"]);
    expect(URL.revokeObjectURL).toHaveBeenCalledWith("blob:export");
    setGatewayKey(null);
  });

  it("throws when the gateway refuses", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ error: { message: "nope" } }), { status: 403 })));
    await expect(downloadExport("jsonl", {})).rejects.toThrow("nope");
  });
});
