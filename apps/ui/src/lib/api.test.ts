import { describe, expect, it, vi } from "vitest";
import { ApiError, KEY_STORAGE, api, gatewayFetch, onForbidden, onUnauthorized, setGatewayKey } from "./api";

function mockFetch(status: number, body: unknown) {
  const fetch = vi.fn<typeof globalThis.fetch>(
    async () =>
      new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json" },
      }),
  );
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

const headersOf = (fetch: ReturnType<typeof mockFetch>) => new Headers(fetch.mock.calls[0][1]?.headers);

describe("api client", () => {
  it("sends no Authorization header without a stored key", async () => {
    const fetch = mockFetch(200, { providers: [] });
    await api.providers();
    expect(fetch.mock.calls[0][0]).toBe("/ui/api/providers");
    expect(headersOf(fetch).has("authorization")).toBe(false);
  });

  it("sends the stored gateway key as a bearer token", async () => {
    setGatewayKey("sk-test");
    expect(localStorage.getItem(KEY_STORAGE)).toBe("sk-test");
    const fetch = mockFetch(200, { object: "list", data: [] });
    await api.models();
    expect(headersOf(fetch).get("authorization")).toBe("Bearer sk-test");
  });

  it("turns a 401 into an ApiError and notifies listeners", async () => {
    mockFetch(401, { error: { message: "key required", type: "authentication_error" } });
    const listener = vi.fn();
    const off = onUnauthorized(listener);
    const error = await api.overview().catch((e: unknown) => e);
    off();
    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 401, message: "key required", type: "authentication_error" });
    expect(listener).toHaveBeenCalledOnce();
  });

  it("does not report other failures as unauthorized", async () => {
    mockFetch(404, { error: { message: "evicted" } });
    const listener = vi.fn();
    const off = onUnauthorized(listener);
    await expect(api.request("r_1")).rejects.toMatchObject({ status: 404, message: "evicted" });
    off();
    expect(listener).not.toHaveBeenCalled();
  });
});

it.each([401, 403])("keeps upstream %s errors out of gateway sign-in", async (status) => {
  mockFetch(status, { error: { message: "provider key refused" } });
  const listener = vi.fn();
  const offUnauthorized = onUnauthorized(listener);
  const offForbidden = onForbidden(listener);
  await expect(gatewayFetch("/v1/chat/completions")).rejects.toMatchObject({ status });
  offUnauthorized();
  offForbidden();
  expect(listener).not.toHaveBeenCalled();
});
