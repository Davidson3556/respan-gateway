// Typed client for the gateway's admin API (/ui/api) and the OpenAI-shaped
// endpoints the playground uses. Types mirror the contract field for field.

export type Kind =
  | "chat"
  | "realtime"
  | "image"
  | "transcription"
  | "speech"
  | "embedding"
  | "video"
  | "passthrough";

export interface Totals {
  requests: number;
  errors: number;
  in_flight: number;
  input_tokens: number;
  output_tokens: number;
}

export interface Overview {
  version: string;
  listen: string;
  started_at: string;
  uptime_seconds: number;
  auth_required: boolean;
  config_source: "gateway.yaml" | "environment" | "proxy";
  totals: Totals;
}

export interface Provider {
  name: string;
  vendor: string | null;
  formats: string[];
  kinds: Kind[];
  base_url: string;
  credentials: "gateway" | "client";
  requests: number;
  errors: number;
  avg_latency_ms: number | null;
}

export interface ServedFormat {
  format: string;
  kind: Kind;
  endpoints: string[];
}

export interface Routes {
  serve: ServedFormat[];
  routes: { prefix: string; provider: string }[];
  defaults: { kind: Kind; provider: string }[];
}

export interface Usage {
  input_tokens: number;
  output_tokens: number;
  /** Seconds of audio billed, as a transcription's provider may report instead of tokens. */
  seconds?: number;
}

export interface RequestRecord {
  id: string;
  started_at: string;
  method: string;
  path: string;
  format: string | null;
  kind: Kind | null;
  model: string | null;
  provider: string | null;
  /** null while the request is in flight */
  status: number | null;
  duration_ms: number | null;
  ttfb_ms: number | null;
  stream: boolean;
  usage: Usage | null;
  error: string | null;
}

export interface Model {
  id: string;
  object: "model";
  owned_by: string;
  /** Gateway extension; may be absent. */
  kind?: Kind;
}

export const KEY_STORAGE = "respan.gatewayKey";

export class ApiError extends Error {
  readonly status: number;
  readonly type: string | null;

  constructor(status: number, message: string, type: string | null = null) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.type = type;
  }
}

// --- gateway key ----------------------------------------------------------

type Listener = () => void;
const keyListeners = new Set<Listener>();
const unauthorizedListeners = new Set<Listener>();

export function getGatewayKey(): string | null {
  try {
    return localStorage.getItem(KEY_STORAGE) || null;
  } catch {
    return null;
  }
}

export function setGatewayKey(key: string | null): void {
  try {
    if (key) localStorage.setItem(KEY_STORAGE, key);
    else localStorage.removeItem(KEY_STORAGE);
  } catch {
    // Storage unavailable (private mode); the key just won't persist.
  }
  keyListeners.forEach((l) => l());
}

export function subscribeGatewayKey(listener: Listener): () => void {
  keyListeners.add(listener);
  return () => keyListeners.delete(listener);
}

/** Called whenever any request comes back 401, so the UI can ask for a key. */
export function onUnauthorized(listener: Listener): () => void {
  unauthorizedListeners.add(listener);
  return () => unauthorizedListeners.delete(listener);
}

// --- requests -------------------------------------------------------------

export function authHeaders(init?: HeadersInit): Headers {
  const headers = new Headers(init);
  const key = getGatewayKey();
  if (key && !headers.has("authorization")) headers.set("authorization", `Bearer ${key}`);
  return headers;
}

async function errorFrom(res: Response): Promise<ApiError> {
  let message = res.statusText || `HTTP ${res.status}`;
  let type: string | null = null;
  try {
    const text = await res.text();
    try {
      const body = JSON.parse(text) as { error?: { message?: string; type?: string } | string };
      if (typeof body.error === "string") message = body.error;
      else if (body.error?.message) {
        message = body.error.message;
        type = body.error.type ?? null;
      }
    } catch {
      if (text.trim()) message = text.trim().slice(0, 300);
    }
  } catch {
    // Body unreadable; keep the status text.
  }
  return new ApiError(res.status, message, type);
}

/** fetch with the gateway key attached; throws ApiError on a non-2xx answer. */
export async function gatewayFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const res = await fetch(path, { ...init, headers: authHeaders(init.headers) });
  if (res.ok) return res;
  if (res.status === 401) unauthorizedListeners.forEach((l) => l());
  throw await errorFrom(res);
}

export async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await gatewayFetch(path, { signal, headers: { accept: "application/json" } });
  return (await res.json()) as T;
}

export async function postJson<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
  const res = await gatewayFetch(path, {
    method: "POST",
    signal,
    headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify(body),
  });
  return (await res.json()) as T;
}

export const api = {
  overview: (signal?: AbortSignal) => getJson<Overview>("/ui/api/overview", signal),
  providers: (signal?: AbortSignal) =>
    getJson<{ providers: Provider[] }>("/ui/api/providers", signal).then((r) => r.providers),
  routes: (signal?: AbortSignal) => getJson<Routes>("/ui/api/routes", signal),
  requests: (limit: number, signal?: AbortSignal) =>
    getJson<{ requests: RequestRecord[] }>(`/ui/api/requests?limit=${limit}`, signal).then(
      (r) => r.requests,
    ),
  request: (id: string, signal?: AbortSignal) =>
    getJson<RequestRecord>(`/ui/api/requests/${encodeURIComponent(id)}`, signal),
  models: (signal?: AbortSignal) =>
    getJson<{ object: "list"; data: Model[] }>("/v1/models", signal).then((r) => r.data),
};
