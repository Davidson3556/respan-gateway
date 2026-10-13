// Typed client for the gateway's admin API (/ui/api) and the OpenAI-shaped
// endpoints the playground uses. Types mirror the contract field for field.

export type Kind =
  | "chat"
  | "realtime"
  | "responses"
  | "image"
  | "transcription"
  | "speech"
  | "embedding"
  | "decision"
  | "video"
  | "mcp"
  | "passthrough";

export interface Totals {
  requests: number;
  errors: number;
  in_flight: number;
  input_tokens: number;
  output_tokens: number;
  /** Total cost in US dollars; present when `prices:` is configured. */
  cost_usd?: number;
}

export interface Overview {
  version: string;
  /** null before the gateway has bound its address */
  listen: string | null;
  started_at: string;
  uptime_seconds: number;
  /** RFC 3339 time of the last config reload; null if never reloaded. */
  reloaded_at: string | null;
  auth_required: boolean;
  /** Where the configuration came from. */
  config_source: "file" | "environment" | "proxy" | "inline";
  /** The config file's path when `config_source` is `file`. */
  config_path: string | null;
  totals: Totals;
}

/** What `POST /ui/api/reload` answers. */
export interface Reloaded {
  reloaded_at: string | null;
  /** What only a restart applies, such as a changed `listen`. */
  restart_required: string[];
}

/** A vendor quirk the gateway handles for a provider. */
export interface Quirk {
  name: string;
  description: string;
}

export interface Provider {
  name: string;
  vendor: string | null;
  formats: string[];
  kinds: Kind[];
  base_url: string;
  credentials: "gateway" | "client";
  quirks: Quirk[];
  requests: number;
  errors: number;
  avg_latency_ms: number | null;
}

/** An MCP server the gateway fronts, served to clients at `endpoint`. */
export interface McpServer {
  name: string;
  transport: "http" | "stdio";
  /** Only the origin of an HTTP server's URL, or the program a stdio server runs (never its path, query or arguments). */
  target: string;
  /** Where clients reach it, such as `/mcp/github`. */
  endpoint: string;
  /** Whose credentials reach it: the gateway's, the client's, or none. */
  credentials: "gateway" | "client" | "none";
  /** `idle` before a request, `failing` if every one failed, else `ok`. */
  status: "idle" | "ok" | "failing";
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
  /** Input tokens the provider served from its cache, when it says. */
  cached_input_tokens?: number;
  /** Seconds of audio billed, as a transcription's provider may report instead of tokens. */
  seconds?: number;
}

/** A tool the gateway ran for the model in a tool loop. */
export interface ToolLoopTool {
  /** As the model named it, `<server>__<tool>`. */
  name: string;
  duration_ms: number;
  error: boolean;
  /** The bytes of its answer the model saw. */
  bytes: number;
}

/** One call to the model in a tool loop, and the tools it asked for. */
export interface ToolLoopStep {
  index: number;
  duration_ms: number;
  input_tokens: number;
  output_tokens: number;
  /** Why the model stopped: `tool_use`, `stop`, … */
  finish?: string;
  tools: ToolLoopTool[];
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
  /** The name of the client key the request came with; absent without named client keys. */
  client?: string;
  /** null while the request is in flight */
  status: number | null;
  duration_ms: number | null;
  ttfb_ms: number | null;
  stream: boolean;
  usage: Usage | null;
  /** What it cost in US dollars; absent when the model is unpriced. */
  cost_usd?: number;
  /** The tool loop's steps, in order; absent without the loop. */
  steps?: ToolLoopStep[];
  error: string | null;
}

/** What `GET /ui/api/config` answers: the config with every secret redacted. */
export interface ConfigView {
  source: "file" | "environment" | "proxy" | "inline";
  path: string | null;
  /** Redaction marker for secrets. */
  placeholder: string;
  text: string;
  format: "toml";
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
  /** Where in a posted document the error is, when the gateway says. */
  readonly line: number | null;
  readonly column: number | null;

  constructor(
    status: number,
    message: string,
    type: string | null = null,
    at: { line?: number | null; column?: number | null } = {},
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.type = type;
    this.line = at.line ?? null;
    this.column = at.column ?? null;
  }
}

// --- gateway key ----------------------------------------------------------

type Listener = () => void;
const keyListeners = new Set<Listener>();
const unauthorizedListeners = new Set<Listener>();
const forbiddenListeners = new Set<Listener>();

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

/** Called whenever any request comes back 403: the key is valid but may not use the admin API. */
export function onForbidden(listener: Listener): () => void {
  forbiddenListeners.add(listener);
  return () => forbiddenListeners.delete(listener);
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
  let at: { line?: number | null; column?: number | null } = {};
  try {
    const text = await res.text();
    try {
      const body = JSON.parse(text) as {
        error?: { message?: string; type?: string; line?: number | null; column?: number | null } | string;
      };
      if (typeof body.error === "string") message = body.error;
      else if (body.error?.message) {
        message = body.error.message;
        type = body.error.type ?? null;
        at = body.error;
      }
    } catch {
      if (text.trim()) message = text.trim().slice(0, 300);
    }
  } catch {
    // Body unreadable; keep the status text.
  }
  return new ApiError(res.status, message, type, at);
}

/** Check a candidate before storing it; failures stay in the sign-in form. */
export async function verifyGatewayKey(key: string): Promise<void> {
  const response = await fetch("/ui/api/overview", {
    headers: { authorization: `Bearer ${key}`, accept: "application/json" },
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw await errorFrom(response);
}

/** fetch with the gateway key attached; throws ApiError on a non-2xx answer. */
export async function gatewayFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const res = await fetch(path, { ...init, headers: authHeaders(init.headers) });
  if (res.ok) return res;
  // Upstream authentication failures belong to the request, not gateway sign-in.
  if (path.startsWith("/ui/api/")) {
    if (res.status === 401) unauthorizedListeners.forEach((l) => l());
    if (res.status === 403) forbiddenListeners.forEach((l) => l());
  }
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
  config: (signal?: AbortSignal) => getJson<ConfigView>("/ui/api/config", signal),
  overview: (signal?: AbortSignal) => getJson<Overview>("/ui/api/overview", signal),
  providers: (signal?: AbortSignal) =>
    getJson<{ providers: Provider[] }>("/ui/api/providers", signal).then((r) => r.providers),
  mcp: (signal?: AbortSignal) =>
    getJson<{ servers: McpServer[] }>("/ui/api/mcp", signal).then((r) => r.servers),
  routes: (signal?: AbortSignal) => getJson<Routes>("/ui/api/routes", signal),
  requests: (limit: number, signal?: AbortSignal) =>
    getJson<{ requests: RequestRecord[] }>(`/ui/api/requests?limit=${limit}`, signal).then(
      (r) => r.requests,
    ),
  request: (id: string, signal?: AbortSignal) =>
    getJson<RequestRecord>(`/ui/api/requests/${encodeURIComponent(id)}`, signal),
  reload: (signal?: AbortSignal) => postJson<Reloaded>("/ui/api/reload", {}, signal),
  models: (signal?: AbortSignal) =>
    getJson<{ object: "list"; data: Model[] }>("/v1/models", signal).then((r) => r.data),
};
