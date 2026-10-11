import type { RequestRecord } from "@/lib/api";

/** ok, error or still in flight. */
export type Outcome = "ok" | "error" | "pending";

export function outcomeOf(r: Pick<RequestRecord, "status" | "error">): Outcome {
  if (r.status === null) return "pending";
  return r.status >= 400 || r.error ? "error" : "ok";
}

/** A first request to paste into a terminal, aimed at this gateway. */
export function curlExample(origin: string, withKey: boolean): string {
  return [
    `curl ${origin}/v1/chat/completions \\`,
    ...(withKey ? [`  -H "Authorization: Bearer $GATEWAY_KEY" \\`] : []),
    `  -H "content-type: application/json" \\`,
    `  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'`,
  ].join("\n");
}

export const ANY = "all";

export interface RequestFilter {
  status: string;
  kind: string;
  provider: string;
  model: string;
  /** Free text, matched against the model and the path. */
  q: string;
}

export function filterRequests(rows: RequestRecord[], f: RequestFilter): RequestRecord[] {
  const needle = f.q.trim().toLowerCase();
  return rows.filter(
    (r) =>
      (f.status === ANY || outcomeOf(r) === f.status) &&
      (f.kind === ANY || r.kind === f.kind) &&
      (f.provider === ANY || r.provider === f.provider) &&
      (f.model === ANY || r.model === f.model) &&
      (!needle ||
        (r.model ?? "").toLowerCase().includes(needle) ||
        r.path.toLowerCase().includes(needle)),
  );
}

/** The distinct, sorted values of a field, for a filter's options. */
export function distinct(values: (string | null)[]): string[] {
  return [...new Set(values.filter((v): v is string => !!v))].sort();
}

export interface Timing {
  /** Time until the first byte, then the rest of the response. */
  ttfb: number;
  rest: number;
  total: number;
  /** The first byte's share of the total, 0 to 1. */
  ttfbShare: number;
}

/** Splits a finished request's duration; null while it is in flight. */
export function timingOf(r: Pick<RequestRecord, "duration_ms" | "ttfb_ms">): Timing | null {
  const { duration_ms: total, ttfb_ms } = r;
  if (total == null) return null;
  const ttfb = Math.min(Math.max(ttfb_ms ?? 0, 0), total);
  return { ttfb, rest: total - ttfb, total, ttfbShare: total > 0 ? ttfb / total : 0 };
}

/** Output tokens per second of a streamed answer, measured after its first byte; null if unmeasurable. */
export function tokensPerSecond(r: RequestRecord): number | null {
  const timing = timingOf(r);
  const tokens = r.usage?.output_tokens ?? 0;
  if (!r.stream || !timing || timing.rest <= 0 || tokens <= 0) return null;
  return tokens / (timing.rest / 1000);
}

const quote = (json: unknown) => `'${JSON.stringify(json).replaceAll("'", "'\\''")}'`;

/**
 * A curl that repeats a request, with a minimal body of the right shape; the
 * gateway records no bodies, so the prompt is a stand-in. Null for traffic
 * that isn't a plain POST (WebSockets, polling).
 */
export function reproduceCurl(r: RequestRecord, origin: string, withKey: boolean): string | null {
  const model = r.model ?? "MODEL";
  const bodies: Record<string, unknown> = {
    "openai-chat": { model, messages: [{ role: "user", content: "hi" }], stream: r.stream },
    "anthropic-messages": {
      model,
      max_tokens: 256,
      messages: [{ role: "user", content: "hi" }],
      stream: r.stream,
    },
    "gemini-generate": { contents: [{ role: "user", parts: [{ text: "hi" }] }] },
    "openai-images": { model, prompt: "a lighthouse at dusk" },
    "openai-speech": { model, input: "Hello from the gateway.", voice: "alloy" },
    "openai-embeddings": { model, input: "hello" },
    "openai-videos": { model, prompt: "a paper boat" },
  };
  const lines = [`curl ${origin}${r.path} \\`];
  if (withKey) lines.push(`  -H "Authorization: Bearer $GATEWAY_KEY" \\`);
  if (r.method === "POST" && r.format === "openai-transcriptions") {
    lines.push(`  -F file=@audio.mp3 \\`, `  -F model=${model}`);
  } else if (r.method === "POST" && r.format && r.format in bodies) {
    lines.push(`  -H "content-type: application/json" \\`, `  -d ${quote(bodies[r.format])}`);
  } else {
    return null;
  }
  return lines.join("\n");
}
