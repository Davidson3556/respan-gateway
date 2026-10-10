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
