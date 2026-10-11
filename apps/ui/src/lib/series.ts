import type { RequestRecord } from "@/lib/api";
import { outcomeOf } from "@/lib/requests";

/** What happened in one minute of traffic. */
export interface Bucket {
  requests: number;
  errors: number;
  /** Mean duration of the requests that finished; 0 when none did. */
  latencyMs: number;
}

/** Folds requests into per-minute buckets over the last `minutes`, oldest first. */
export function bucketByMinute(records: RequestRecord[], minutes: number, now: number): Bucket[] {
  const sums = Array.from({ length: minutes }, () => ({ requests: 0, errors: 0, latency: 0, timed: 0 }));
  const end = Math.floor(now / 60_000);
  for (const r of records) {
    const age = end - Math.floor(new Date(r.started_at).getTime() / 60_000);
    if (age < 0 || age >= minutes) continue;
    const b = sums[minutes - 1 - age];
    b.requests += 1;
    if (outcomeOf(r) === "error") b.errors += 1;
    if (r.duration_ms !== null) {
      b.latency += r.duration_ms;
      b.timed += 1;
    }
  }
  return sums.map((b) => ({
    requests: b.requests,
    errors: b.errors,
    latencyMs: b.timed ? b.latency / b.timed : 0,
  }));
}
