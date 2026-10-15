import { api as gateway, type Provider, type RequestRecord } from "@/lib/api";
import type { ChatApiId } from "@/lib/chat-apis";

/** A call to find the record of: in the client format `api`, for `model`, begun at `since`. */
export interface Want {
  api: string;
  model: string;
  since: number;
  /** Its path, where calls of the format go to several, such as a job's polls. */
  path?: string;
}

/** Who answered a chat call, and what the gateway did on the way. */
export interface Served {
  id: string;
  provider: string | null;
  /** The client API it was translated from, when the provider speaks another. */
  translatedFrom: ChatApiId | null;
  /** Calls to the model, when the gateway ran a tool loop. */
  steps: RequestRecord["steps"];
}

/**
 * Finds the record of a call the playground made: the newest of `records` in
 * the client's format (at its path, if given), for this model, that began no
 * earlier than `since` (a little slack allows for clock skew between browser
 * and gateway).
 */
export function findCall(
  records: RequestRecord[],
  { api, model, since, path }: Want,
): RequestRecord | undefined {
  return records
    .filter(
      (r) =>
        r.format === api &&
        (path === undefined || r.path === path) &&
        (r.model === null || r.model === model) &&
        Date.parse(r.started_at) >= since - 5000,
    )
    .sort((a, b) => Date.parse(b.started_at) - Date.parse(a.started_at))[0];
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** The record of the call just made; the gateway finishes it a moment after the call ends. */
export async function locateCall(want: Want): Promise<RequestRecord | undefined> {
  let found: RequestRecord | undefined;
  for (let attempt = 0; attempt < 4; attempt++) {
    if (attempt > 0) await sleep(400);
    found = findCall(await gateway.requests(20), want) ?? found;
    if (found && found.status !== null) break;
  }
  return found;
}

export function servedBy(record: RequestRecord, providers: Provider[] | undefined): Served {
  const provider = providers?.find((p) => p.name === record.provider);
  const translated =
    provider &&
    record.format &&
    provider.formats.length > 0 &&
    !provider.formats.includes(record.format);
  return {
    id: record.id,
    provider: record.provider,
    translatedFrom: translated ? (record.format as ChatApiId) : null,
    steps: record.steps,
  };
}
