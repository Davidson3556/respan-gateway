import type { Provider, RequestRecord } from "@/lib/api";
import type { ChatApiId } from "@/lib/chat-apis";

/** Who answered a chat call, and what the gateway did on the way. */
export interface Served {
  id: string;
  provider: string | null;
  /** The client API it was translated from, when the provider speaks another. */
  translatedFrom: ChatApiId | null;
  /** Calls to the model, when the gateway ran an agent loop. */
  steps: RequestRecord["steps"];
}

/**
 * Finds the record of the call a chat made: the newest of `records` in the
 * client's format, for this model, that began no earlier than `since` (a
 * little slack allows for clock skew between browser and gateway).
 */
export function findCall(
  records: RequestRecord[],
  { api, model, since }: { api: ChatApiId; model: string; since: number },
): RequestRecord | undefined {
  return records
    .filter(
      (r) =>
        r.format === api &&
        (r.model === null || r.model === model) &&
        Date.parse(r.started_at) >= since - 5000,
    )
    .sort((a, b) => Date.parse(b.started_at) - Date.parse(a.started_at))[0];
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
