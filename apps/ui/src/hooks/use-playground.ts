// What each playground tab keeps the same way: the API it speaks, and the
// record of its last call.

import { useCallback, useState } from "react";
import { useRoutes } from "@/hooks/queries";
import type { RequestRecord } from "@/lib/api";
import { servedApis, type ApiOption } from "@/lib/playground-apis";
import { locateCall, type Want } from "@/lib/served";

/** A tab's choice of API among `all`, offering those the gateway serves as `kind`. */
export function useApiChoice<A extends ApiOption>(kind: string, all: A[]) {
  const { data: routes } = useRoutes();
  const apis = servedApis(kind, all, routes?.serve);
  const [chosen, choose] = useState(all[0].id);
  const api = apis.find((a) => a.id === chosen) ?? apis[0];
  return { kind, apis, api, choose };
}

export type ApiChoice = ReturnType<typeof useApiChoice<ApiOption>>;

/** The record of a tab's last call, found once the call ends. */
export function useLastCall() {
  const [record, setRecord] = useState<RequestRecord>();
  /** Runs `call`, then finds its record, as `want` says it. */
  const track = useCallback(async <T,>(want: Omit<Want, "since">, call: () => Promise<T>) => {
    const since = Date.now();
    setRecord(undefined);
    try {
      return await call();
    } finally {
      void locateCall({ ...want, since })
        .then((found) => found && setRecord(found))
        .catch(() => undefined);
    }
  }, []);
  return { record, track };
}
