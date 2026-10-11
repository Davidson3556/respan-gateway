import { SearchX } from "lucide-react";
import { useMemo, useState } from "react";
import { Outlet, useParams, useSearchParams } from "react-router";
import { ExportMenu } from "@/components/export-menu";
import { EmptyState } from "@/components/empty-state";
import { NoRequests } from "@/components/no-requests";
import { PageHeader } from "@/components/page-header";
import { RequestFilters } from "@/components/request-filters";
import { RequestsTable } from "@/components/requests-table";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { useLiveMode, useRequests } from "@/hooks/use-live-requests";
import { describeError } from "@/lib/query-client";
import { ANY, distinct, filterRequests, outcomeOf, type RequestFilter } from "@/lib/requests";

const PAGE = 100;

export function RequestsPage() {
  const { data, isPending, isError, error } = useRequests();
  const mode = useLiveMode();
  const { id } = useParams();
  const [params, setParams] = useSearchParams();
  const [shown, setShown] = useState(PAGE);
  // The filter lives in the URL, so a filtered view can be shared and survives a reload.
  const filter: RequestFilter = {
    status: params.get("status") ?? ANY,
    kind: params.get("kind") ?? ANY,
    provider: params.get("provider") ?? ANY,
    model: params.get("model") ?? ANY,
    q: params.get("q") ?? "",
  };

  const set = (name: keyof RequestFilter, value: string) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value && value !== ANY) next.set(name, value);
        else next.delete(name);
        return next;
      },
      { replace: true },
    );

  const all = useMemo(() => data ?? [], [data]);
  const rows = filterRequests(all, filter);
  const options = useMemo(
    () => ({
      kinds: distinct(all.map((r) => r.kind)),
      providers: distinct(all.map((r) => r.provider)),
      models: distinct(all.map((r) => r.model)),
    }),
    [all],
  );
  // How many each status pill would show, given the other filters.
  const counts: Record<string, number> = { ok: 0, error: 0, pending: 0 };
  const others = filterRequests(all, { ...filter, status: ANY });
  for (const r of others) counts[outcomeOf(r)]++;
  counts[ANY] = others.length;

  return (
    <div className="grid grid-cols-1 gap-6">
      <PageHeader
        title="Requests"
        description={
          mode === "live"
            ? "Updating live as requests start and finish."
            : "The most recent requests through the gateway."
        }
        actions={<ExportMenu filter={filter} />}
      />

      {isError && !data ? (
        <div
          role="alert"
          className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm"
        >
          <p className="font-medium">Couldn't load requests</p>
          <p className="text-muted-foreground">{describeError(error)}</p>
        </div>
      ) : data?.length === 0 ? (
        <NoRequests />
      ) : (
        <Card className="gap-0 overflow-hidden py-0">
          <RequestFilters
            filter={filter}
            onChange={set}
            onClear={() => setParams({}, { replace: true })}
            counts={counts}
            options={options}
            shown={rows.length}
            total={all.length}
          />
          {!isPending && rows.length === 0 ? (
            <EmptyState icon={SearchX} title="No requests match" className="m-4">
              <p>Try a broader search or clear the filters.</p>
            </EmptyState>
          ) : (
            <RequestsTable requests={rows.slice(0, shown)} loading={isPending} selectedId={id} />
          )}
          {rows.length > shown && (
            <div className="border-t p-2 text-center">
              <Button variant="ghost" size="sm" onClick={() => setShown((n) => n + PAGE)}>
                Show {Math.min(PAGE, rows.length - shown)} more
              </Button>
            </div>
          )}
        </Card>
      )}
      <Outlet />
    </div>
  );
}
