import { Search, SearchX, X } from "lucide-react";
import { useMemo, useState } from "react";
import { Outlet, useParams, useSearchParams } from "react-router";
import { EmptyState } from "@/components/empty-state";
import { NoRequests } from "@/components/no-requests";
import { PageHeader } from "@/components/page-header";
import { RequestsTable } from "@/components/requests-table";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useLiveMode, useRequests } from "@/hooks/use-live-requests";
import { formatCount } from "@/lib/format";
import { outcomeOf } from "@/lib/requests";

const ALL = "all";
const PAGE = 100;

function FilterSelect({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: { value: string; label: string }[];
  onChange: (value: string) => void;
}) {
  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger size="sm" className="w-[9.5rem]" aria-label={label}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={ALL}>All {label.toLowerCase()}</SelectItem>
        {options.map((o) => (
          <SelectItem key={o.value} value={o.value}>
            {o.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

const distinct = (values: (string | null)[]) =>
  [...new Set(values.filter((v): v is string => !!v))].sort();

export function RequestsPage() {
  const { data, isPending } = useRequests();
  const mode = useLiveMode();
  const { id } = useParams();
  const [params, setParams] = useSearchParams();
  const [shown, setShown] = useState(PAGE);
  const status = params.get("status") ?? ALL;
  const kind = params.get("kind") ?? ALL;
  const provider = params.get("provider") ?? ALL;
  const q = params.get("q") ?? "";
  const filtered = status !== ALL || kind !== ALL || provider !== ALL || q !== "";

  const set = (name: string, value: string) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value && value !== ALL) next.set(name, value);
        else next.delete(name);
        return next;
      },
      { replace: true },
    );

  const kinds = useMemo(() => distinct((data ?? []).map((r) => r.kind)), [data]);
  const providers = useMemo(() => distinct((data ?? []).map((r) => r.provider)), [data]);

  const rows = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return (data ?? []).filter(
      (r) =>
        (status === ALL || outcomeOf(r) === status) &&
        (kind === ALL || r.kind === kind) &&
        (provider === ALL || r.provider === provider) &&
        (!needle ||
          (r.model ?? "").toLowerCase().includes(needle) ||
          r.path.toLowerCase().includes(needle)),
    );
  }, [data, status, kind, provider, q]);

  return (
    <div className="grid gap-6">
      <PageHeader
        title="Requests"
        description={
          mode === "live"
            ? "Updating live as requests start and finish."
            : "The most recent requests through the gateway."
        }
      />

      {data?.length === 0 ? (
        <NoRequests />
      ) : (
        <Card className="gap-0 overflow-hidden py-0 shadow-none">
          <div className="flex flex-wrap items-center gap-2 border-b p-3">
            <div className="relative min-w-48 flex-1 sm:max-w-xs">
              <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={q}
                onChange={(e) => set("q", e.target.value)}
                placeholder="Search model or path"
                aria-label="Search model or path"
                className="h-8 pl-8"
              />
            </div>
            <FilterSelect
              label="Statuses"
              value={status}
              onChange={(v) => set("status", v)}
              options={[
                { value: "ok", label: "Succeeded" },
                { value: "error", label: "Failed" },
                { value: "pending", label: "In flight" },
              ]}
            />
            <FilterSelect
              label="Kinds"
              value={kind}
              onChange={(v) => set("kind", v)}
              options={kinds.map((k) => ({ value: k, label: k }))}
            />
            <FilterSelect
              label="Providers"
              value={provider}
              onChange={(v) => set("provider", v)}
              options={providers.map((p) => ({ value: p, label: p }))}
            />
            {filtered && (
              <Button variant="ghost" size="sm" onClick={() => setParams({}, { replace: true })}>
                <X /> Clear
              </Button>
            )}
            <span className="ml-auto text-xs text-muted-foreground tabular">
              {data && `${formatCount(rows.length)} of ${formatCount(data.length)}`}
            </span>
          </div>
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
