import { ChevronRight, History, SlidersHorizontal } from "lucide-react";
import { useState, type FormEvent, type ReactNode } from "react";
import { Link } from "react-router";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { ServedBadge } from "@/components/served-badge";
import { useProviders } from "@/hooks/queries";
import type { RequestRecord } from "@/lib/api";
import type { ApiOption } from "@/lib/playground-apis";
import { servedBy } from "@/lib/served";
import { SelectField } from "./select-field";

/** Every tab's layout: its configuration beside what it makes. */
export function PlaygroundLayout({ config, children }: { config: ReactNode; children: ReactNode }) {
  return (
    <div className="grid grid-cols-[minmax(0,1fr)] items-start gap-5 lg:grid-cols-[20rem_minmax(0,1fr)] xl:grid-cols-[22rem_minmax(0,1fr)]">
      {config}
      {children}
    </div>
  );
}

/** The API a tab speaks: a choice of those served, else the one it has, with its endpoint. */
function ApiField({
  id,
  apis,
  api,
  onApi,
  disabled,
}: {
  id: string;
  apis: ApiOption[];
  api: ApiOption;
  onApi: (id: string) => void;
  disabled?: boolean;
}) {
  return (
    <div className="grid min-w-0 gap-1.5">
      {apis.length > 1 ? (
        <SelectField
          id={id}
          label="API"
          value={api.id}
          onChange={onApi}
          options={apis.map((a) => a.id)}
          optionLabel={(v) => apis.find((a) => a.id === v)?.label ?? v}
          disabled={disabled}
        />
      ) : (
        <div className="grid min-w-0 gap-2">
          <Label id={id}>API</Label>
          <p aria-labelledby={id} className="flex h-9 items-center rounded-md border border-dashed px-3 text-sm">
            {api.label}
          </p>
        </div>
      )}
      <code className="truncate text-[11px] text-muted-foreground" title={api.endpoint}>
        {api.endpoint}
      </code>
    </div>
  );
}

/** Where the last call went, and a link to its record. */
function LastCall({ record }: { record: RequestRecord }) {
  const { data: providers } = useProviders();
  const served = servedBy(record, providers);
  return (
    <div className="flex flex-wrap items-center gap-1.5 border-t pt-4 text-xs text-muted-foreground">
      {served.provider && <ServedBadge provider={served.provider} />}
      <Link to={`/requests/${record.id}`} className="underline-offset-2 hover:underline">
        Request details
      </Link>
    </div>
  );
}

/**
 * Every tab's configuration card: its API, then `children` (the model and the
 * tab's own fields), the last call's record, and a link to the requests of
 * the tab's `kind`. With `onSubmit` it is a form; `collapsible` hides it on a
 * small screen until asked, for a tab whose input is elsewhere.
 */
export function ConfigCard({
  id,
  kind,
  apis,
  api,
  onApi,
  locked,
  record,
  onSubmit,
  collapsible,
  children,
}: {
  id: string;
  kind: string;
  apis: ApiOption[];
  api: ApiOption;
  onApi: (id: string) => void;
  /** No choice may change, as while a session is open. */
  locked?: boolean;
  record?: RequestRecord;
  onSubmit?: () => void;
  collapsible?: boolean;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const fields = (
    <>
      <ApiField id={`${id}-api`} apis={apis} api={api} onApi={onApi} disabled={locked} />
      {children}
    </>
  );
  const submit = (e: FormEvent) => {
    e.preventDefault();
    onSubmit?.();
  };
  return (
    <Card className="h-fit min-w-0 gap-5 p-5">
      <div className="flex items-center justify-between gap-2">
        <span className="flex items-center gap-2 text-xs font-medium tracking-wider text-muted-foreground uppercase">
          <SlidersHorizontal className="size-3.5" aria-hidden="true" /> Configuration
        </span>
        <span className="flex items-center gap-1">
          <Button asChild variant="ghost" size="sm" className="-my-2 h-7 text-muted-foreground">
            <Link to={`/requests?kind=${encodeURIComponent(kind)}`}>
              <History aria-hidden="true" /> Requests
            </Link>
          </Button>
          {collapsible && (
            <Button
              variant="ghost"
              size="sm"
              className="-my-2 -mr-2 h-7 lg:hidden"
              aria-expanded={open}
              aria-controls={`${id}-settings`}
              onClick={() => setOpen(!open)}
            >
              {open ? "Hide" : "Edit"}
              <ChevronRight className={`size-3.5 transition-transform ${open ? "rotate-90" : ""}`} />
            </Button>
          )}
        </span>
      </div>
      <div id={`${id}-settings`} className={`${!collapsible || open ? "grid" : "hidden"} min-w-0 gap-5 lg:grid`}>
        {onSubmit ? (
          <form onSubmit={submit} className="grid min-w-0 gap-5">
            {fields}
          </form>
        ) : (
          fields
        )}
        {record && <LastCall record={record} />}
      </div>
    </Card>
  );
}
