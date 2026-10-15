import { Search, X } from "lucide-react";
import { ModelCombobox } from "@/components/model-combobox";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ANY, type Outcome, type RequestFilter } from "@/lib/requests";
import { cn } from "@/lib/utils";

const outcomes: { value: Outcome | typeof ANY; label: string; dot?: string }[] = [
  { value: ANY, label: "All" },
  { value: "ok", label: "Succeeded", dot: "bg-success" },
  { value: "error", label: "Failed", dot: "bg-destructive" },
  { value: "pending", label: "In flight", dot: "bg-brand" },
];

/** Status as pills, each with how many requests it would show. */
function StatusPills({
  value,
  counts,
  onChange,
}: {
  value: string;
  counts: Record<string, number>;
  onChange: (value: string) => void;
}) {
  return (
    <div role="group" aria-label="Status" className="flex flex-wrap gap-1.5">
      {outcomes.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={value === o.value}
          onClick={() => onChange(o.value)}
          className={cn(
            "inline-flex h-8 items-center gap-1.5 rounded-full border px-3 text-xs font-medium outline-none transition-colors hover:bg-muted focus-visible:ring-[3px] focus-visible:ring-ring/50",
            value === o.value && "border-brand bg-brand-soft/60",
          )}
        >
          {o.dot && <span className={cn("size-1.5 rounded-full", o.dot)} aria-hidden="true" />}
          {o.label}
          <span className="text-muted-foreground tabular">{counts[o.value] ?? 0}</span>
        </button>
      ))}
    </div>
  );
}

export function FilterSelect({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: string[];
  onChange: (value: string) => void;
}) {
  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger size="sm" className="w-full sm:w-36" aria-label={label}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={ANY}>All {label.toLowerCase()}s</SelectItem>
        {options.map((o) => (
          <SelectItem key={o} value={o}>
            {o}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

/** The requests page's search box, status pills and kind, provider and model menus. */
export function RequestFilters({
  filter,
  onChange,
  onClear,
  counts,
  options,
  shown,
  total,
}: {
  filter: RequestFilter;
  onChange: (name: keyof RequestFilter, value: string) => void;
  onClear: () => void;
  counts: Record<string, number>;
  options: { kinds: string[]; providers: string[]; models: string[]; clients: string[] };
  shown: number;
  total: number;
}) {
  const active = Object.entries(filter).some(([k, v]) => v !== (k === "q" ? "" : ANY));
  return (
    <div className="grid gap-3 border-b p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <StatusPills value={filter.status} counts={counts} onChange={(v) => onChange("status", v)} />
        <span className="text-xs text-muted-foreground tabular" aria-live="polite">
          {shown.toLocaleString()} of {total.toLocaleString()}
        </span>
      </div>
      <div className="grid grid-cols-2 gap-2 sm:flex sm:flex-wrap sm:items-center">
        <div className="relative col-span-2 sm:min-w-56 sm:flex-1 sm:max-w-xs">
          <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            type="search"
            value={filter.q}
            onChange={(e) => onChange("q", e.target.value)}
            placeholder="Search model or path"
            aria-label="Search model or path"
            className="h-8 pl-8"
          />
        </div>
        <FilterSelect label="Kind" value={filter.kind} options={options.kinds} onChange={(v) => onChange("kind", v)} />
        <FilterSelect label="Provider" value={filter.provider} options={options.providers} onChange={(v) => onChange("provider", v)} />
        <ModelCombobox
          label="Model"
          value={filter.model}
          onChange={(v) => onChange("model", v)}
          models={options.models.map((id) => ({ id }))}
          all="All models"
          allValue={ANY}
          size="sm"
          className="sm:w-44"
        />
        {options.clients.length > 0 && (
          <FilterSelect label="Client" value={filter.client} options={options.clients} onChange={(v) => onChange("client", v)} />
        )}
        {active && (
          <Button variant="ghost" size="sm" onClick={onClear}>
            <X /> Clear
          </Button>
        )}
      </div>
    </div>
  );
}
