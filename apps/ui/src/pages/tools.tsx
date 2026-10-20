import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Blocks,
  ChevronDown,
  Globe,
  KeyRound,
  Layers,
  Plug,
  RefreshCw,
  Repeat,
  ShieldAlert,
  Terminal,
  UserRound,
} from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";
import { Link } from "react-router";
import { CodeBlock } from "@/components/code-block";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { useMcpOverview, useMcpTools } from "@/hooks/queries";
import { api, type McpOverview, type McpServer, type McpTool, type McpTools } from "@/lib/api";
import { formatCount, formatMs, formatPercent } from "@/lib/format";
import { describeError } from "@/lib/query-client";

const exampleConfig = `[tools.github]
url  = "https://api.githubcopilot.com/mcp/"
auth = { bearer = "env:GITHUB_TOKEN" }

[tools.files]
command = "npx"
args    = ["-y", "@modelcontextprotocol/server-filesystem", "."]

[tools.search]
vendor = "linkup"
key    = "env:LINKUP_API_KEY"`;

const statusStyle: Record<McpServer["status"], string> = {
  idle: "text-muted-foreground",
  ok: "text-success",
  failing: "text-destructive",
};

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <div className="text-[11px] font-medium tracking-wide text-muted-foreground uppercase">{label}</div>
      <div className="truncate text-sm font-medium tabular-nums">{value}</div>
    </div>
  );
}

/**
 * Turns `server` (or one of its tools) on or off in the gateway's memory:
 * shown at once, put back if the gateway refuses.
 */
function useSwitch(server: string, tool?: string) {
  const client = useQueryClient();
  const key = tool === undefined ? ["mcp-overview"] : ["mcp-tools", server];
  const set = (enabled: boolean) => (data: McpOverview | McpTools | undefined) => {
    if (!data) return data;
    if ("servers" in data) {
      return { ...data, servers: data.servers.map((s) => (s.name === server ? { ...s, enabled } : s)) };
    }
    return { ...data, tools: data.tools.map((t) => (t.name === tool ? { ...t, enabled } : t)) };
  };
  return useMutation({
    mutationFn: (enabled: boolean) =>
      tool === undefined ? api.setServerEnabled(server, enabled) : api.setToolEnabled(server, tool, enabled),
    onMutate: (enabled) => client.setQueryData(key, set(enabled)),
    onError: (error, enabled) => {
      client.setQueryData(key, set(!enabled));
      toast.error(`Couldn't turn ${tool ?? server} ${enabled ? "on" : "off"}`, { description: describeError(error) });
    },
    onSettled: () => client.invalidateQueries({ queryKey: key }),
  });
}

/** Whether a title only repeats the name, as "Get attachment" does get_attachment. */
const sameName = (title: string, name: string) =>
  title.toLowerCase().replace(/[^a-z0-9]/g, "") === name.toLowerCase().replace(/[^a-z0-9]/g, "");

function ToolRow({ server, tool }: { server: McpServer; tool: McpTool }) {
  const toggle = useSwitch(server.name, tool.name);
  const [open, setOpen] = useState(false);
  const title = tool.title && !sameName(tool.title, tool.name) ? tool.title : null;
  const on = tool.enabled && server.enabled;
  return (
    <li className="flex items-start gap-3 px-5 py-2 hover:bg-muted/40">
      <button type="button" onClick={() => setOpen(!open)} aria-expanded={open}
        className={`grid min-w-0 flex-1 gap-0.5 text-left ${on ? "" : "opacity-50"}`}>
        <span className="flex min-w-0 items-center gap-1.5">
          <code className="truncate text-[13px] font-medium">{tool.name}</code>
          {title && <span className="truncate text-xs text-muted-foreground">{title}</span>}
          {tool.read_only && <Badge variant="outline" className="h-4 px-1 text-[10px] font-normal">read-only</Badge>}
          {tool.destructive && (
            <Badge variant="outline" className="h-4 px-1 text-[10px] font-normal text-destructive">
              <ShieldAlert aria-hidden />destructive
            </Badge>
          )}
          {tool.tool_loop && <span title="The tool loop offers it to models"><Repeat className="size-3 shrink-0 text-muted-foreground" aria-label="tool loop" /></span>}
          {tool.aggregate && <span title="Offered at /mcp"><Layers className="size-3 shrink-0 text-muted-foreground" aria-label="offered at /mcp" /></span>}
        </span>
        {tool.description && (
          <span className={`text-xs text-muted-foreground ${open ? "whitespace-pre-line" : "truncate"}`}>{tool.description}</span>
        )}
      </button>
      <Switch size="sm" className="mt-1 data-[state=checked]:bg-success" checked={tool.enabled} disabled={!server.enabled}
        onCheckedChange={(next) => toggle.mutate(next)}
        aria-label={`Offer ${tool.name}`} title={server.enabled ? undefined : `${server.name} is off`} />
    </li>
  );
}

/** Turns every tool of `names` on or off at once, as the switches would one by one. */
function useBulkSwitch(server: string) {
  const client = useQueryClient();
  const key = ["mcp-tools", server];
  return useMutation({
    mutationFn: ({ names, enabled }: { names: string[]; enabled: boolean }) =>
      Promise.all(names.map((name) => api.setToolEnabled(server, name, enabled))),
    onMutate: ({ names, enabled }) =>
      client.setQueryData<McpTools>(key, (data) =>
        data && { ...data, tools: data.tools.map((t) => (names.includes(t.name) ? { ...t, enabled } : t)) }),
    onError: (error, { enabled }) =>
      toast.error(`Couldn't turn every tool ${enabled ? "on" : "off"}`, { description: describeError(error) }),
    onSettled: () => client.invalidateQueries({ queryKey: key }),
  });
}

/** The kinds of tool a list is filtered to, by the server's hints. */
const kinds = {
  all: { label: "All", is: () => true },
  read: { label: "Read-only", is: (t: McpTool) => t.read_only === true },
  write: { label: "Writes", is: (t: McpTool) => t.read_only !== true && t.destructive !== true },
  destructive: { label: "Destructive", is: (t: McpTool) => t.destructive === true },
  off: { label: "Off", is: (t: McpTool) => !t.enabled },
} satisfies Record<string, { label: string; is: (t: McpTool) => boolean }>;
type Kind = keyof typeof kinds;

/** A server's tools, filterable by text and kind, with a switch each and for all those shown. */
function ToolList({ server, tools }: { server: McpServer; tools: McpTool[] }) {
  const [query, setQuery] = useState("");
  const [kind, setKind] = useState<Kind>("all");
  const bulk = useBulkSwitch(server.name);
  const q = query.trim().toLowerCase();
  const shown = tools.filter((t) =>
    kinds[kind].is(t) && (!q || [t.name, t.title, t.description].some((x) => x?.toLowerCase().includes(q))));
  const shownOn = shown.filter((t) => t.enabled).length;
  // The master switch is on while every shown tool is; flipping it sets them all.
  const allOn = shown.length > 0 && shownOn === shown.length;
  const set = (enabled: boolean) => bulk.mutate({ names: shown.filter((t) => t.enabled !== enabled).map((t) => t.name), enabled });
  const what = shown.length === tools.length ? `all ${tools.length}` : `the ${shown.length} shown`;
  return (
    <div className="border-t">
      <div className="flex flex-wrap items-center gap-2 px-5 py-2">
        {tools.length > 8 && (
          <Input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Filter tools"
            aria-label={`Filter the tools of ${server.name}`} className="h-8 max-w-xs" />
        )}
        <div role="group" aria-label={`Kinds of tool of ${server.name}`} className="flex flex-wrap rounded-md border p-0.5">
          {(Object.keys(kinds) as Kind[]).map((k) => {
            const count = tools.filter(kinds[k].is).length;
            return (
              <button key={k} type="button" aria-pressed={kind === k} onClick={() => setKind(k)}
                disabled={!count && k !== "all"}
                className={`rounded px-2 py-0.5 text-xs transition-colors disabled:opacity-40 ${kind === k ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground"}`}>
                {kinds[k].label} <span className="tabular-nums opacity-70">{count}</span>
              </button>
            );
          })}
        </div>

      </div>
      {shown.length > 0 && (
        <div className="flex items-center gap-3 border-t bg-muted/30 px-5 py-1.5">
          <span className="flex-1 text-xs text-muted-foreground">{shownOn} of {shown.length} on</span>
          <label className="flex cursor-pointer items-center gap-2 text-xs font-medium">
            Turn {what} {allOn ? "off" : "on"}
            <Switch size="sm" className="data-[state=checked]:bg-success" checked={allOn} disabled={!server.enabled || bulk.isPending}
              onCheckedChange={(next) => set(next)} aria-label={`Turn ${what} tools ${allOn ? "off" : "on"}`} />
          </label>
        </div>
      )}
      {shown.length ? (
        <ul className="max-h-[28rem] divide-y overflow-y-auto border-t" aria-label={`Tools of ${server.name}`}>
          {shown.map((t) => <ToolRow key={t.name} server={server} tool={t} />)}
        </ul>
      ) : <p className="border-t px-5 py-3 text-sm text-muted-foreground">No tool matches.</p>}
    </div>
  );
}

/** A server's tools, asked when opened: asking may start a stdio server. */
function Tools({ server }: { server: McpServer }) {
  const [open, setOpen] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const client = useQueryClient();
  const { data, error, isFetching } = useMcpTools(server.name, open);
  const key = ["mcp-tools", server.name];
  // Asks the server anew; a failure shows as the list's own, refetched.
  const refresh = () => {
    setRefreshing(true);
    api.mcpTools(server.name, true)
      .then((tools) => client.setQueryData(key, tools))
      .catch(() => client.invalidateQueries({ queryKey: key }))
      .finally(() => setRefreshing(false));
  };
  const loginHint = server.auth?.includes("login") ? `respan-gateway mcp login ${server.name}` : null;
  return (
    <div className="border-t">
      <div className="flex items-center gap-2 px-5 py-3">
        <button type="button" onClick={() => setOpen(!open)} aria-expanded={open}
          className="flex items-center gap-2 text-sm font-medium hover:underline">
          <ChevronDown className={`size-4 text-muted-foreground transition-transform ${open ? "rotate-180" : ""}`} aria-hidden />
          Tools
          {data && <span className="font-normal text-muted-foreground">{data.tools.length}</span>}
        </button>
        {open && data && (
          <span className="text-xs text-muted-foreground">listed in {formatMs(data.took_ms)}</span>
        )}
        {open && (
          <Button variant="ghost" size="sm" className="ml-auto h-7" onClick={refresh} disabled={refreshing || isFetching}
            aria-label={`Ask ${server.name} for its tools again`}>
            <RefreshCw className={refreshing || isFetching ? "animate-spin" : ""} aria-hidden />
          </Button>
        )}
      </div>
      {open && (
        error ? (
          <div role="alert" className="grid gap-2 px-5 pb-4 text-sm">
            <p className="text-destructive">{describeError(error)}</p>
            {loginHint && (
              <p className="flex flex-wrap items-center gap-2 text-muted-foreground">
                Log in again: <code className="rounded bg-muted px-1.5 py-0.5 text-xs">{loginHint}</code>
                <CopyButton value={loginHint} label="Copy the login command" className="size-6" />
              </p>
            )}
          </div>
        ) : !data ? (
          <div className="grid gap-2 px-5 pb-4"><Skeleton className="h-10" /><Skeleton className="h-10" /></div>
        ) : data.tools.length ? (
          <ToolList server={server} tools={data.tools} />
        ) : <p className="px-5 pb-4 text-sm text-muted-foreground">It lists no tools.</p>
      )}
    </div>
  );
}

function ServerCard({ server: s, overview }: { server: McpServer; overview: McpOverview }) {
  const Transport = s.transport === "stdio" ? Terminal : s.transport === "vendor" ? Globe : Plug;
  const inLoop = overview.tool_loop?.servers.includes(s.name);
  const inAggregate = overview.aggregate?.servers.includes(s.name);
  const toggle = useSwitch(s.name);
  return (
    <Card className={`min-w-0 gap-0 overflow-hidden py-0 ${s.enabled ? "" : "opacity-70"}`}>
      <div className="grid gap-4 p-5">
        <div className="flex flex-wrap items-start gap-3">
          <span aria-hidden className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-brand-soft text-brand">
            <Transport className="size-4" />
          </span>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
              <h2 className="text-base font-semibold">{s.name}</h2>
              {s.enabled
                ? <span className={`text-xs ${statusStyle[s.status]}`}>{s.status}</span>
                : <span className="text-xs text-muted-foreground">off</span>}
            </div>
            <p className="truncate font-mono text-xs text-muted-foreground" title={s.target}>
              {s.transport === "stdio" ? "runs " : ""}{s.target}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-1.5">
            {inLoop && <Badge variant="secondary" className="font-normal"><Repeat aria-hidden />tool loop</Badge>}
            {inAggregate && <Badge variant="secondary" className="font-normal"><Layers aria-hidden />/mcp</Badge>}
            <Switch checked={s.enabled} onCheckedChange={(on) => toggle.mutate(on)} className="ml-1 data-[state=checked]:bg-success"
              aria-label={`Serve ${s.name}`} title="Until the gateway restarts" />
          </div>
        </div>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,1.3fr)_repeat(3,minmax(0,0.7fr))]">
          <div className="min-w-0">
            <div className="text-[11px] font-medium tracking-wide text-muted-foreground uppercase">Clients connect to</div>
            <div className="flex min-w-0 items-center gap-1">
              <code className="truncate text-sm">{s.endpoint}</code>
              <CopyButton value={s.endpoint} label="Copy endpoint" className="size-6 shrink-0" />
            </div>
          </div>
          <div className="min-w-0">
            <div className="text-[11px] font-medium tracking-wide text-muted-foreground uppercase">Authenticates</div>
            <div className="flex min-w-0 items-center gap-1.5 text-sm" title={s.auth}>
              {s.credentials === "client" ? <UserRound className="size-3.5 shrink-0" aria-hidden /> : <KeyRound className="size-3.5 shrink-0" aria-hidden />}
              <span className="truncate">{s.auth ?? s.credentials}</span>
            </div>
          </div>
          <Stat label="Requests" value={formatCount(s.requests)} />
          <Stat label="Errors" value={s.requests ? formatPercent(s.errors / s.requests) : "—"} />
          <Stat label="Avg latency" value={formatMs(s.avg_latency_ms)} />
        </div>
        {s.requests > 0 && (
          <Link to={`/requests?provider=${encodeURIComponent(s.name)}`} className="w-fit text-xs text-muted-foreground hover:underline">
            Its requests →
          </Link>
        )}
      </div>
      <Tools server={s} />
    </Card>
  );
}

export function ToolsPage() {
  const { data, isPending, error } = useMcpOverview();
  return (
    <div className="grid min-w-0 gap-6">
      <PageHeader title="Tools"
        description="Tool servers the gateway fronts over MCP behind its client keys: MCP servers and tool vendors, what each offers, how it signs in, and who uses it." />
      {error && !data ? (
        <div role="alert" className="text-sm text-destructive">
          <p>Couldn't load tool servers</p>
          <p>{describeError(error)}</p>
        </div>
      ) : isPending ? (
        <Skeleton className="h-48 w-full rounded-xl" />
      ) : !data?.servers.length ? (
        <EmptyState icon={Blocks} title="No tool servers">
          <p>Add one to gateway.toml under <code>[tools]</code>: an MCP server's <code>url</code> or <code>command</code>, as mcp.json would, or a tool <code>vendor</code>.</p>
          <CodeBlock className="w-full text-left" code={exampleConfig} />
        </EmptyState>
      ) : (
        <>
          {(data.aggregate || data.tool_loop) && (
            <div className="grid gap-4 md:grid-cols-2">
              {data.aggregate && (
                <Card className="gap-1 p-5">
                  <div className="flex items-center gap-2 text-sm font-semibold"><Layers className="size-4" aria-hidden />One endpoint for all</div>
                  <div className="flex items-center gap-1">
                    <code className="text-sm">{data.aggregate.endpoint}</code>
                    <CopyButton value={data.aggregate.endpoint} label="Copy the aggregate endpoint" className="size-6" />
                  </div>
                  <p className="text-xs text-muted-foreground">Serves {data.aggregate.servers.join(", ")} as one server, tools named server__tool.</p>
                </Card>
              )}
              {data.tool_loop && (
                <Card className="gap-1 p-5">
                  <div className="flex items-center gap-2 text-sm font-semibold"><Repeat className="size-4" aria-hidden />Tool loop</div>
                  <p className="text-xs text-muted-foreground">
                    Offers the tools of {data.tool_loop.servers.join(", ")} to chat models and runs the calls, so a client that knows nothing of MCP gets answers that used them.
                  </p>
                </Card>
              )}
            </div>
          )}
          <div className="grid gap-4">
            {data.servers.map((s) => <ServerCard key={s.name} server={s} overview={data} />)}
          </div>
        </>
      )}
    </div>
  );
}
