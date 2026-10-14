import { useQueryClient } from "@tanstack/react-query";
import {
  Blocks,
  ChevronDown,
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
import { Link } from "react-router";
import { CodeBlock } from "@/components/code-block";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useMcpOverview, useMcpTools } from "@/hooks/queries";
import { api, type McpOverview, type McpServer, type McpTool } from "@/lib/api";
import { formatCount, formatMs, formatPercent } from "@/lib/format";
import { describeError } from "@/lib/query-client";

const exampleConfig = `[mcp.github]
url = "https://api.githubcopilot.com/mcp/"
auth = { bearer = "env:GITHUB_TOKEN" }

[mcp.files]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "."]`;

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

function ToolRow({ tool }: { tool: McpTool }) {
  return (
    <li className="grid gap-1 px-5 py-3">
      <div className="flex flex-wrap items-center gap-2">
        <code className="text-sm font-medium">{tool.name}</code>
        {tool.title && tool.title !== tool.name && <span className="text-sm text-muted-foreground">{tool.title}</span>}
        <span className="ml-auto flex flex-wrap gap-1.5">
          {tool.read_only && <Badge variant="outline" className="font-normal">read-only</Badge>}
          {tool.destructive && (
            <Badge variant="outline" className="font-normal text-destructive">
              <ShieldAlert aria-hidden />destructive
            </Badge>
          )}
          {tool.tool_loop && (
            <Badge variant="secondary" className="font-normal" title="The tool loop offers it to models">
              <Repeat aria-hidden />tool loop
            </Badge>
          )}
          {tool.aggregate && (
            <Badge variant="secondary" className="font-normal" title="Offered at /mcp">
              <Layers aria-hidden />/mcp
            </Badge>
          )}
        </span>
      </div>
      {tool.description && <p className="line-clamp-2 text-xs text-muted-foreground" title={tool.description}>{tool.description}</p>}
    </li>
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
          <ul className="divide-y border-t" aria-label={`Tools of ${server.name}`}>
            {data.tools.map((t) => <ToolRow key={t.name} tool={t} />)}
          </ul>
        ) : <p className="px-5 pb-4 text-sm text-muted-foreground">It lists no tools.</p>
      )}
    </div>
  );
}

function ServerCard({ server: s, overview }: { server: McpServer; overview: McpOverview }) {
  const Transport = s.transport === "stdio" ? Terminal : Plug;
  const inLoop = overview.tool_loop?.servers.includes(s.name);
  const inAggregate = overview.aggregate?.servers.includes(s.name);
  return (
    <Card className="min-w-0 gap-0 overflow-hidden py-0">
      <div className="grid gap-4 p-5">
        <div className="flex flex-wrap items-start gap-3">
          <span aria-hidden className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-brand-soft text-brand">
            <Transport className="size-4" />
          </span>
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
              <h2 className="text-base font-semibold">{s.name}</h2>
              <span className={`text-xs ${statusStyle[s.status]}`}>{s.status}</span>
            </div>
            <p className="truncate font-mono text-xs text-muted-foreground" title={s.target}>
              {s.transport === "stdio" ? "runs " : ""}{s.target}
            </p>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {inLoop && <Badge variant="secondary" className="font-normal"><Repeat aria-hidden />tool loop</Badge>}
            {inAggregate && <Badge variant="secondary" className="font-normal"><Layers aria-hidden />/mcp</Badge>}
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

export function McpPage() {
  const { data, isPending, error } = useMcpOverview();
  return (
    <div className="grid min-w-0 gap-6">
      <PageHeader title="MCP"
        description="Tool servers the gateway fronts behind its client keys: what each offers, how it signs in, and who uses it." />
      {error && !data ? (
        <div role="alert" className="text-sm text-destructive">
          <p>Couldn't load MCP servers</p>
          <p>{describeError(error)}</p>
        </div>
      ) : isPending ? (
        <Skeleton className="h-48 w-full rounded-xl" />
      ) : !data?.servers.length ? (
        <EmptyState icon={Blocks} title="No MCP servers">
          <p>Add a server to gateway.toml, as mcp.json would: a <code>url</code>, or a <code>command</code> to run.</p>
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
