import { ArrowUp, ChevronRight, Eraser, MessageSquare, Sparkles, Square, Wrench } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent } from "react";
import { Link } from "react-router";
import { CopyButton } from "@/components/copy-button";
import { EmptyState } from "@/components/empty-state";
import { Markdown } from "@/components/markdown";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { ServedBadge } from "@/components/served-badge";
import { useProviders } from "@/hooks/queries";
import { useModelChoice } from "@/hooks/use-model-choice";
import { useApiChoice } from "@/hooks/use-playground";
import type { Provider, RequestRecord } from "@/lib/api";
import { chatApi, chatApis } from "@/lib/chat-apis";
import { streamChat, type ChatMessage, type ChatStats } from "@/lib/chat";
import { formatCount, formatMs } from "@/lib/format";
import { locateCall, servedBy } from "@/lib/served";
import { ConfigCard, PlaygroundLayout } from "./layout";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";

const suggestions = ["Explain how an API gateway works", "Write a haiku about latency", "Summarise HTTP/3 in three bullets"];

interface Turn {
  role: "user" | "assistant";
  content: string;
  stats?: ChatStats;
  error?: unknown;
  stopped?: boolean;
  /** The gateway's record of the call, once it has one. */
  record?: RequestRecord;
}

function Served({ record, providers }: { record: RequestRecord; providers: Provider[] | undefined }) {
  const served = servedBy(record, providers);
  const tools = (served.steps ?? []).flatMap((s) => s.tools);
  return (
    <div className="mt-2 grid gap-2">
      <div className="flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
        {served.provider && <ServedBadge provider={served.provider} />}
        {served.translatedFrom && (
          <Badge variant="secondary" className="font-normal">
            translated from {chatApi(served.translatedFrom).label}
          </Badge>
        )}
        <Link to={`/requests/${served.id}`} className="underline-offset-2 hover:underline">
          Request details
        </Link>
      </div>
      {tools.length > 0 && (
        <details className="group text-xs">
          <summary className="flex w-fit cursor-pointer list-none items-center gap-1 rounded text-muted-foreground hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none [&::-webkit-details-marker]:hidden">
            <ChevronRight className="size-3.5 transition-transform group-open:rotate-90" aria-hidden="true" />
            <Wrench className="size-3.5" aria-hidden="true" />
            Used {tools.length} {tools.length === 1 ? "tool" : "tools"}
          </summary>
          <ul className="mt-2 grid gap-1 border-l-2 border-brand/40 pl-3" aria-label="Tools used">
            {tools.map((tool, i) => (
              <li key={i} className="flex flex-wrap items-baseline gap-x-2">
                <span className={`font-mono break-all ${tool.error ? "text-destructive" : ""}`}>
                  {tool.name}
                </span>
                <span className="font-mono text-muted-foreground tabular">
                  {formatMs(tool.duration_ms)}
                  {tool.error && " · failed"}
                </span>
              </li>
            ))}
          </ul>
          <Link
            to={`/requests/${served.id}`}
            className="mt-2 inline-block text-muted-foreground underline-offset-2 hover:underline"
          >
            See every step
          </Link>
        </details>
      )}
    </div>
  );
}

function Stats({ stats }: { stats: ChatStats }) {
  const parts = [
    stats.inputTokens != null &&
      `${formatCount(stats.inputTokens)} → ${formatCount(stats.outputTokens ?? 0)} tokens`,
    stats.ttfbMs != null && `first token ${formatMs(stats.ttfbMs)}`,
    `${formatMs(stats.totalMs)} total`,
    stats.finishReason && stats.finishReason !== "stop" && `finish: ${stats.finishReason}`,
  ].filter(Boolean);
  return <p className="text-xs text-muted-foreground tabular">{parts.join(" · ")}</p>;
}

function Message({
  turn,
  streaming,
  providers,
}: {
  turn: Turn;
  streaming: boolean;
  providers: Provider[] | undefined;
}) {
  if (turn.role === "user") {
    return (
      <div className="flex justify-end">
        <div className="max-w-[85%] break-words rounded-2xl rounded-br-md border border-brand/10 bg-brand-soft/50 px-5 py-3 text-sm leading-relaxed whitespace-pre-wrap">
          {turn.content}
        </div>
      </div>
    );
  }
  return (
    <div className="flex gap-3">
      <span
        aria-hidden="true"
        className="mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-lg bg-brand-soft text-brand"
      >
        <Sparkles className="size-3.5" />
      </span>
      <div className="min-w-0 flex-1 break-words">
        {turn.content ? (
          <Markdown source={turn.content} />
        ) : (
          streaming && <span className="text-sm text-muted-foreground">Thinking…</span>
        )}
        {streaming && (
          <span className="ml-0.5 inline-block h-4 w-1.5 translate-y-0.5 animate-pulse rounded-sm bg-brand" />
        )}
        {turn.error !== undefined && (
          <div className="mt-2">
            <RequestError error={turn.error} what="chat" />
          </div>
        )}
        {turn.stats && (
          <div className="mt-4 flex flex-wrap items-center gap-2">
            <Stats stats={turn.stats} />
            <CopyButton value={turn.content} label="Copy reply" className="size-6" />
          </div>
        )}
        {turn.record && <Served record={turn.record} providers={providers} />}
        {turn.stopped && <p className="mt-2 text-xs text-muted-foreground">Stopped</p>}
      </div>
    </div>
  );
}

export function ChatTab() {
  const choice = useModelChoice("chat", "gpt-4o-mini");
  const { data: providers } = useProviders();
  const apiChoice = useApiChoice("chat", chatApis);
  const selected = apiChoice.api;
  const [system, setSystem] = useState("");
  const [input, setInput] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [controller, setController] = useState<AbortController | null>(null);
  const bottom = useRef<HTMLDivElement>(null);
  const streaming = controller !== null;

  useEffect(() => {
    bottom.current?.scrollIntoView({ block: "nearest" });
  }, [turns]);

  useEffect(() => () => controller?.abort(), [controller]);

  const updateAt = (index: number, patch: (t: Turn) => Turn) =>
    setTurns((prev) => prev.map((t, i) => (i === index ? patch(t) : t)));

  const send = async (e?: FormEvent) => {
    e?.preventDefault();
    const text = input.trim();
    if (!text || streaming) return;
    const history = [...turns, { role: "user", content: text } as Turn];
    const messages: ChatMessage[] = [
      ...(system.trim() ? [{ role: "system" as const, content: system.trim() }] : []),
      ...history.filter((t) => t.content.trim()).map(({ role, content }) => ({ role, content })),
    ];
    const ctrl = new AbortController();
    setInput("");
    setTurns([...history, { role: "assistant", content: "" }]);
    setController(ctrl);
    const index = history.length;
    const updateLast = (patch: (t: Turn) => Turn) => updateAt(index, patch);
    const since = Date.now();
    try {
      const stats = await streamChat({
        api: selected.id,
        model: choice.model,
        messages,
        signal: ctrl.signal,
        onDelta: (delta) => updateLast((t) => ({ ...t, content: t.content + delta })),
      });
      updateLast((t) => ({ ...t, stats }));
    } catch (error) {
      updateLast((t) => (ctrl.signal.aborted ? { ...t, stopped: true } : { ...t, error }));
    } finally {
      setController(null);
      void locateCall({ api: selected.id, model: choice.model, since })
        .then((record) => record && updateLast((t) => ({ ...t, record })))
        .catch(() => undefined);
    }
  };

  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      void send();
    }
  };

  return (
    <PlaygroundLayout
      config={
        <ConfigCard id="chat" kind="chat" apis={apiChoice.apis} api={selected} onApi={apiChoice.choose} collapsible>
          <ModelPicker id="chat-model" choice={choice} />
          <div className="grid min-w-0 gap-2">
            <Label htmlFor="chat-system">System prompt</Label>
            <Textarea
              id="chat-system"
              value={system}
              onChange={(e) => setSystem(e.target.value)}
              placeholder="You are a helpful assistant."
              className="min-h-32 resize-y text-sm leading-relaxed"
            />
          </div>
        </ConfigCard>
      }
    >
      <Card className="flex h-[calc(100dvh-18rem)] min-h-[32rem] flex-col gap-0 overflow-hidden py-0 shadow-sm">
        <div className="flex shrink-0 items-center justify-between gap-3 border-b bg-muted/15 px-4 py-3 sm:px-6">
          <div className="min-w-0">
            <div className="flex items-center gap-2 text-sm font-medium">
              <MessageSquare className="size-4 shrink-0 text-brand" />
              <span className="truncate">{choice.model || "Conversation"}</span>
            </div>
            <p className="mt-1 flex flex-wrap gap-x-2 text-xs text-muted-foreground">
              <span>{selected.label}</span>
            </p>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setTurns([])}
            disabled={turns.length === 0 || streaming}
            aria-label="Clear conversation"
            className="shrink-0 text-muted-foreground"
          >
            <Eraser /> <span className="hidden sm:inline">Clear conversation</span>
          </Button>
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain p-4 sm:px-8 sm:py-8">
          {/* Announce completion only; per-token updates would be read out one by one. */}
          <p role="status" className="sr-only">
            {!streaming && turns.length > 0 ? "Response complete" : ""}
          </p>
          {turns.length === 0 ? (
            <EmptyState icon={MessageSquare} title="Talk to any model" className="mx-auto min-h-full max-w-2xl justify-center border-none px-0 py-8 [&_h3]:text-xl [&_h3]:tracking-tight">
              <div className="mt-4 grid w-full gap-2">
                {suggestions.map((text) => (
                  <Button key={text} variant="outline" onClick={() => setInput(text)} className="h-auto justify-between gap-3 rounded-xl bg-background/50 px-4 py-3 text-left font-normal whitespace-normal hover:border-brand/40 hover:bg-brand-soft/30">
                    {text}<ArrowUp className="size-3.5 shrink-0 rotate-45 text-muted-foreground" />
                  </Button>
                ))}
              </div>
            </EmptyState>
          ) : (
            <div className="mx-auto grid w-full max-w-3xl grid-cols-[minmax(0,1fr)] gap-8">
              {turns.map((t, i) => (
                <Message
                  key={i}
                  turn={t}
                  streaming={streaming && i === turns.length - 1}
                  providers={providers}
                />
              ))}
              <div ref={bottom} />
            </div>
          )}
        </div>
        <form onSubmit={send} className="shrink-0 px-3 pt-2 pb-3 sm:px-6 sm:pb-4">
          <div className="relative mx-auto max-w-3xl rounded-2xl border bg-background shadow-sm transition-shadow focus-within:border-brand/50 focus-within:ring-[3px] focus-within:ring-brand/10">
            <Textarea
              aria-label="Message"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={onKeyDown}
              placeholder={`Message ${choice.model}…`}
              className="max-h-48 min-h-24 resize-none rounded-2xl border-0 bg-transparent px-4 py-4 pr-16 text-sm shadow-none focus-visible:ring-0 dark:bg-transparent"
            />
            {streaming ? (
              <Button
                type="button"
                size="icon"
                variant="secondary"
                className="absolute right-3 bottom-3 size-9 rounded-xl"
                onClick={() => controller.abort()}
                aria-label="Stop generating"
              >
                <Square className="size-3.5 fill-current" />
              </Button>
            ) : (
              <Button
                type="submit"
                size="icon"
                className="absolute right-3 bottom-3 size-9 rounded-xl"
                disabled={!input.trim()}
                aria-label="Send message"
              >
                <ArrowUp />
              </Button>
            )}
          </div>
          <p className="mx-auto mt-2 max-w-3xl px-1 text-[11px] text-muted-foreground">
            Enter to send · Shift+Enter for a new line
          </p>
        </form>
      </Card>
    </PlaygroundLayout>
  );
}
