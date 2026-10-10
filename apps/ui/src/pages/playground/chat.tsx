import { ArrowUp, Eraser, MessageSquare, Square } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent } from "react";
import { EmptyState } from "@/components/empty-state";
import { Markdown } from "@/components/markdown";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useModelChoice } from "@/hooks/use-model-choice";
import { streamChat, type ChatMessage, type ChatStats } from "@/lib/chat";
import { formatCount, formatMs } from "@/lib/format";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";

interface Turn {
  role: "user" | "assistant";
  content: string;
  stats?: ChatStats;
  error?: unknown;
  stopped?: boolean;
}

function Stats({ stats }: { stats: ChatStats }) {
  const parts = [
    stats.inputTokens != null &&
      `${formatCount(stats.inputTokens)} → ${formatCount(stats.outputTokens ?? 0)} tokens`,
    stats.ttfbMs != null && `first token ${formatMs(stats.ttfbMs)}`,
    `${formatMs(stats.totalMs)} total`,
    stats.finishReason && stats.finishReason !== "stop" && `finish: ${stats.finishReason}`,
  ].filter(Boolean);
  return <p className="mt-2 text-xs text-muted-foreground tabular">{parts.join(" · ")}</p>;
}

function Message({ turn, streaming }: { turn: Turn; streaming: boolean }) {
  if (turn.role === "user") {
    return (
      <div className="flex justify-end">
        <div className="max-w-[85%] rounded-2xl rounded-br-md bg-primary px-4 py-2.5 text-sm whitespace-pre-wrap text-primary-foreground">
          {turn.content}
        </div>
      </div>
    );
  }
  return (
    <div className="max-w-[92%]">
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
      {turn.stats && <Stats stats={turn.stats} />}
      {turn.stopped && <p className="mt-2 text-xs text-muted-foreground">Stopped</p>}
    </div>
  );
}

export function ChatTab() {
  const choice = useModelChoice("chat", "gpt-4o-mini");
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

  const updateLast = (patch: (t: Turn) => Turn) =>
    setTurns((prev) => [...prev.slice(0, -1), patch(prev[prev.length - 1])]);

  const send = async (e?: FormEvent) => {
    e?.preventDefault();
    const text = input.trim();
    if (!text || streaming) return;
    const history = [...turns.filter((t) => t.error === undefined), { role: "user", content: text } as Turn];
    const messages: ChatMessage[] = [
      ...(system.trim() ? [{ role: "system" as const, content: system.trim() }] : []),
      ...history.map(({ role, content }) => ({ role, content })),
    ];
    const ctrl = new AbortController();
    setInput("");
    setTurns([...history, { role: "assistant", content: "" }]);
    setController(ctrl);
    try {
      const stats = await streamChat({
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
    }
  };

  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      void send();
    }
  };

  return (
    <div className="grid gap-4 lg:grid-cols-[18rem_1fr]">
      <Card className="h-fit gap-5 p-4 shadow-none">
        <ModelPicker id="chat-model" choice={choice} />
        <div className="grid gap-2">
          <Label htmlFor="chat-system">System prompt</Label>
          <Textarea
            id="chat-system"
            value={system}
            onChange={(e) => setSystem(e.target.value)}
            placeholder="You are a helpful assistant."
            className="min-h-28 text-sm"
          />
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setTurns([])}
          disabled={turns.length === 0 || streaming}
        >
          <Eraser /> Clear conversation
        </Button>
      </Card>

      <Card className="flex min-h-[32rem] flex-col gap-0 overflow-hidden py-0 shadow-none">
        <div className="flex-1 overflow-y-auto p-4 sm:p-6">
          {/* Announce completion only; per-token updates would be read out one by one. */}
          <p role="status" className="sr-only">
            {!streaming && turns.length > 0 ? "Response complete" : ""}
          </p>
          {turns.length === 0 ? (
            <EmptyState icon={MessageSquare} title="Talk to any model" className="border-none">
              <p>
                Messages go through the gateway's <code>/v1/chat/completions</code>, streamed, so
                you'll see routing, translation and usage exactly as a client would.
              </p>
            </EmptyState>
          ) : (
            <div className="grid gap-6">
              {turns.map((t, i) => (
                <Message key={i} turn={t} streaming={streaming && i === turns.length - 1} />
              ))}
              <div ref={bottom} />
            </div>
          )}
        </div>
        <form onSubmit={send} className="border-t bg-muted/20 p-3">
          <div className="relative rounded-xl border bg-background shadow-xs focus-within:ring-[3px] focus-within:ring-ring/40">
            <Textarea
              aria-label="Message"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={onKeyDown}
              placeholder={`Message ${choice.model}…`}
              className="max-h-48 min-h-14 resize-none border-0 pr-14 shadow-none focus-visible:ring-0 dark:bg-transparent"
            />
            {streaming ? (
              <Button
                type="button"
                size="icon"
                variant="secondary"
                className="absolute right-2 bottom-2 size-8 rounded-lg"
                onClick={() => controller.abort()}
                aria-label="Stop generating"
              >
                <Square className="size-3.5 fill-current" />
              </Button>
            ) : (
              <Button
                type="submit"
                size="icon"
                className="absolute right-2 bottom-2 size-8 rounded-lg"
                disabled={!input.trim()}
                aria-label="Send message"
              >
                <ArrowUp />
              </Button>
            )}
          </div>
          <p className="mt-2 px-1 text-[11px] text-muted-foreground">
            Enter to send · Shift+Enter for a new line
          </p>
        </form>
      </Card>
    </div>
  );
}
