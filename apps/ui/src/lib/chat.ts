import { gatewayFetch } from "@/lib/api";
import { readSse } from "@/lib/sse";

export interface ChatMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

export interface ChatStats {
  inputTokens: number | null;
  outputTokens: number | null;
  ttfbMs: number | null;
  totalMs: number;
  finishReason: string | null;
}

interface ChunkChoice {
  delta?: { content?: string | null };
  finish_reason?: string | null;
}

interface Chunk {
  choices?: ChunkChoice[];
  usage?: { prompt_tokens?: number; completion_tokens?: number } | null;
  error?: { message?: string };
}

/**
 * Streams an OpenAI chat completion through the gateway, calling onDelta with
 * each piece of text as it arrives. Abort with `signal` to stop early.
 */
export async function streamChat({
  model,
  messages,
  signal,
  onDelta,
}: {
  model: string;
  messages: ChatMessage[];
  signal: AbortSignal;
  onDelta: (text: string) => void;
}): Promise<ChatStats> {
  const started = performance.now();
  const stats: ChatStats = {
    inputTokens: null,
    outputTokens: null,
    ttfbMs: null,
    totalMs: 0,
    finishReason: null,
  };
  const res = await gatewayFetch("/v1/chat/completions", {
    method: "POST",
    signal,
    headers: { "content-type": "application/json", accept: "text/event-stream" },
    body: JSON.stringify({
      model,
      messages,
      stream: true,
      stream_options: { include_usage: true },
    }),
  });
  if (!res.body) throw new Error("The gateway sent an empty response");

  await readSse(res.body, ({ data }) => {
    if (data === "[DONE]") return;
    const chunk = JSON.parse(data) as Chunk;
    if (chunk.error) throw new Error(chunk.error.message ?? "The provider reported an error");
    const choice = chunk.choices?.[0];
    const text = choice?.delta?.content;
    if (text) {
      stats.ttfbMs ??= performance.now() - started;
      onDelta(text);
    }
    if (choice?.finish_reason) stats.finishReason = choice.finish_reason;
    if (chunk.usage) {
      stats.inputTokens = chunk.usage.prompt_tokens ?? null;
      stats.outputTokens = chunk.usage.completion_tokens ?? null;
    }
  });
  stats.totalMs = performance.now() - started;
  return stats;
}
