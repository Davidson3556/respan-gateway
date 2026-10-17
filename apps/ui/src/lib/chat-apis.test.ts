import { describe, expect, it } from "vitest";
import { chatApi, chatApis, type ChatApiId, type ChatDelta } from "./chat-apis";
import type { ServedFormat } from "./api";
import { servedApis } from "./playground-apis";
import { SseParser } from "./sse";

const messages = [
  { role: "system" as const, content: "Be brief" },
  { role: "user" as const, content: "Hi" },
  { role: "assistant" as const, content: "Hello" },
  { role: "user" as const, content: "Again" },
];

/** Reads a raw SSE stream through an adapter and merges what it saw. */
function run(id: ChatApiId, raw: string) {
  const out: ChatDelta & { text: string } = { text: "" };
  for (const event of new SseParser().push(raw)) {
    const d = chatApi(id).read(event);
    out.text += d.text ?? "";
    for (const k of ["inputTokens", "outputTokens", "finishReason"] as const) {
      if (d[k] !== undefined) Object.assign(out, { [k]: d[k] });
    }
  }
  return out;
}

describe("request adapters", () => {
  it("keep the system prompt in messages for OpenAI Chat", () => {
    expect(chatApi("openai-chat").body("m", messages)).toMatchObject({ model: "m", stream: true, messages });
  });

  it("lift the system prompt out for Anthropic Messages", () => {
    const body = chatApi("anthropic-messages").body("m", messages) as Record<string, unknown>;
    expect(body).toMatchObject({ system: "Be brief", stream: true, max_tokens: 4096 });
    expect(body.messages).toHaveLength(3);
  });

  it("send instructions and input for Responses", () => {
    const body = chatApi("openai-responses").body("m", messages) as Record<string, unknown>;
    expect(body).toMatchObject({ instructions: "Be brief", stream: true });
    expect(body.input).toHaveLength(3);
  });

  it("send contents with model turns for Gemini, and name the model in the path", () => {
    const gemini = chatApi("gemini-generate");
    expect(gemini.body("m", messages)).toEqual({
      systemInstruction: { parts: [{ text: "Be brief" }] },
      contents: [
        { role: "user", parts: [{ text: "Hi" }] },
        { role: "model", parts: [{ text: "Hello" }] },
        { role: "user", parts: [{ text: "Again" }] },
      ],
    });
    expect(gemini.path("gemini-2.5-flash")).toBe(
      "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
    );
  });
});

describe("stream adapters", () => {
  it("read OpenAI Chat chunks and usage", () => {
    const raw =
      `data: {"choices":[{"delta":{"content":"He"}}]}\n\n` +
      `data: {"choices":[{"delta":{"content":"y"},"finish_reason":"stop"}]}\n\n` +
      `data: {"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":2}}\n\n` +
      `data: [DONE]\n\n`;
    expect(run("openai-chat", raw)).toEqual({
      text: "Hey",
      finishReason: "stop",
      inputTokens: 4,
      outputTokens: 2,
    });
  });

  it("read Anthropic events", () => {
    const raw =
      `event: message_start\ndata: {"message":{"usage":{"input_tokens":9}}}\n\n` +
      `event: content_block_delta\ndata: {"delta":{"type":"text_delta","text":"Hi "}}\n\n` +
      `event: content_block_delta\ndata: {"delta":{"type":"input_json_delta","partial_json":"{}"}}\n\n` +
      `event: content_block_delta\ndata: {"delta":{"type":"text_delta","text":"you"}}\n\n` +
      `event: message_delta\ndata: {"delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}\n\n` +
      `event: ping\ndata: {}\n\n`;
    expect(run("anthropic-messages", raw)).toEqual({
      text: "Hi you",
      finishReason: "end_turn",
      inputTokens: 9,
      outputTokens: 3,
    });
  });

  it("read Responses events", () => {
    const raw =
      `event: response.created\ndata: {"type":"response.created","response":{}}\n\n` +
      `event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"Yo"}\n\n` +
      `event: response.completed\ndata: {"type":"response.completed","response":{"usage":{"input_tokens":5,"output_tokens":1}}}\n\n`;
    expect(run("openai-responses", raw)).toEqual({
      text: "Yo",
      finishReason: "stop",
      inputTokens: 5,
      outputTokens: 1,
    });
  });

  it("read Gemini chunks", () => {
    const raw =
      `data: {"candidates":[{"content":{"parts":[{"text":"A"},{"text":"B"}]}}]}\n\n` +
      `data: {"candidates":[{"content":{"parts":[{"text":"C"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":6,"candidatesTokenCount":3}}\n\n`;
    expect(run("gemini-generate", raw)).toEqual({
      text: "ABC",
      finishReason: "stop",
      inputTokens: 6,
      outputTokens: 3,
    });
  });

  it("throw on an error each API reports in-stream", () => {
    expect(() => run("openai-chat", `data: {"error":{"message":"boom"}}\n\n`)).toThrow("boom");
    expect(() =>
      run("anthropic-messages", `event: error\ndata: {"error":{"message":"overloaded"}}\n\n`),
    ).toThrow("overloaded");
    expect(() =>
      run("openai-responses", `data: {"type":"response.failed","response":{"error":{"message":"no"}}}\n\n`),
    ).toThrow("no");
    expect(() => run("gemini-generate", `data: {"error":{"message":"quota"}}\n\n`)).toThrow("quota");
  });
});

describe("servedApis for chat", () => {
  it("lists only the chat APIs the gateway serves, in display order", () => {
    const serve: Pick<ServedFormat, "format" | "kind">[] = [
      { format: "gemini-generate", kind: "chat" },
      { format: "openai-chat", kind: "chat" },
      { format: "openai-realtime", kind: "realtime" },
    ];
    expect(servedApis("chat", chatApis, serve).map((a) => a.id)).toEqual(["openai-chat", "gemini-generate"]);
  });

  it("falls back to OpenAI Chat before the gateway has answered", () => {
    expect(servedApis("chat", chatApis, undefined).map((a) => a.id)).toEqual(["openai-chat"]);
    expect(chatApis).toHaveLength(4);
  });
});
