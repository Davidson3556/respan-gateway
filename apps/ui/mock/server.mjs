// A stand-in for the gateway, for developing and screenshotting the UI
// without the Rust binary. It serves the admin API from the UI contract with
// generated traffic, and fake /v1 endpoints for the playground (chat, images,
// transcription, speech, embeddings and video jobs; a video prompt containing
// "fail" makes its job fail). Zero dependencies.
//
//   pnpm mock                      # http://127.0.0.1:8080, open
//   MOCK_KEY=secret pnpm mock      # every call needs "Bearer secret"
//   PORT=8081 MOCK_QUIET=1 pnpm mock   # another port; no generated traffic

import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { crc32, deflateSync } from "node:zlib";

const PORT = Number(process.env.PORT ?? 8080);
const KEY = process.env.MOCK_KEY ?? "";
const QUIET = !!process.env.MOCK_QUIET;
const startedAt = new Date();

// Providers as the registry in crates/providers/README.md has them: each
// vendor's formats and quirks. `gateway` credentials are the gateway's own
// key; `client` providers take the caller's.
const quirk = {
  thought_signature: {
    name: "thought_signature",
    description:
      "A Gemini 3 model refuses a function call in a request's history that has no `thoughtSignature`, as one from another format has none; it takes its documented placeholder in place of one.",
  },
  openai_transcription_model: {
    name: "openai_transcription_model",
    description:
      "Realtime transcribes the user's audio only with a named model: a session that turns transcription on without one asks for `gpt-4o-mini-transcribe`.",
  },
  model_id_prefix: {
    name: "model_id_prefix",
    description: "Its OpenAI-compatible model list names each model `models/<id>`.",
  },
  x_groq_usage: {
    name: "x_groq_usage",
    description:
      "A response reports usage under `x_groq`, beside it, rather than in `usage`; the object's other fields stay native.",
  },
  reasoning_content: {
    name: "reasoning_content",
    description:
      "Reasoning is `reasoning_content`, in a request's messages and a response's message or delta.",
  },
  azure_inert: {
    name: "inert_choice",
    description:
      "A choice's `content_filter_results` and `content_filter_offsets` describe the moderation it passed.",
  },
  openrouter_inert: {
    name: "inert_choice",
    description:
      "A choice's `native_finish_reason`, the upstream provider's own reason, describes its finish.",
  },
  vllm_inert: {
    name: "inert_choice",
    description:
      "A choice's `stop_reason`, the stop token or string that ended it, describes its finish.",
  },
};

const kindOf = {
  "openai-chat": "chat",
  "openai-responses": "chat",
  "anthropic-messages": "chat",
  "gemini-generate": "chat",
  "openai-realtime": "realtime",
  "gemini-live": "realtime",
  "responses-ws": "responses",
  "openai-images": "image",
  "openai-transcriptions": "transcription",
  "openai-speech": "speech",
  "openai-videos": "video",
  "gemini-veo": "video",
  "openai-embeddings": "embedding",
};

const vendor = (name, formats, quirks, base_url, credentials = "gateway") => ({
  name,
  vendor: name,
  formats,
  kinds: [...new Set(formats.map((f) => kindOf[f]))],
  base_url,
  credentials,
  quirks,
});

const providers = [
  vendor(
    "openai",
    [
      "openai-chat",
      "openai-responses",
      "openai-realtime",
      "responses-ws",
      "openai-images",
      "openai-transcriptions",
      "openai-speech",
      "openai-videos",
      "openai-embeddings",
    ],
    [quirk.openai_transcription_model],
    "https://api.openai.com/v1",
  ),
  vendor("anthropic", ["anthropic-messages"], [], "https://api.anthropic.com"),
  vendor(
    "gemini",
    [
      "openai-chat",
      "gemini-generate",
      "gemini-live",
      "openai-images",
      "gemini-veo",
      "openai-embeddings",
    ],
    [quirk.model_id_prefix, quirk.thought_signature],
    "https://generativelanguage.googleapis.com/v1beta/openai",
  ),
  vendor(
    "groq",
    ["openai-chat", "openai-transcriptions", "openai-speech"],
    [quirk.x_groq_usage],
    "https://api.groq.com/openai/v1",
    "client",
  ),
  vendor("deepseek", ["openai-chat"], [quirk.reasoning_content], "https://api.deepseek.com"),
  vendor("openrouter", ["openai-chat"], [quirk.openrouter_inert], "https://openrouter.ai/api/v1"),
  vendor("azure", ["openai-chat"], [quirk.azure_inert], "https://acme.openai.azure.com/openai/v1"),
  vendor(
    "vllm",
    ["openai-chat"],
    [quirk.reasoning_content, quirk.vllm_inert],
    "http://gpu-box:8000/v1",
    "client",
  ),
];

const routes = {
  serve: [
    { format: "openai-chat", kind: "chat", endpoints: ["POST /v1/chat/completions"] },
    { format: "openai-responses", kind: "chat", endpoints: ["POST /v1/responses"] },
    { format: "anthropic-messages", kind: "chat", endpoints: ["POST /v1/messages"] },
    {
      format: "gemini-generate",
      kind: "chat",
      endpoints: [
        "POST /v1beta/models/{model}:generateContent",
        "POST /v1beta/models/{model}:streamGenerateContent",
      ],
    },
    { format: "openai-realtime", kind: "realtime", endpoints: ["GET /v1/realtime"] },
    { format: "responses-ws", kind: "responses", endpoints: ["GET /v1/responses"] },
    {
      format: "openai-images",
      kind: "image",
      endpoints: ["POST /v1/images/generations", "POST /v1/images/edits"],
    },
    {
      format: "openai-transcriptions",
      kind: "transcription",
      endpoints: ["POST /v1/audio/transcriptions", "POST /v1/audio/translations"],
    },
    { format: "openai-speech", kind: "speech", endpoints: ["POST /v1/audio/speech"] },
    {
      format: "openai-videos",
      kind: "video",
      endpoints: ["POST /v1/videos", "GET /v1/videos/{id}", "GET /v1/videos/{id}/content"],
    },
    { format: "openai-embeddings", kind: "embedding", endpoints: ["POST /v1/embeddings"] },
  ],
  // As the router lists them: the `models` tables, then providers' claims.
  routes: [
    { prefix: "claude", provider: "anthropic" },
    { prefix: "llama", provider: "groq" },
    { prefix: "gemini", provider: "gemini" },
    { prefix: "deepseek", provider: "deepseek" },
    { prefix: "gpt", provider: "openai" },
    { prefix: "whisper", provider: "openai" },
    { prefix: "text-embedding", provider: "openai" },
    { prefix: "sora", provider: "openai" },
  ],
  defaults: [
    { kind: "chat", provider: "openai" },
    { kind: "realtime", provider: "openai" },
    { kind: "responses", provider: "openai" },
    { kind: "image", provider: "openai" },
    { kind: "transcription", provider: "openai" },
    { kind: "speech", provider: "openai" },
    { kind: "video", provider: "openai" },
    { kind: "embedding", provider: "openai" },
  ],
};

const models = [
  ["gpt-4o-mini", "openai", "chat"],
  ["gpt-4.1", "openai", "chat"],
  ["claude-sonnet-4-5", "anthropic", "chat"],
  ["claude-haiku-4-5", "anthropic", "chat"],
  ["gemini-2.5-flash", "gemini", "chat"],
  ["llama-3.3-70b-versatile", "groq", "chat"],
  ["deepseek-reasoner", "deepseek", "chat"],
  ["gpt-realtime", "openai", "realtime"],
  ["gpt-image-1", "openai", "image"],
  ["whisper-1", "openai", "transcription"],
  ["whisper-large-v3", "groq", "transcription"],
  ["gpt-4o-mini-tts", "openai", "speech"],
  ["sora-2", "openai", "video"],
  ["text-embedding-3-small", "openai", "embedding"],
].map(([id, owned_by, kind]) => ({ id, object: "model", owned_by, kind }));

// --- request records ------------------------------------------------------

const records = []; // newest first, at most 1000
const clients = new Set();
let seq = 0;

const pick = (items) => items[Math.floor(Math.random() * items.length)];
const between = (lo, hi) => Math.round(lo + Math.random() * (hi - lo));
const providerFor = (model) =>
  routes.routes.find((r) => model.startsWith(r.prefix))?.provider ?? "openai";

function broadcast(record) {
  const frame = `event: request\ndata: ${JSON.stringify(record)}\n\n`;
  for (const res of clients) res.write(frame);
}

function begin({ path, format, kind, model, stream, at = new Date() }) {
  const record = {
    id: `r_${String(++seq).padStart(6, "0")}`,
    started_at: at.toISOString(),
    method: "POST",
    path,
    format,
    kind,
    model,
    provider: model ? providerFor(model) : null,
    status: null,
    duration_ms: null,
    ttfb_ms: null,
    stream,
    usage: null,
    error: null,
  };
  records.unshift(record);
  records.length = Math.min(records.length, 1000);
  broadcast(record);
  return record;
}

function finish(record, { status, duration, usage = null, error = null }) {
  Object.assign(record, {
    status,
    duration_ms: duration,
    ttfb_ms: Math.round(duration * (0.15 + Math.random() * 0.3)),
    usage,
    error,
  });
  broadcast(record);
}

const failures = [
  [429, "Rate limit reached for requests per minute; retry after 2s"],
  [500, "upstream connection reset by peer"],
  [400, "messages: at least one message is required"],
  [401, "Incorrect API key provided"],
];

// The calls clients make, weighted: mostly chat, some of everything else.
const traffic = [
  [10, { path: "/v1/chat/completions", format: "openai-chat", kind: "chat" }],
  [4, { path: "/v1/messages", format: "anthropic-messages", kind: "chat" }],
  [1, { path: "/v1/images/generations", format: "openai-images", kind: "image" }],
  [1, { path: "/v1/audio/transcriptions", format: "openai-transcriptions", kind: "transcription" }],
  [1, { path: "/v1/audio/speech", format: "openai-speech", kind: "speech" }],
  [1, { path: "/v1/embeddings", format: "openai-embeddings", kind: "embedding" }],
].flatMap(([weight, call]) => Array(weight).fill(call));

function fakeTraffic(at) {
  const call = pick(traffic);
  const model = pick(models.filter((m) => m.kind === call.kind)).id;
  const chat = call.kind === "chat";
  const record = begin({ ...call, model, stream: chat && Math.random() < 0.6, at });
  const failed = Math.random() < 0.06;
  const [status, error] = failed ? pick(failures) : [200, null];
  const usage = chat
    ? { input_tokens: between(12, 2400), output_tokens: between(8, 900) }
    : call.kind === "transcription"
      ? { input_tokens: 0, output_tokens: 0, seconds: between(3, 90) }
      : { input_tokens: between(4, 200), output_tokens: 0 };
  const outcome = {
    status,
    error,
    duration: between(180, chat ? (model.startsWith("llama") ? 900 : 3200) : 4500),
    usage: failed ? null : usage,
  };
  return { record, outcome };
}

// History: a few dozen requests over the last half hour.
for (let i = 0; i < 90; i++) {
  const at = new Date(Date.now() - between(5, 29 * 60) * 1000);
  const { record, outcome } = fakeTraffic(at);
  finish(record, outcome);
}
records.sort((a, b) => (a.started_at < b.started_at ? 1 : -1));

function tick() {
  if (!QUIET) {
    const { record, outcome } = fakeTraffic();
    setTimeout(() => finish(record, outcome), outcome.duration);
  }
  setTimeout(tick, between(700, 4000));
}
tick();

function totals() {
  const done = records.filter((r) => r.status !== null);
  return {
    requests: records.length,
    errors: done.filter((r) => r.status >= 400).length,
    in_flight: records.length - done.length,
    input_tokens: done.reduce((n, r) => n + (r.usage?.input_tokens ?? 0), 0),
    output_tokens: done.reduce((n, r) => n + (r.usage?.output_tokens ?? 0), 0),
  };
}

function providerStats(name) {
  const mine = records.filter((r) => r.provider === name && r.status !== null);
  return {
    requests: mine.length,
    errors: mine.filter((r) => r.status >= 400).length,
    avg_latency_ms: mine.length
      ? Math.round(mine.reduce((n, r) => n + r.duration_ms, 0) / mine.length)
      : null,
  };
}

// --- fake media -------------------------------------------------------------

/** A soft gradient PNG, as base64: what gpt-image-1 answers with. */
function pngImage(prompt, i, size = 384) {
  const hue = (prompt.length * 37 + i * 70) % 360;
  const shade = (h, l) => {
    const a = 0.7 * Math.min(l, 1 - l);
    const f = (n) => l - a * Math.max(-1, Math.min((n + h / 30) % 12 - 3, 9 - ((n + h / 30) % 12), 1));
    return [f(0), f(8), f(4)].map((v) => Math.round(v * 255));
  };
  const rows = Buffer.alloc(size * (size * 3 + 1));
  for (let y = 0; y < size; y++) {
    rows[y * (size * 3 + 1)] = 0;
    for (let x = 0; x < size; x++) {
      const t = (x + y) / (2 * size);
      let [r, g, b] = shade((hue + t * 60) % 360, 0.62 - t * 0.24);
      const near = Math.hypot(x - (110 + i * 50), y - 150) < size * 0.2;
      if (near) [r, g, b] = [r, g, b].map((v) => Math.round(v + (255 - v) * 0.2));
      rows.set([r, g, b], y * (size * 3 + 1) + 1 + x * 3);
    }
  }
  const chunk = (type, data) => {
    const head = Buffer.alloc(4);
    head.writeUInt32BE(data.length);
    const body = Buffer.concat([Buffer.from(type), data]);
    const crc = Buffer.alloc(4);
    crc.writeUInt32BE(crc32(body));
    return Buffer.concat([head, body, crc]);
  };
  const header = Buffer.alloc(13);
  header.writeUInt32BE(size, 0);
  header.writeUInt32BE(size, 4);
  header.set([8, 2, 0, 0, 0], 8);
  const file = Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk("IHDR", header),
    chunk("IDAT", deflateSync(rows)),
    chunk("IEND", Buffer.alloc(0)),
  ]);
  return file.toString("base64");
}

/** A gentle two-note chime as 16-bit mono WAV. */
function chimeWav(seconds = 1.4, rate = 22050) {
  const n = Math.floor(seconds * rate);
  const buf = Buffer.alloc(44 + n * 2);
  buf.write("RIFF", 0);
  buf.writeUInt32LE(36 + n * 2, 4);
  buf.write("WAVEfmt ", 8);
  buf.writeUInt32LE(16, 16);
  buf.writeUInt16LE(1, 20);
  buf.writeUInt16LE(1, 22);
  buf.writeUInt32LE(rate, 24);
  buf.writeUInt32LE(rate * 2, 28);
  buf.writeUInt16LE(2, 32);
  buf.writeUInt16LE(16, 34);
  buf.write("data", 36);
  buf.writeUInt32LE(n * 2, 40);
  for (let i = 0; i < n; i++) {
    const t = i / rate;
    const f = t < seconds / 2 ? 660 : 880;
    const env = Math.exp(-3 * (t % (seconds / 2)));
    buf.writeInt16LE(Math.round(Math.sin(2 * Math.PI * f * t) * env * 9000), 44 + i * 2);
  }
  return buf;
}

const reply = `Here's how a request moves through **respan gateway**:

1. The client format that recognises the request decodes it.
2. Plugins run \`on_client\`, then the router picks a provider.
3. The provider format encodes the call, translating if the formats differ.

\`\`\`bash
curl localhost:8080/v1/chat/completions -d '{"model":"claude-sonnet-4-5", ...}'
\`\`\`

Responses stream back through the same plugins, *event by event*.`;

// --- HTTP -------------------------------------------------------------------

const json = (res, status, body) => {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
};
const notFound = (res, message = "Not found") =>
  json(res, 404, { error: { message, type: "not_found_error" } });

async function readBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  return Buffer.concat(chunks);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function authorized(req) {
  if (!KEY) return true;
  const header = req.headers.authorization ?? "";
  return header === `Bearer ${KEY}` || req.headers["x-api-key"] === KEY;
}

async function chat(req, res) {
  const body = JSON.parse((await readBody(req)).toString() || "{}");
  const model = body.model ?? "gpt-4o-mini";
  const record = begin({
    path: "/v1/chat/completions",
    format: "openai-chat",
    kind: "chat",
    model,
    stream: !!body.stream,
  });
  const started = Date.now();
  const usage = { prompt_tokens: between(20, 80), completion_tokens: reply.split(/\s+/).length };
  const done = () =>
    finish(record, {
      status: 200,
      duration: Date.now() - started,
      usage: { input_tokens: usage.prompt_tokens, output_tokens: usage.completion_tokens },
    });
  const base = { id: `chatcmpl-${record.id}`, object: "chat.completion.chunk", model };

  if (!body.stream) {
    await sleep(600);
    done();
    return json(res, 200, {
      ...base,
      object: "chat.completion",
      choices: [{ index: 0, message: { role: "assistant", content: reply }, finish_reason: "stop" }],
      usage,
    });
  }

  res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
  let closed = false;
  req.on("close", () => (closed = true));
  await sleep(350);
  for (const piece of reply.match(/\S+\s*/g)) {
    if (closed) break;
    res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: { content: piece } }] })}\n\n`);
    await sleep(between(15, 45));
  }
  res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`);
  res.write(`data: ${JSON.stringify({ ...base, choices: [], usage })}\n\n`);
  res.end("data: [DONE]\n\n");
  done();
}

async function media(req, res, { path, format, kind, model, answer }) {
  const raw = await readBody(req);
  const record = begin({ path, format, kind, model, stream: false });
  const started = Date.now();
  await sleep(between(600, 1400));
  finish(record, { status: 200, duration: Date.now() - started });
  answer(raw);
}

// A 64-number vector of a text's letter pairs, so texts that share words are alike.
function hashedVector(text) {
  const vector = new Array(64).fill(0);
  const words = text.toLowerCase().replace(/[^a-z0-9 ]/g, "");
  for (let i = 0; i + 1 < words.length; i++) {
    vector[(words.charCodeAt(i) * 31 + words.charCodeAt(i + 1)) % 64] += 1;
  }
  const norm = Math.hypot(...vector) || 1;
  return vector.map((v) => Number((v / norm).toFixed(6)));
}

const sentences = [
  "This is a mock transcript of the audio you sent.",
  "In the real gateway it comes from the transcription provider.",
  "Each line carries a timestamp when you ask for subtitles.",
];

/** Answers a transcription in the multipart form's `response_format`. */
function transcript(res, raw) {
  const format = /name="response_format"\r\n\r\n(\w+)/.exec(raw.toString("latin1"))?.[1] ?? "json";
  const at = (i, sep) =>
    new Date(i * 3000).toISOString().slice(11, 23).replace(".", sep);
  const end = (i, sep) =>
    new Date(i * 3000 + 2800).toISOString().slice(11, 23).replace(".", sep);
  if (format === "srt" || format === "vtt") {
    const sep = format === "srt" ? "," : ".";
    const cues = sentences.map((line, i) => {
      const times = `${at(i, sep)} --> ${end(i, sep)}\n${line}\n`;
      return format === "srt" ? `${i + 1}\n${times}` : times;
    });
    res.writeHead(200, { "content-type": format === "srt" ? "text/plain" : "text/vtt" });
    return res.end((format === "vtt" ? "WEBVTT\n\n" : "") + cues.join("\n"));
  }
  const text = sentences.join(" ");
  if (format === "text") {
    res.writeHead(200, { "content-type": "text/plain" });
    return res.end(text);
  }
  json(res, 200, { text });
}

// --- video jobs: queued, then rendering for a few seconds, then done ----------

const JOB_MS = 7000;
const jobs = new Map();
const clip = readFileSync(new URL("./clip.webm", import.meta.url));

function jobView(job) {
  const age = Date.now() - job.created;
  const progress = Math.min(100, Math.round((age / JOB_MS) * 100));
  const status =
    progress >= 100 ? (job.fail ? "failed" : "completed") : age < 1200 ? "queued" : "in_progress";
  return {
    id: job.id,
    object: "video",
    model: job.model,
    status,
    progress,
    seconds: job.seconds,
    error:
      status === "failed"
        ? { code: "moderation_blocked", message: "The prompt was blocked by the provider's safety system." }
        : null,
  };
}

async function videos(req, res, path) {
  const [, , , id, rest] = path.split("/");
  if (req.method === "POST" && !id) {
    const body = JSON.parse((await readBody(req)).toString() || "{}");
    const job = {
      id: `video_${Math.random().toString(36).slice(2, 10)}`,
      created: Date.now(),
      model: body.model ?? "sora-2",
      seconds: body.seconds ?? "4",
      fail: /fail/i.test(body.prompt ?? ""),
    };
    jobs.set(job.id, job);
    const record = begin({ path, format: "openai-videos", kind: "video", model: job.model, stream: false });
    finish(record, { status: 200, duration: between(200, 500) });
    return json(res, 200, jobView(job));
  }
  const job = jobs.get(id);
  if (!job) return notFound(res, `No video ${id}`);
  if (req.method === "GET" && !rest) return json(res, 200, jobView(job));
  if (req.method === "GET" && rest === "content") {
    if (jobView(job).status !== "completed") {
      return json(res, 409, { error: { message: "The video isn't ready" } });
    }
    res.writeHead(200, { "content-type": "video/webm" });
    return res.end(clip);
  }
  notFound(res, `No format serves ${req.method} ${path}`);
}

const routesTable = {
  "GET /healthz": (_req, res) => res.end("ok"),
  "GET /ui/api/overview": (_req, res) =>
    json(res, 200, {
      version: "0.1.0",
      listen: `127.0.0.1:${PORT}`,
      started_at: startedAt.toISOString(),
      uptime_seconds: Math.floor((Date.now() - startedAt) / 1000) + 7_380,
      auth_required: !!KEY,
      config_source: "./gateway.yaml",
      totals: totals(),
    }),
  "GET /ui/api/providers": (_req, res) =>
    json(res, 200, { providers: providers.map((p) => ({ ...p, ...providerStats(p.name) })) }),
  "GET /ui/api/routes": (_req, res) => json(res, 200, routes),
  "GET /ui/api/requests": (req, res) => {
    const limit = Number(new URL(req.url, "http://x").searchParams.get("limit") ?? 100);
    json(res, 200, { requests: records.slice(0, limit) });
  },
  "GET /ui/api/events": (req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    res.write(": connected\n\n");
    clients.add(res);
    const ping = setInterval(() => res.write(": ping\n\n"), 15_000);
    req.on("close", () => {
      clearInterval(ping);
      clients.delete(res);
    });
  },
  "GET /v1/models": (_req, res) => json(res, 200, { object: "list", data: models }),
  "POST /v1/chat/completions": chat,
  "POST /v1/images/generations": (req, res) =>
    media(req, res, {
      path: "/v1/images/generations",
      format: "openai-images",
      kind: "image",
      model: "gpt-image-1",
      answer: (raw) => {
        const body = JSON.parse(raw.toString() || "{}");
        const n = Math.min(4, Number(body.n ?? 1));
        json(res, 200, {
          created: Math.floor(Date.now() / 1000),
          data: Array.from({ length: n }, (_, i) => ({ b64_json: pngImage(body.prompt ?? "", i) })),
        });
      },
    }),
  "POST /v1/audio/transcriptions": (req, res) =>
    media(req, res, {
      path: "/v1/audio/transcriptions",
      format: "openai-transcriptions",
      kind: "transcription",
      model: "whisper-1",
      answer: (raw) => transcript(res, raw),
    }),
  "POST /v1/embeddings": (req, res) =>
    media(req, res, {
      path: "/v1/embeddings",
      format: "openai-embeddings",
      kind: "embedding",
      model: "text-embedding-3-small",
      answer: (raw) => {
        const body = JSON.parse(raw.toString() || "{}");
        const inputs = [body.input].flat();
        json(res, 200, {
          object: "list",
          model: body.model,
          data: inputs.map((text, index) => ({
            object: "embedding",
            index,
            embedding: hashedVector(String(text)),
          })),
          usage: { prompt_tokens: inputs.length * 4, total_tokens: inputs.length * 4 },
        });
      },
    }),
  "POST /v1/audio/speech": (req, res) =>
    media(req, res, {
      path: "/v1/audio/speech",
      format: "openai-speech",
      kind: "speech",
      model: "gpt-4o-mini-tts",
      answer: () => {
        res.writeHead(200, { "content-type": "audio/wav" });
        res.end(chimeWav());
      },
    }),
};

createServer((req, res) => {
  const path = new URL(req.url, "http://x").pathname;
  if (path !== "/healthz" && !authorized(req)) {
    return json(res, 401, {
      error: { message: "A gateway key is required", type: "authentication_error" },
    });
  }
  const route = path.startsWith("/ui/api/requests/") ? null : routesTable[`${req.method} ${path}`];
  if (route) {
    return Promise.resolve(route(req, res)).catch((err) => {
      console.error(err);
      if (!res.headersSent) json(res, 500, { error: { message: String(err), type: "mock_error" } });
    });
  }
  if (path === "/v1/videos" || path.startsWith("/v1/videos/")) {
    return videos(req, res, path).catch((err) =>
      json(res, 500, { error: { message: String(err), type: "mock_error" } }),
    );
  }
  if (req.method === "GET" && path.startsWith("/ui/api/requests/")) {
    const id = decodeURIComponent(path.slice("/ui/api/requests/".length));
    const record = records.find((r) => r.id === id);
    return record ? json(res, 200, record) : notFound(res, "Request evicted or unknown");
  }
  notFound(res, `No format serves ${req.method} ${path}`);
}).listen(PORT, "127.0.0.1", () => {
  console.log(`mock gateway on http://127.0.0.1:${PORT}${KEY ? " (key required)" : ""}`);
});
