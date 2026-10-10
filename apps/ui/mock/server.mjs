// A stand-in for the gateway, for developing and screenshotting the UI
// without the Rust binary. It serves the admin API from the UI contract with
// generated traffic, and fake /v1 endpoints for the playground (chat, images,
// transcription and speech; video answers 404 to show the "no provider"
// state). Zero dependencies.
//
//   pnpm mock                      # http://127.0.0.1:8080, open
//   MOCK_KEY=secret pnpm mock      # every call needs "Bearer secret"
//   PORT=8081 MOCK_QUIET=1 pnpm mock   # another port; no generated traffic

import { createServer } from "node:http";

const PORT = Number(process.env.PORT ?? 8080);
const KEY = process.env.MOCK_KEY ?? "";
const QUIET = !!process.env.MOCK_QUIET;
const startedAt = new Date();

const providers = [
  {
    name: "openai",
    vendor: "openai",
    formats: ["openai-chat", "openai-images", "openai-transcriptions", "openai-speech"],
    kinds: ["chat", "image", "transcription", "speech"],
    base_url: "https://api.openai.com/v1",
    credentials: "gateway",
  },
  {
    name: "anthropic",
    vendor: "anthropic",
    formats: ["anthropic-messages"],
    kinds: ["chat"],
    base_url: "https://api.anthropic.com",
    credentials: "gateway",
  },
  {
    name: "groq",
    vendor: "groq",
    formats: ["groq-chat"],
    kinds: ["chat"],
    base_url: "https://api.groq.com/openai/v1",
    credentials: "client",
  },
];

const routes = {
  serve: [
    { format: "openai-chat", kind: "chat", endpoints: ["POST /v1/chat/completions"] },
    { format: "anthropic-messages", kind: "chat", endpoints: ["POST /v1/messages"] },
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
  ],
  routes: [
    { prefix: "claude", provider: "anthropic" },
    { prefix: "llama", provider: "groq" },
  ],
  defaults: [
    { kind: "chat", provider: "openai" },
    { kind: "image", provider: "openai" },
    { kind: "transcription", provider: "openai" },
    { kind: "speech", provider: "openai" },
  ],
};

const models = [
  ["gpt-4o-mini", "openai", "chat"],
  ["gpt-4.1", "openai", "chat"],
  ["claude-sonnet-4-5", "anthropic", "chat"],
  ["claude-haiku-4-5", "anthropic", "chat"],
  ["llama-3.3-70b-versatile", "groq", "chat"],
  ["gpt-image-1", "openai", "image"],
  ["whisper-1", "openai", "transcription"],
  ["gpt-4o-mini-tts", "openai", "speech"],
].map(([id, owned_by, kind]) => ({ id, object: "model", owned_by, kind }));

// --- request records ------------------------------------------------------

const records = []; // newest first, at most 1000
const clients = new Set();
let seq = 0;

const pick = (items) => items[Math.floor(Math.random() * items.length)];
const between = (lo, hi) => Math.round(lo + Math.random() * (hi - lo));
const providerFor = (model) =>
  model.startsWith("claude") ? "anthropic" : model.startsWith("llama") ? "groq" : "openai";

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

function fakeTraffic(at) {
  const anthropicClient = Math.random() < 0.3;
  const model = pick(models.filter((m) => m.kind === "chat")).id;
  const record = begin({
    path: anthropicClient ? "/v1/messages" : "/v1/chat/completions",
    format: anthropicClient ? "anthropic-messages" : "openai-chat",
    kind: "chat",
    model,
    stream: Math.random() < 0.6,
    at,
  });
  const failed = Math.random() < 0.06;
  const [status, error] = failed ? pick(failures) : [200, null];
  const outcome = {
    status,
    error,
    duration: between(180, model.startsWith("llama") ? 900 : 3200),
    usage: failed ? null : { input_tokens: between(12, 2400), output_tokens: between(8, 900) },
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

function svgImage(prompt, i) {
  const hue = (prompt.length * 37 + i * 70) % 360;
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="1024" height="1024" viewBox="0 0 1024 1024">
<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
<stop offset="0" stop-color="hsl(${hue} 70% 62%)"/><stop offset="1" stop-color="hsl(${(hue + 60) % 360} 70% 38%)"/></linearGradient></defs>
<rect width="1024" height="1024" fill="url(#g)"/>
<circle cx="${300 + i * 120}" cy="380" r="170" fill="white" fill-opacity="0.18"/>
<text x="64" y="940" font-family="sans-serif" font-size="40" fill="white" fill-opacity="0.85">${prompt
    .slice(0, 40)
    .replace(/[<&>]/g, "")}</text></svg>`;
  return `data:image/svg+xml;base64,${Buffer.from(svg).toString("base64")}`;
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

async function media(req, res, { path, kind, model, answer }) {
  const raw = await readBody(req);
  const record = begin({ path, format: "openai-" + kind, kind, model, stream: false });
  const started = Date.now();
  await sleep(between(600, 1400));
  finish(record, { status: 200, duration: Date.now() - started });
  answer(raw);
}

const routesTable = {
  "GET /healthz": (_req, res) => res.end("ok"),
  "GET /ui/api/overview": (_req, res) =>
    json(res, 200, {
      version: "0.0.0-mock",
      listen: `127.0.0.1:${PORT}`,
      started_at: startedAt.toISOString(),
      uptime_seconds: Math.floor((Date.now() - startedAt) / 1000) + 7_380,
      auth_required: !!KEY,
      config_source: "gateway.yaml",
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
      kind: "image",
      model: "gpt-image-1",
      answer: (raw) => {
        const body = JSON.parse(raw.toString() || "{}");
        const n = Math.min(4, Number(body.n ?? 1));
        json(res, 200, {
          created: Math.floor(Date.now() / 1000),
          data: Array.from({ length: n }, (_, i) => ({ url: svgImage(body.prompt ?? "", i) })),
        });
      },
    }),
  "POST /v1/audio/transcriptions": (req, res) =>
    media(req, res, {
      path: "/v1/audio/transcriptions",
      kind: "transcription",
      model: "whisper-1",
      answer: (raw) =>
        json(res, 200, {
          text: `This is a mock transcript of ${Math.round(raw.length / 1024)} KB of audio. In the real gateway it comes from the transcription provider.`,
        }),
    }),
  "POST /v1/audio/speech": (req, res) =>
    media(req, res, {
      path: "/v1/audio/speech",
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
  if (req.method === "GET" && path.startsWith("/ui/api/requests/")) {
    const id = decodeURIComponent(path.slice("/ui/api/requests/".length));
    const record = records.find((r) => r.id === id);
    return record ? json(res, 200, record) : notFound(res, "Request evicted or unknown");
  }
  notFound(res, `No format serves ${req.method} ${path}`);
}).listen(PORT, "127.0.0.1", () => {
  console.log(`mock gateway on http://127.0.0.1:${PORT}${KEY ? " (key required)" : ""}`);
});
