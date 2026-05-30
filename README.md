# ✈ Chat-style streaming UX for any backend

> **Stream the work, not just the answer.** A tiny, dependency-free skeleton that reproduces the
> claude.ai / Codex "watch it think" UX — live, step-by-step reasoning streamed to the browser —
> for *any* multi-step backend, not just chatbots.

![runtime: Bun](https://img.shields.io/badge/runtime-Bun-black)
![dependencies: 0](https://img.shields.io/badge/dependencies-0-brightgreen)
![license: MIT](https://img.shields.io/badge/license-MIT-blue)

Two files. No frontend framework, no client SSE library, no build step. One `POST` request that
answers with `text/event-stream` and streams partial results back **on the same request** — the
exact pattern modern AI chat apps use. The included demo is a **travel assistant** ("where can I
fly that's warm, non-stop, under 4 hours, the day after tomorrow?"), but the travel logic is just
a placeholder for *your* pipeline.

---

## The idea in 30 seconds

Open devtools on claude.ai and watch a message go out. The reply comes back as
`Content-Type: text/event-stream` — Server-Sent Events — but over a **POST**, on the same request
that carried your prompt. That's unusual: the browser's built-in `EventSource` only does `GET` and
can't send a body, so the textbook SSE setup needs two requests (a `POST` to start a job, then a
separate `GET` to subscribe).

The single-`POST` approach collapses that. The price is you give up `EventSource` and read the
stream yourself with `fetch` — which turns out to be ~15 lines. This repo is that pattern, isolated
and reusable.

```
Classic SSE                          This project
───────────                          ────────────
POST /start        → { jobId }       POST /assist   → text/event-stream (same request)
GET  /events?id=…  (EventSource)     fetch() + response.body
                                         .pipeThrough(new TextDecoderStream())
                                         .getReader()
```

The LLM here is **one link in a fixed chain**, not a tool-calling agent. It does a single job
(turn a sentence into `{ days_from_now, weather, hours }`) and gets out of the way. Everything
after it is deterministic API plumbing that narrates itself.

---

## What the demo does

A query like *"somewhere hot, non-stop, under 4h, in 2 days"* streams these steps live:

```
Browser                                  Bun server  /assist
   │  POST { q: "..." }
   ├────────────────────────────────────────▶
   │                                          │  one ReadableStream, many SSE frames:
   │  ◀── understand ─────────────────────────┤  "Understanding your request…"
   │  ◀── intent ─────────────────────────────┤  LLM (Groq/Cerebras) → {days, weather, hours}
   │  ◀── geo ────────────────────────────────┤  ip-api          → your city
   │  ◀── departure ──────────────────────────┤  AirLabs /nearby  → home airport
   │  ◀── routes ─────────────────────────────┤  AirLabs /routes  → non-stop destinations
   │  ◀── airports ───────────────────────────┤  AirLabs /airports→ coords + haversine distance
   │  ◀── result ─────────────────────────────┤  OpenWeather      → forecast-filtered list
   │  ◀── done ───────────────────────────────┤
   │                                          │
  reader.read() loop                     controller.close()
```

Each step also ships the **raw upstream JSON** in a collapsed "raw response" panel, so you can
inspect exactly what every service returned (toggle off with `DEBUG=false`).

---

## Quick start

You need free API keys for **Groq** (or **Cerebras**), **AirLabs**, and **OpenWeather**.

```bash
# 1. install Bun — https://bun.sh
curl -fsSL https://bun.sh/install | bash

# 2. clone and configure
git clone https://github.com/airlabs-co/assistant-demo
cd assistant-demo
cp .env.example .env        # then paste your keys

# 3. run
bun run server.js
# open http://localhost:3000
```

That's it — no `npm install`, because there are no dependencies.

---

## Configuration

All via `.env` (Bun loads it automatically).

| Variable | Default | What it does |
|---|---|---|
| `LLM_PROVIDER` | `groq` | `groq` or `cerebras` — both speak the OpenAI chat-completions dialect |
| `GROQ_API_KEY` | — | from [console.groq.com](https://console.groq.com) |
| `GROQ_MODEL` | `llama-3.3-70b-versatile` | any Groq chat model |
| `CEREBRAS_API_KEY` | — | from [cloud.cerebras.ai](https://cloud.cerebras.ai) |
| `CEREBRAS_MODEL` | `llama-3.3-70b` | any Cerebras chat model |
| `AIRLABS_API_KEY` | — | from [airlabs.co](https://airlabs.co) (nearby + routes + airports) |
| `OPENWEATHER_API_KEY` | — | from [openweathermap.org](https://openweathermap.org/api) |
| `PORT` | `3000` | HTTP port |
| `STEP_DELAY_MS` | `280` | pause between streamed steps so the reveal is legible (set `0` to disable) |
| `MAX_WEATHER_LOOKUPS` | `25` | cap weather calls per request (free-tier friendly) |
| `DEBUG` | `true` | stream raw upstream JSON into the UI panels; set `false` in production |
| `DEMO_LAT` / `DEMO_LON` / `DEMO_CITY` / `DEMO_COUNTRY` / `DEMO_CC` | — | force a location instead of IP lookup (handy for reproducible screen recordings) |

---

## How it works

### Server (`server.js`)

`Bun.serve` routes `POST /assist`, builds a `ReadableStream`, and runs the pipeline inside it.
Every milestone is one SSE frame:

```js
const stream = new ReadableStream({
  async start(controller) {
    const send = (o) =>
      controller.enqueue(enc.encode(`data: ${JSON.stringify(o)}\n\n`));
    await runPipeline(query, clientIP, send);   // each step calls send(...)
    controller.close();
  },
});

return new Response(stream, {
  headers: {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",  // stop CDN/proxy from buffering
    "X-Accel-Buffering": "no",                  // stop nginx from buffering
  },
});
```

### Client (`index.html`)

No `EventSource`. Read the POST response as a stream and parse SSE frames by hand:

```js
const reader = res.body
  .pipeThrough(new TextDecoderStream())  // bytes → UTF-8, handles split multibyte chars
  .getReader();

let buffer = "";
while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  buffer += value;
  const frames = buffer.split("\n\n");   // SSE frames end in a blank line
  buffer = frames.pop();                 // keep the trailing partial frame
  for (const frame of frames) {
    const line = frame.split("\n").find((l) => l.startsWith("data:"));
    if (line) handle(JSON.parse(line.slice(5).trim()));
  }
}
```

### The event contract

The only API between back and front is the shape of each frame:

| field | type | meaning |
|---|---|---|
| `id` | number | incrementing step counter |
| `kind` | string | `understand` · `intent` · `geo` · `departure` · `routes` · `airports` · `result` · `error` · `done` |
| `text` | string | the human-readable line shown in the timeline |
| `data` | object? | structured payload (e.g. the final destinations, rendered as cards) |
| `debug` | any? | raw upstream JSON for the collapsible "raw response" panel |

---

## Make it your own (startup skeleton)

The travel pipeline is a stand-in. To repurpose this for your product, you touch **three places**:

1. **`runPipeline()` in `server.js`** — replace the chain of steps with *your* slow work: DB
   queries, third-party APIs, scoring, ranking, whatever. After each meaningful step, call
   `send({ kind, text, data, debug })`. That's the whole game.
2. **`extractIntent()`** — keep it if you want free-text input parsed by an LLM; delete it and
   accept a structured request body if you don't.
3. **Frontend `handle()` / `addStep()` / `renderResults()` in `index.html`** — map each `kind`
   to UI. Add a renderer for your own `data` shape (the demo renders destination cards; you might
   render a table, a map, a diff, a quote).

Keep the response headers, `idleTimeout`, and the buffer-split reader as-is — that's the
domain-agnostic machinery.

**The pattern fits anything that's a sequence of slow steps hiding behind a spinner**, e.g.:

- logistics / B2B routing and multi-leg planning
- a research agent assembling and citing sources as it goes
- an insurance quote walking through its risk checks out loud
- CI/deploy dashboards, data imports, multi-step checkout, fraud review

---

## Production notes & gotchas

- **Proxy buffering kills SSE.** Reverse proxies buffer responses by default — the user sees
  nothing, then everything at once. The `X-Accel-Buffering: no` + `Cache-Control: no-transform`
  headers cover nginx and most CDNs.
- **Bump the idle timeout.** A pipeline waiting on several APIs can outlive a default socket
  timeout; this repo sets Bun's `idleTimeout` to 120s.
- **Turn `DEBUG` off in production** so you stop shipping raw third-party payloads to clients.
  (Responses never contain your API keys — keys live in the request URL, not the body — but raw
  payloads are still noise/leakage you probably don't want public.)
- **Mind free-tier limits.** Weather lookups are capped (`MAX_WEATHER_LOOKUPS`) and run in small
  concurrent batches; LLM calls use JSON mode so you parse structure, not prose.
- **Forecast horizon.** OpenWeather's free 5-day/3-hour forecast covers `days_from_now` 0–5.

---

## Project structure

```
.
├── server.js        # Bun.serve backend + the streaming pipeline
├── index.html       # search box + live "flight-path" timeline (the SSE reader lives here)
├── .env.example     # copy to .env and add your keys
└── README.md
```

---

## Tech & data

- **Runtime:** [Bun](https://bun.sh) — fast cold start, first-class streaming `Response`
- **LLM:** [Groq](https://console.groq.com) or [Cerebras](https://cloud.cerebras.ai) free tiers (OpenAI-compatible)
- **Flights:** [AirLabs](https://airlabs.co) — nearby airports, [routes](https://airlabs.co/docs/routes), airport coordinates
- **Weather:** [OpenWeather](https://openweathermap.org/api) 5-day / 3-hour forecast
- **Geo:** [ip-api](https://ip-api.com)

## License

MIT © 2026 AirLabs — see [`LICENSE`](LICENSE). Do whatever you want; a star is appreciated.

> The data providers above have their own terms — check each before going to production.
