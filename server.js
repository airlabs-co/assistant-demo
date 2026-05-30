// server.js — Travel Assistant streaming backend (Bun.sh, no TypeScript)
//
// The whole point of this demo: ONE POST request to /assist that immediately
// answers with `Content-Type: text/event-stream` and streams partial results
// back on that same request — the way claude.ai and Codex do it — instead of
// the classic "POST to start, then open a separate GET EventSource" dance.
//
// Run:  bun run server.js     (Bun auto-loads .env)
// Open: http://localhost:3000

const PORT = Number(process.env.PORT || 3000);

// --- Provider switch (both are OpenAI-compatible chat/completions endpoints) ---
// LLM_PROVIDER = "groq" (default) | "cerebras"
const PROVIDER = (process.env.LLM_PROVIDER || "groq").toLowerCase();
const LLM = {
    groq: {
        url: "https://api.groq.com/openai/v1/chat/completions",
        key: process.env.GROQ_API_KEY,
        model: process.env.GROQ_MODEL || "llama-3.3-70b-versatile",
    },
    cerebras: {
        url: "https://api.cerebras.ai/v1/chat/completions",
        key: process.env.CEREBRAS_API_KEY,
        model: process.env.CEREBRAS_MODEL || "llama-3.3-70b",
    },
}[PROVIDER];

const AIRLABS_KEY = process.env.AIRLABS_API_KEY;
const OWM_KEY = process.env.OPENWEATHER_API_KEY;

// Attach the raw upstream JSON to each streamed step so it can be inspected in
// the UI's collapsible "raw response" panels. Set DEBUG=false in production to
// stop shipping raw third-party payloads down the wire. Responses never contain
// your API keys (keys live in the request URL, not the response body).
const DEBUG = (process.env.DEBUG ?? "true").toLowerCase() !== "false";
const dbg = (v) => (DEBUG ? v : undefined); // -> undefined keys are dropped by JSON.stringify

// Tiny artificial pause between steps so the streaming reveal is legible on
// screen (and in a screen-recorded .gif). Set STEP_DELAY_MS=0 to disable.
const STEP_DELAY_MS = Number(process.env.STEP_DELAY_MS ?? 280);

// Temperature buckets in °C — tweak to taste.
const TEMP = { coldMax: 20, hotMin: 25 };
// Don't fan out weather lookups to a whole hub at once (be kind to free tiers).
const MAX_WEATHER_LOOKUPS = Number(process.env.MAX_WEATHER_LOOKUPS || 25);
const WEATHER_CONCURRENCY = 5;

const DOW = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]; // AirLabs `days` codes

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function haversineKm(a, b) {
    const R = 6371;
    const toRad = (d) => (d * Math.PI) / 180;
    const dLat = toRad(b.lat - a.lat);
    const dLng = toRad(b.lng - a.lng);
    const s =
        Math.sin(dLat / 2) ** 2 +
        Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2;
    return Math.round(2 * R * Math.asin(Math.sqrt(s)));
}

function classifyTemp(c) {
    if (c <= TEMP.coldMax) return "cold";
    if (c >= TEMP.hotMin) return "hot";
    return "normal";
}

// AirLabs wraps payloads as { request, response, terms }. `response` is an array
// for list endpoints (routes, airports) and an object for /nearby.
function unwrap(json) {
    return json && "response" in json ? json.response : json;
}

async function getJSON(url, init) {
    const res = await fetch(url, init);
    if (!res.ok) throw new Error(`${res.status} ${res.statusText} for ${new URL(url).pathname}`);
    return res.json();
}

// ----------------------------------------------------------------------------
// External services
// ----------------------------------------------------------------------------

// 1) Geolocation by IP. ip-api free tier is HTTP-only and geolocates whatever IP
//    you query; with no IP it uses the caller's public IP (good for localhost).
async function geoFromIP(ip) {
    if (process.env.DEMO_LAT && process.env.DEMO_LON) {
        const geo = {
            lat: Number(process.env.DEMO_LAT),
            lon: Number(process.env.DEMO_LON),
            city: process.env.DEMO_CITY || "Demo City",
            country: process.env.DEMO_COUNTRY || "",
            countryCode: process.env.DEMO_CC || "",
        };
        return { geo, raw: { source: "DEMO_* env override", ...geo } };
    }
    const isPrivate =
        !ip ||
        ip === "::1" ||
        ip.startsWith("127.") ||
        ip.startsWith("10.") ||
        ip.startsWith("192.168.") ||
        /^172\.(1[6-9]|2\d|3[0-1])\./.test(ip);
    const suffix = isPrivate ? "" : encodeURIComponent(ip);
    const url = `http://ip-api.com/json/${suffix}?fields=status,message,lat,lon,city,country,countryCode`;
    const j = await getJSON(url);
    if (j.status !== "success") throw new Error(`ip-api: ${j.message || "lookup failed"}`);
    return { geo: j, raw: j };
}

// 2) LLM intent extraction → strict JSON (used here as ONE link in the chain,
//    not as a classic tool-calling agent).
async function extractIntent(text) {
    if (!LLM?.key) throw new Error(`${PROVIDER.toUpperCase()}_API_KEY is missing`);
    const sys =
        "You convert a traveler's free-text wish into STRICT JSON. Return ONLY a JSON " +
        "object, no prose, with keys: " +
        "days_from_now (integer 0-5; today=0, tomorrow=1, day after tomorrow=2), " +
        "weather (one of 'cold','normal','hot'; map warm/sunny/beach->hot, mild->normal, " +
        "cool/snow->cold), " +
        "hours (number 0-12, max non-stop flight hours accepted; default 12), " +
        "summary (one short first-person sentence restating the plan).";
    const body = {
        model: LLM.model,
        temperature: 0.2,
        response_format: { type: "json_object" },
        messages: [
            { role: "system", content: sys },
            { role: "user", content: text },
        ],
    };
    const j = await getJSON(LLM.url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${LLM.key}` },
        body: JSON.stringify(body),
    });
    const raw = j.choices?.[0]?.message?.content ?? "{}";
    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch {
        parsed = JSON.parse(raw.slice(raw.indexOf("{"), raw.lastIndexOf("}") + 1));
    }
    const intent = {
        days_from_now: Math.min(5, Math.max(0, Math.round(Number(parsed.days_from_now) || 0))),
        weather: ["cold", "normal", "hot"].includes(parsed.weather) ? parsed.weather : "normal",
        hours: Math.min(12, Math.max(0, Number(parsed.hours) || 12)) || 12,
        summary: String(parsed.summary || "Planning your trip"),
    };
    return { intent, raw: parsed };
}

// 3) Nearest sensible departure airport. We pick the most popular airport in the
//    radius (the city's real hub usually has scheduled routes); swap for strict
//    nearest if you prefer.
async function nearestDeparture(lat, lng) {
    const url = `https://airlabs.co/api/v9/nearby?lat=${lat}&lng=${lng}&distance=120&api_key=${AIRLABS_KEY}`;
    const json = await getJSON(url);
    const r = unwrap(json);
    const airports = (r.airports || []).filter((a) => a.iata_code && a.lat && a.lng);
    if (!airports.length) throw new Error("no nearby airport with an IATA code");
    airports.sort((a, b) => (b.popularity || 0) - (a.popularity || 0));
    return { dep: airports[0], raw: r };
}

// 4) Routes DB → every scheduled non-stop out of the departure airport.
//    Docs: https://airlabs.co/docs/routes
async function routesFrom(depIata) {
    const url =
        `https://airlabs.co/api/v9/routes?dep_iata=${depIata}` +
        `&_fields=airline_iata,flight_iata,arr_iata,dep_time,duration,days&api_key=${AIRLABS_KEY}`;
    const json = await getJSON(url);
    return { routes: unwrap(json) || [], raw: unwrap(json) };
}

// 5) Airports DB → resolve coordinates + names for a batch of IATA codes.
async function airportsByCodes(codes) {
    const url =
        `https://airlabs.co/api/v9/airports?iata_code=${codes.join(",")}` +
        `&_fields=name,iata_code,city,country_code,lat,lng&api_key=${AIRLABS_KEY}`;
    const json = await getJSON(url);
    const list = unwrap(json) || [];
    const map = new Map();
    for (const a of list) map.set(a.iata_code, a);
    return { map, raw: list };
}

// 6) OpenWeather 5-day / 3-hour forecast, reduced to the target day.
async function forecastForDay(lat, lon, targetDate) {
    const url =
        `https://api.openweathermap.org/data/2.5/forecast?lat=${lat}&lon=${lon}` +
        `&units=metric&appid=${OWM_KEY}`;
    const j = await getJSON(url);
    const dayStr = targetDate.toISOString().slice(0, 10);
    let slots = (j.list || []).filter((s) => (s.dt_txt || "").startsWith(dayStr));
    if (!slots.length && j.list?.length) slots = j.list.slice(-8); // fall back to last day
    if (!slots.length) return { weather: null, raw: j };
    const temps = slots.map((s) => s.main.temp);
    const maxTemp = Math.round(Math.max(...temps));
    const midday =
        slots.find((s) => (s.dt_txt || "").includes("12:00:00")) || slots[Math.floor(slots.length / 2)];
    return { weather: { temp: maxTemp, condition: midday.weather?.[0]?.main || "—" }, raw: j };
}

// ----------------------------------------------------------------------------
// The pipeline — each `send()` is an SSE frame the browser renders immediately.
// ----------------------------------------------------------------------------
async function runPipeline(query, clientIP, send) {
    let n = 0;
    const step = async (payload) => {
        if (STEP_DELAY_MS) await sleep(STEP_DELAY_MS);
        send({ id: ++n, ...payload });
    };

    await step({ kind: "understand", text: "Understanding your request…" });

    // Fire geolocation and intent extraction concurrently.
    const geoP = geoFromIP(clientIP);
    const intentP = extractIntent(query);

    const { intent, raw: intentRaw } = await intentP;
    await step({
        kind: "intent",
        text: intent.summary,
        data: { days_from_now: intent.days_from_now, weather: intent.weather, hours: intent.hours },
        debug: dbg(intentRaw),
    });

    const { geo, raw: geoRaw } = await geoP;
    await step({
        kind: "geo",
        text: `Pinned you near ${geo.city}, ${geo.countryCode || geo.country}.`,
        debug: dbg(geoRaw),
    });

    const { dep, raw: nearbyRaw } = await nearestDeparture(geo.lat, geo.lon);
    await step({
        kind: "departure",
        text: `Departing from ${dep.name} (${dep.iata_code}).`,
        debug: dbg(nearbyRaw),
    });

    // Target weekday = today + days_from_now, in AirLabs' 3-letter code.
    const target = new Date();
    target.setDate(target.getDate() + intent.days_from_now);
    const dow = DOW[target.getDay()];
    const dateLabel = target.toLocaleDateString("en-US", { weekday: "long", month: "short", day: "numeric" });

    const { routes, raw: routesRaw } = await routesFrom(dep.iata_code);
    const byArr = new Map(); // arr_iata -> { minMinutes, flights[] }
    for (const r of routes) {
        if (Array.isArray(r.days) && !r.days.includes(dow)) continue; // not flying that day
        const e = byArr.get(r.arr_iata) || { minMinutes: Infinity, flights: [] };
        if (r.duration) e.minMinutes = Math.min(e.minMinutes, r.duration);
        if (e.flights.length < 3 && r.flight_iata)
            e.flights.push({ flight: r.flight_iata, airline: r.airline_iata, dep_time: r.dep_time });
        byArr.set(r.arr_iata, e);
    }
    await step({
        kind: "routes",
        text: `${byArr.size} non-stop destinations on ${dow.toUpperCase()}. Measuring distances…`,
        debug: dbg(routesRaw),
    });

    // Resolve coordinates, compute distance + flight-time, filter by max hours.
    const codes = [...byArr.keys()];
    let airports = new Map();
    let airportsRaw = null;
    if (codes.length) ({ map: airports, raw: airportsRaw } = await airportsByCodes(codes));
    const depPt = { lat: dep.lat, lng: dep.lng };
    let candidates = [];
    for (const [iata, info] of byArr) {
        const a = airports.get(iata);
        if (!a || !a.lat || !a.lng) continue;
        const distance_km = haversineKm(depPt, { lat: a.lat, lng: a.lng });
        // Prefer real schedule duration; otherwise estimate from distance.
        const est_hours =
            info.minMinutes !== Infinity ? info.minMinutes / 60 : distance_km / 750 + 0.75;
        if (est_hours > intent.hours + 0.25) continue; // too long, drop it
        candidates.push({
            iata,
            name: a.name,
            city: a.city,
            country_code: a.country_code,
            lat: a.lat,
            lng: a.lng,
            distance_km,
            est_hours: Math.round(est_hours * 10) / 10,
            flights: info.flights,
        });
    }
    candidates.sort((x, y) => x.distance_km - y.distance_km);
    candidates = candidates.slice(0, MAX_WEATHER_LOOKUPS);
    await step({
        kind: "airports",
        text: `Resolved ${candidates.length} airports within ${intent.hours}h. Reading the sky for ${dateLabel}…`,
        debug: dbg(airportsRaw),
    });

    // Weather lookups in small concurrent batches; keep matches for the band asked.
    // weatherRaw collects one raw OpenWeather response per candidate (bounded by
    // MAX_WEATHER_LOOKUPS) for the result step's "raw response" panel.
    const matches = [];
    const weatherRaw = [];
    for (let i = 0; i < candidates.length; i += WEATHER_CONCURRENCY) {
        const batch = candidates.slice(i, i + WEATHER_CONCURRENCY);
        const results = await Promise.all(
            batch.map(async (c) => {
                try {
                    const { weather, raw } = await forecastForDay(c.lat, c.lon ?? c.lng, target);
                    if (DEBUG) weatherRaw.push({ iata: c.iata, city: c.city, response: raw });
                    return weather
                        ? { ...c, temp_c: weather.temp, condition: weather.condition, band: classifyTemp(weather.temp) }
                        : null;
                } catch (e) {
                    if (DEBUG) weatherRaw.push({ iata: c.iata, city: c.city, error: String(e?.message || e) });
                    return null;
                }
            })
        );
        for (const r of results) if (r && r.band === intent.weather) matches.push(r);
    }
    matches.sort((a, b) => a.est_hours - b.est_hours);

    await step({
        kind: "result",
        text: matches.length
            ? `Here are ${matches.length} ${intent.weather} destinations for ${dateLabel}:`
            : `No ${intent.weather} non-stop spots within ${intent.hours}h on ${dateLabel}. Try widening the window.`,
        data: { from: { name: dep.name, iata: dep.iata_code }, destinations: matches },
        debug: dbg(weatherRaw),
    });

    send({ id: ++n, kind: "done" });
}

// ----------------------------------------------------------------------------
// HTTP server — static index.html + the streaming POST /assist endpoint.
// ----------------------------------------------------------------------------
const enc = new TextEncoder();

Bun.serve({
    port: PORT,
    idleTimeout: 120, // keep the stream alive long enough for the whole chain
    development: false,
    async fetch(req, server) {
        const url = new URL(req.url);

        if (req.method === "GET" && url.pathname === "/") {
            return new Response(Bun.file("index.html"), {
                headers: { "Content-Type": "text/html; charset=utf-8" },
            });
        }

        if (req.method === "POST" && url.pathname === "/assist") {
            let query = "";
            try {
                ({ q: query } = await req.json());
            } catch {
                return new Response("Bad JSON", { status: 400 });
            }
            const fwd = req.headers.get("x-forwarded-for");
            const clientIP = (fwd ? fwd.split(",")[0].trim() : server.requestIP(req)?.address) || "";

            const stream = new ReadableStream({
                async start(controller) {
                    const send = (obj) => controller.enqueue(enc.encode(`data: ${JSON.stringify(obj)}\n\n`));
                    try {
                        await runPipeline(String(query || "").trim(), clientIP, send);
                    } catch (err) {
                        send({ kind: "error", text: String(err?.message || err) });
                    } finally {
                        controller.close();
                    }
                },
            });

            return new Response(stream, {
                headers: {
                    "Content-Type": "text/event-stream; charset=utf-8",
                    "Cache-Control": "no-cache, no-transform",
                    Connection: "keep-alive",
                    "X-Accel-Buffering": "no", // disable proxy buffering (e.g. nginx)
                },
            });
        }

        return new Response("Not found", { status: 404 });
    },
});

console.log(
    `✈  Travel Assistant on http://localhost:${PORT}  (LLM: ${PROVIDER} / ${LLM?.model}, debug: ${DEBUG ? "on" : "off"})`
);