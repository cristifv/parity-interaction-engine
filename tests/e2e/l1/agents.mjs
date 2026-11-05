// IE step 6 (DEMO level 1): two browser agents on the gateway's own agent page, in headless
// Chromium (Playwright's image, sandbox on, as pwuser), driven by run.sh over a small HTTP API on
// loopback. The page's own rules decide everything a softphone does (it auto-answers only an
// interaction RE reserved for its session); the driver only clicks what an agent would click.
//   node agents.mjs <out dir> <page url> <control port>
// Inputs: agent-<agent>.wav (each fake microphone, read-only mount /in); each agent's dev JWT
// comes in /<a>/login's body (run.sh reads it from its 600 file), is kept in memory for reloads,
// and goes nowhere but the page's login box: never logged or written.
// API (POST unless noted; JSON answers):
//   /<a>/login            body {"token"}: open the page, log in, wait for the snapshot and SIP
//                         registration
//   /<a>/ready, /<a>/not_ready
//   GET /<a>/offer?wait_ms=N   the offer shown (or 404 after N ms)
//   /<a>/accept           click Accept on the shown offer
//   /<a>/reload           reload the page and log in again (a reconnect)
//   GET /<a>/answered?wait_ms=N  the call the page answered (or 404)
//   /<a>/capture          start measuring what the agent hears (Goertzel per 1 s window)
//   GET /<a>/heard        the windows so far
//   /<a>/dtmf             body {"codes": "..."}: send the agent's action code on the call
//   GET /<a>/wrap?wait_ms=N   wait for the wrap-up panel; /<a>/wrap_done clicks Done
//   GET /<a>/log          the page's diagnostics log (never credentials, SDP or digits)
//   /<a>/drop_invites     body {"on": true|false}: while on, an INVITE on the agent's SIP
//                         WebSocket never reaches the page (its softphone doesn't ring: the agent
//                         leg goes unanswered); everything else passes (Playwright routeWebSocket)
//   /quit
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { chromium } from "playwright";

const [out, url, port] = process.argv.slice(2);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const driverLog = fs.createWriteStream(path.join(out, "driver.jsonl"));
const note = (kind, data = {}) => driverLog.write(JSON.stringify({ t: Date.now(), kind, ...data }) + "\n");

const browsers = {};
const dropInvites = {};
async function agentPage(a) {
  if (browsers[a]) return browsers[a];
  const browser = await chromium.launch({
    chromiumSandbox: true,
    args: [
      "--allow-loopback-in-peer-connection",
      "--use-fake-ui-for-media-stream",
      "--use-fake-device-for-media-stream",
      `--use-file-for-fake-audio-capture=/in/agent-${a}.wav`,
      "--autoplay-policy=no-user-gesture-required",
      "--host-resolver-rules=MAP acme.localhost 127.0.0.1",
    ],
  });
  const context = await browser.newContext({ permissions: ["microphone"] });
  const page = await context.newPage();
  // The SIP WebSocket (any WebSocket but the gateway's) through the driver: passed both ways,
  // except an INVITE to the page while /<a>/drop_invites is on.
  await page.routeWebSocket((u) => !u.pathname.startsWith("/agents/"), (ws) => {
    const server = ws.connectToServer();
    ws.onMessage((m) => server.send(m));
    server.onMessage((m) => {
      if (dropInvites[a] && typeof m === "string" && m.startsWith("INVITE ")) {
        note("invite_dropped", { agent: a });
        return;
      }
      ws.send(m);
    });
    ws.onClose((code, reason) => server.close({ code, reason }));
    server.onClose((code, reason) => ws.close({ code, reason }));
  });
  page.on("pageerror", (e) => note("page_error", { agent: a, error: String(e).slice(0, 300) }));
  page.on("console", (m) => {
    if (m.type() === "error") note("console_error", { agent: a, text: m.text().slice(0, 300) });
  });
  browsers[a] = { browser, page, logFrom: 0 };
  return browsers[a];
}

async function flushLog(a) {
  const b = browsers[a];
  if (!b) return [];
  const fresh = await b.page.evaluate((from) => (window.parityAgent ? window.parityAgent.log.slice(from) : []), b.logFrom).catch(() => []);
  b.logFrom += fresh.length;
  if (fresh.length) fs.appendFileSync(path.join(out, `page-${a}.jsonl`), fresh.map((e) => JSON.stringify(e)).join("\n") + "\n");
  return fresh;
}

const tokens = {};
async function login(a) {
  const b = await agentPage(a);
  const token = tokens[a];
  if (!token) throw new Error("no token for this agent");
  await flushLog(a);
  b.logFrom = 0;
  await b.page.goto(url);
  await b.page.fill("#token", token);
  await b.page.click("#connect");
  await b.page.waitForFunction(() => window.parityAgent?.log.some((e) => e.kind === "frame" && e.type === "snapshot"), null, { timeout: 15000 });
  await b.page.waitForFunction(() => window.parityAgent.log.some((e) => e.kind === "sip_registration" && e.state === "Registered"), null, { timeout: 20000 });
  const snap = await b.page.evaluate(() => window.parityAgent.log.find((e) => e.kind === "frame" && e.type === "snapshot"));
  const offerShown = await b.page.isVisible("#offer");
  return { snapshot: snap, offer_shown: offerShown };
}

async function waitFor(fn, waitMs) {
  const end = Date.now() + waitMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > end) return null;
    await sleep(100);
  }
}

function startCapture() {
  const stream = document.getElementById("remote").srcObject;
  const ctx = new AudioContext({ sampleRate: 48000 });
  const src = ctx.createMediaStreamSource(stream);
  const proc = ctx.createScriptProcessor(4096, 1, 1);
  const freqs = [440, 1100, 1900, 2100];
  const coef = freqs.map((f) => 2 * Math.cos((2 * Math.PI * f) / 48000));
  const win = 48000;
  const fresh = () => ({ n: 0, sq: 0, s1: freqs.map(() => 0), s2: freqs.map(() => 0) });
  const st = { started: Date.now(), windows: [], cur: fresh() };
  proc.onaudioprocess = (e) => {
    const x = e.inputBuffer.getChannelData(0);
    for (let i = 0; i < x.length; i++) {
      const w = st.cur;
      const v = x[i];
      w.sq += v * v;
      for (let k = 0; k < freqs.length; k++) {
        const s0 = v + coef[k] * w.s1[k] - w.s2[k];
        w.s2[k] = w.s1[k];
        w.s1[k] = s0;
      }
      if (++w.n === win) {
        const power = freqs.map((_, k) => w.s1[k] * w.s1[k] + w.s2[k] * w.s2[k] - coef[k] * w.s1[k] * w.s2[k]);
        const others = (power[1] + power[2] + power[3]) / 3;
        st.windows.push({ end_ms: Date.now(), rms: Math.round(Math.sqrt(w.sq / win) * 1e4) / 1e4, ratio_440: Math.round((power[0] / Math.max(1e-12, others)) * 10) / 10 });
        st.cur = fresh();
      }
    }
  };
  src.connect(proc);
  proc.connect(ctx.destination);
  window.__heard = st;
}

const routes = {
  login: async (a, _q, body) => {
    tokens[a] = String(JSON.parse(body || "{}").token || "").trim();
    return login(a);
  },
  ready: async (a) => { await browsers[a].page.click('[data-state="ready"]'); return { ok: true }; },
  not_ready: async (a) => { await browsers[a].page.click('[data-state="not_ready"]'); return { ok: true }; },
  offer: async (a, q) => {
    const p = browsers[a].page;
    const id = await waitFor(async () => ((await p.isVisible("#offer")) ? await p.getAttribute("#offer", "data-offer-id") : null), Number(q.get("wait_ms") ?? 0));
    return id ? { offer_id: id } : null;
  },
  accept: async (a) => { await browsers[a].page.click("#accept"); return { ok: true }; },
  drop_invites: async (a, _q, body) => {
    dropInvites[a] = Boolean(JSON.parse(body || "{}").on);
    note("drop_invites", { agent: a, on: dropInvites[a] });
    return { ok: true };
  },
  reload: async (a) => {
    await flushLog(a);
    return login(a);
  },
  answered: async (a, q) => {
    const p = browsers[a].page;
    const e = await waitFor(() => p.evaluate(() => window.parityAgent.log.find((x) => x.kind === "answered") || null), Number(q.get("wait_ms") ?? 0));
    return e;
  },
  capture: async (a) => {
    const p = browsers[a].page;
    await p.waitForFunction(() => document.getElementById("remote").srcObject?.getAudioTracks().length > 0, null, { timeout: 5000 });
    await p.evaluate(startCapture);
    return { ok: true };
  },
  heard: async (a) => browsers[a].page.evaluate(() => (window.__heard ? { started: window.__heard.started, windows: window.__heard.windows } : { windows: [] })),
  dtmf: async (a, _q, body) => {
    const codes = String(JSON.parse(body || "{}").codes || "");
    const ok = await browsers[a].page.evaluate((c) => window.parityAgent.sendDtmf(c), codes);
    note("dtmf", { agent: a, tones: codes.length, ok }); // never the codes
    return { ok };
  },
  wrap: async (a, q) => {
    const p = browsers[a].page;
    return (await waitFor(() => p.isVisible("#wrap"), Number(q.get("wait_ms") ?? 0))) ? { ok: true } : null;
  },
  wrap_done: async (a) => {
    const p = browsers[a].page;
    await p.fill("#disposition", "sale");
    await p.click("#done");
    return { ok: true };
  },
  log: async (a) => {
    await flushLog(a);
    return fs.existsSync(path.join(out, `page-${a}.jsonl`)) ? fs.readFileSync(path.join(out, `page-${a}.jsonl`), "utf8").trim().split("\n").filter(Boolean).map((l) => JSON.parse(l)) : [];
  },
};

let quit;
const done = new Promise((r) => (quit = r));
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://127.0.0.1");
  let body = "";
  for await (const c of req) body += c;
  const [, a, what] = u.pathname.split("/");
  try {
    if (a === "quit") {
      res.end("{}");
      quit();
      return;
    }
    const fn = routes[what];
    if (!fn || !/^a[0-9]$/.test(a)) throw new Error("no such route");
    note("cmd", { agent: a, what });
    const r = await fn(a, u.searchParams, body);
    await flushLog(a).catch(() => {});
    if (r === null) {
      res.statusCode = 404;
      res.end("{}");
    } else {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(r));
    }
  } catch (e) {
    note("cmd_failed", { agent: a, what, error: String(e).slice(0, 300) });
    res.statusCode = 500;
    res.end(JSON.stringify({ error: String(e).slice(0, 300) }));
  }
});
server.listen(Number(port), "127.0.0.1", () => note("listening", { port: Number(port) }));
const keep = setInterval(() => Object.keys(browsers).forEach((a) => flushLog(a).catch(() => {})), 1000);
await done;
clearInterval(keep);
for (const a of Object.keys(browsers)) {
  await flushLog(a).catch(() => {});
  await browsers[a].browser.close();
}
server.close();
driverLog.end();
process.exit(0);
