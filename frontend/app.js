"use strict";

const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const KiB = 1024;
const MiB = 1024 * KiB;

/* ------------------------------------------------------------------ *
 * Configuration
 * ------------------------------------------------------------------ */
const CONFIG = {
  latency: {
    warmup: 2,          // probes discarded (connection setup, JIT, caches)
    samples: 20,        // probes that count
    intervalMs: 50,
    timeoutMs: 1500
  },
  download: {
    streams: 4,                 // parallel connections
    requestBytes: 16 * MiB,     // each worker re-requests until the phase ends
    rampMs: 1200,               // TCP slow-start window, excluded from the result
    minMs: 4500,
    maxMs: 10000,
    maxBytes: 500 * MiB,        // data cap per phase (protects metered links)
    stableWindowMs: 1500,
    stableTol: 0.05
  },
  upload: {
    streams: 3,
    requestBytes: 2 * MiB,     // small bodies: Chrome only reports upload progress in coarse steps
    rampMs: 1200,
    minMs: 4500,
    maxMs: 10000,
    maxBytes: 300 * MiB,
    stableWindowMs: 1500,
    stableTol: 0.05
  },
  loadedPingIntervalMs: 150,
  historyKey: "cloudspeed-history-v1",
  historyMax: 10
};

/* ------------------------------------------------------------------ *
 * Statistics helpers (the same metrics NetPulse reports per target)
 * ------------------------------------------------------------------ */
function percentile(sorted, p) {
  if (!sorted.length) return NaN;
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

function mean(values) {
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function stddev(values) {
  if (values.length < 2) return 0;
  const m = mean(values);
  return Math.sqrt(
    values.reduce((s, v) => s + (v - m) ** 2, 0) / (values.length - 1)
  );
}

/**
 * samples: round-trip times in ms, in the order they were taken.
 * sent: number of probes that were sent (replies that never arrived = loss).
 * Jitter is the mean absolute difference between consecutive samples
 * (the same idea as RFC 3550 interarrival jitter).
 */
function latencyStats(samples, sent) {
  if (!samples.length) return null;

  const sorted = [...samples].sort((a, b) => a - b);
  const m = mean(samples);

  let jitter = 0;
  if (samples.length > 1) {
    let sum = 0;
    for (let i = 1; i < samples.length; i++) {
      sum += Math.abs(samples[i] - samples[i - 1]);
    }
    jitter = sum / (samples.length - 1);
  }

  return {
    count: samples.length,
    sent,
    lossPercent: sent > 0 ? ((sent - samples.length) / sent) * 100 : 0,
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean: m,
    median: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    stddev: stddev(samples),
    jitter
  };
}

/* ------------------------------------------------------------------ *
 * Formatting / UI helpers
 * ------------------------------------------------------------------ */
function formatMB(bytes) {
  return (bytes / 1_000_000).toFixed(2) + " MB";
}

function fmtMbps(v) {
  if (!Number.isFinite(v)) return "--";
  if (v >= 100) return v.toFixed(0);
  if (v >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

function fmtMs(v, digits = 1) {
  return Number.isFinite(v) ? v.toFixed(digits) : "--";
}

function setText(id, text) {
  const el = $(id);
  if (el) el.textContent = text;
}

function updateProgress(value, label) {
  const v = Math.max(0, Math.min(100, value));
  $("progressBar").style.width = v + "%";
  setText("percent", Math.round(v) + "%");
  if (label) setText("phase", label);
}

let downloadedBytes = 0;
let uploadedBytes = 0;

function updateData() {
  setText("downloadData", formatMB(downloadedBytes));
  setText("uploadData", formatMB(uploadedBytes));
}

/* ------------------------------------------------------------------ *
 * Latency: WebSocket echo (falls back to HTTP)
 * ------------------------------------------------------------------ */
class Pinger {
  constructor() {
    this.mode = "websocket";
    this.ws = null;
    this.pending = new Map();
    this.seq = 0;
  }

  async open() {
    try {
      const proto = location.protocol === "https:" ? "wss://" : "ws://";
      const ws = new WebSocket(proto + location.host + "/ws/ping");

      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("ws timeout")), 2500);
        ws.onopen = () => { clearTimeout(timer); resolve(); };
        ws.onerror = () => { clearTimeout(timer); reject(new Error("ws error")); };
      });

      ws.onmessage = (e) => {
        const id = Number(e.data);
        const p = this.pending.get(id);
        if (!p) return;
        clearTimeout(p.timer);
        this.pending.delete(id);
        p.resolve(performance.now() - p.start);
      };
      ws.onclose = () => {
        for (const [, p] of this.pending) {
          clearTimeout(p.timer);
          p.resolve(null);
        }
        this.pending.clear();
      };

      this.ws = ws;
      this.mode = "websocket";
    } catch (err) {
      console.warn("WebSocket unavailable, using HTTP latency probes:", err);
      this.mode = "http";
    }
  }

  /** Resolves with the RTT in ms, or null on timeout/failure. */
  ping(timeoutMs = CONFIG.latency.timeoutMs) {
    if (this.mode === "websocket") {
      return new Promise((resolve) => {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return resolve(null);

        const id = ++this.seq;
        const start = performance.now();
        const timer = setTimeout(() => {
          this.pending.delete(id);
          resolve(null);
        }, timeoutMs);

        this.pending.set(id, { start, resolve, timer });

        try {
          this.ws.send(String(id));
        } catch {
          clearTimeout(timer);
          this.pending.delete(id);
          resolve(null);
        }
      });
    }

    return this.httpPing(timeoutMs);
  }

  async httpPing(timeoutMs) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    const start = performance.now();
    try {
      const res = await fetch("/api/ping?t=" + Math.random(), {
        cache: "no-store",
        signal: ctrl.signal
      });
      if (!res.ok) return null;
      await res.arrayBuffer();
      return performance.now() - start;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  close() {
    if (this.ws) {
      this.ws.onclose = null;
      try { this.ws.close(); } catch { /* ignore */ }
      this.ws = null;
    }
    for (const [, p] of this.pending) clearTimeout(p.timer);
    this.pending.clear();
  }
}

async function measureIdleLatency(pinger) {
  const L = CONFIG.latency;
  const samples = [];
  let sent = 0;

  setText("status", "Measuring latency...");

  for (let i = 0; i < L.warmup + L.samples; i++) {
    const rtt = await pinger.ping(L.timeoutMs);

    if (i >= L.warmup) {
      sent++;
      if (rtt !== null) samples.push(rtt);

      if (samples.length) {
        const live = latencyStats(samples, sent);
        setText("ping", fmtMs(live.median));
        setText("pingSub", "jitter " + fmtMs(live.jitter) + " ms");
      }
    }

    updateProgress(((i + 1) / (L.warmup + L.samples)) * 15, "Measuring ping");
    await sleep(L.intervalMs);
  }

  const stats = latencyStats(samples, sent);
  if (!stats) throw new Error("Server did not answer latency probes");
  return stats;
}

/** Keeps pinging while a download/upload is running (loaded latency). */
function startLoadedProbe(pinger) {
  let running = true;
  const samples = [];
  let sent = 0;

  const loop = (async () => {
    while (running) {
      const t0 = performance.now();
      sent++;
      const rtt = await pinger.ping(1000);
      if (rtt !== null) samples.push(rtt);

      const wait = CONFIG.loadedPingIntervalMs - (performance.now() - t0);
      if (wait > 0 && running) await sleep(wait);
    }
  })();

  return {
    async stop() {
      running = false;
      await loop;
      return latencyStats(samples, sent);
    }
  };
}

/* ------------------------------------------------------------------ *
 * Throughput measurement
 * ------------------------------------------------------------------ */

/** Counts bytes and lets us ask "what was the rate between t1 and t2?". */
class Meter {
  constructor() {
    this.t0 = performance.now();
    this.bytes = 0;
    this.points = [{ t: 0, b: 0 }];
    this.acked = [];      // completed (server-acknowledged) uploads: { t, b }
    this.ackedBytes = 0;
  }

  ack(n) {
    this.ackedBytes += n;
    this.acked.push({ t: this.now(), b: this.ackedBytes });
  }

  now() {
    return performance.now() - this.t0;
  }

  add(n) {
    this.bytes += n;
  }

  sample() {
    this.points.push({ t: this.now(), b: this.bytes });
  }

  bytesAt(t) {
    const pts = this.points;
    if (t <= 0) return 0;
    const last = pts[pts.length - 1];
    if (t >= last.t) return last.b;

    for (let i = pts.length - 1; i > 0; i--) {
      if (pts[i - 1].t <= t) {
        const a = pts[i - 1];
        const b = pts[i];
        const f = b.t === a.t ? 0 : (t - a.t) / (b.t - a.t);
        return a.b + (b.b - a.b) * f;
      }
    }
    return 0;
  }

  /** Mbps between two times (ms since start). */
  rate(from, to) {
    if (to <= from) return 0;
    return ((this.bytesAt(to) - this.bytesAt(from)) * 8) / ((to - from) * 1000);
  }
}

function windowSpeeds(meter, from, to, step) {
  const out = [];
  for (let t = from; t + step <= to; t += step) {
    out.push(meter.rate(t, t + step));
  }
  return out;
}

/** True when the last window's speed is within tol of the one before it. */
function isStable(meter, cfg, t) {
  const w = cfg.stableWindowMs;
  if (t - cfg.rampMs < 2 * w) return false;

  const a = meter.rate(t - w, t);
  const b = meter.rate(t - 2 * w, t - w);
  const hi = Math.max(a, b);

  return hi > 0 && Math.abs(a - b) / hi < cfg.stableTol;
}

async function downloadWorker(cfg, meter, signal, errors) {
  try {
    while (!signal.aborted) {
      const res = await fetch(
        "/api/download?size=" + cfg.requestBytes + "&t=" + Math.random(),
        { cache: "no-store", signal }
      );
      if (!res.ok || !res.body) throw new Error("Download request failed (" + res.status + ")");

      const reader = res.body.getReader();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        meter.add(value.byteLength);
      }
    }
  } catch (err) {
    if (!signal.aborted) errors.push(err.message || String(err));
  }
}

function makePayload(size) {
  // crypto.getRandomValues accepts at most 65536 bytes per call.
  const data = new Uint8Array(size);
  for (let off = 0; off < size; off += 65536) {
    crypto.getRandomValues(data.subarray(off, Math.min(off + 65536, size)));
  }
  return new Blob([data], { type: "application/octet-stream" });
}

/**
 * Uploads use XMLHttpRequest because fetch() cannot report upload progress.
 * upload.onprogress tells us how many bytes have been handed to the network
 * stack; the ramp-up window is discarded so the initial socket-buffer burst
 * does not inflate the result.
 */
function uploadWorker(cfg, meter, signal, payload, errors) {
  return new Promise((resolve) => {
    const next = () => {
      if (signal.aborted) return resolve();

      const xhr = new XMLHttpRequest();
      let counted = 0;

      xhr.open("POST", "/api/upload?t=" + Math.random());
      xhr.setRequestHeader("Content-Type", "application/octet-stream");

      xhr.upload.onprogress = (e) => {
        meter.add(e.loaded - counted);
        counted = e.loaded;
      };
      xhr.upload.onload = () => {
        meter.add(payload.size - counted);
        counted = payload.size;
      };
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          meter.ack(payload.size);
          next();
        } else {
          errors.push("Upload request failed (" + xhr.status + ")");
          resolve();
        }
      };
      xhr.onerror = () => {
        if (!signal.aborted) errors.push("Upload connection error");
        resolve();
      };
      xhr.onabort = () => resolve();

      signal.addEventListener("abort", () => xhr.abort(), { once: true });
      xhr.send(payload);
    };

    next();
  });
}

/* Live chart ------------------------------------------------------- */
const chart = { points: [], offset: 0 };

function niceMax(v) {
  if (!(v > 0)) return 10;
  const target = v * 1.15;
  const pow = Math.pow(10, Math.floor(Math.log10(target)));
  for (const m of [1, 2, 2.5, 5, 10]) {
    if (m * pow >= target) return m * pow;
  }
  return 10 * pow;
}

function drawChart() {
  const canvas = $("chart");
  if (!canvas) return;

  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  if (!w || !h) return;

  if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
  }

  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const css = getComputedStyle(document.documentElement);
  const colors = {
    grid: css.getPropertyValue("--border").trim() || "#26324a",
    muted: css.getPropertyValue("--muted").trim() || "#94a3b8",
    download: css.getPropertyValue("--accent").trim() || "#38bdf8",
    upload: css.getPropertyValue("--green").trim() || "#34d399"
  };

  const pad = { l: 46, r: 12, t: 12, b: 22 };
  const pw = w - pad.l - pad.r;
  const ph = h - pad.t - pad.b;

  const pts = chart.points;
  const maxV = niceMax(pts.reduce((m, p) => Math.max(m, p.v), 0));
  const maxX = Math.max(pts.length ? pts[pts.length - 1].x : 0, 1);

  ctx.font = "11px Inter, 'Segoe UI', Arial, sans-serif";
  ctx.fillStyle = colors.muted;
  ctx.strokeStyle = colors.grid;
  ctx.lineWidth = 1;

  for (let i = 0; i <= 3; i++) {
    const y = pad.t + ph - (ph * i) / 3;
    ctx.beginPath();
    ctx.moveTo(pad.l, y);
    ctx.lineTo(w - pad.r, y);
    ctx.stroke();
    ctx.textAlign = "right";
    ctx.fillText(String(Math.round((maxV * i) / 3)), pad.l - 6, y + 4);
  }

  ctx.textAlign = "left";
  ctx.fillText("0 s", pad.l, h - 6);
  ctx.textAlign = "right";
  ctx.fillText(maxX.toFixed(0) + " s", w - pad.r, h - 6);

  if (!pts.length) {
    ctx.textAlign = "center";
    ctx.fillText("Throughput will appear here during the test", pad.l + pw / 2, pad.t + ph / 2);
    return;
  }

  const X = (x) => pad.l + (x / maxX) * pw;
  const Y = (v) => pad.t + ph - (v / maxV) * ph;

  for (const kind of ["download", "upload"]) {
    const series = pts.filter((p) => p.kind === kind);
    if (series.length < 2) continue;

    ctx.beginPath();
    series.forEach((p, i) => (i ? ctx.lineTo(X(p.x), Y(p.v)) : ctx.moveTo(X(p.x), Y(p.v))));
    ctx.strokeStyle = colors[kind];
    ctx.lineWidth = 2;
    ctx.lineJoin = "round";
    ctx.stroke();

    ctx.lineTo(X(series[series.length - 1].x), Y(0));
    ctx.lineTo(X(series[0].x), Y(0));
    ctx.closePath();
    ctx.globalAlpha = 0.12;
    ctx.fillStyle = colors[kind];
    ctx.fill();
    ctx.globalAlpha = 1;
  }
}

/* One throughput phase --------------------------------------------- */
async function runThroughput(kind, pinger, progressBase, progressSpan) {
  const cfg = CONFIG[kind];
  const isDownload = kind === "download";

  setText("status", isDownload ? "Testing download speed..." : "Testing upload speed...");
  setText("phase", isDownload ? "Measuring download" : "Measuring upload");

  const payload = isDownload ? null : makePayload(cfg.requestBytes);
  const errors = [];
  const ctrl = new AbortController();
  const probe = startLoadedProbe(pinger);
  const meter = new Meter();

  const workers = [];
  for (let i = 0; i < cfg.streams; i++) {
    workers.push(
      isDownload
        ? downloadWorker(cfg, meter, ctrl.signal, errors)
        : uploadWorker(cfg, meter, ctrl.signal, payload, errors)
    );
  }

  let lastBytes = 0;
  let stable = false;

  await new Promise((resolve) => {
    const timer = setInterval(() => {
      meter.sample();
      const t = meter.now();

      // global "data used" counters
      const delta = meter.bytes - lastBytes;
      lastBytes = meter.bytes;
      if (isDownload) downloadedBytes += delta;
      else uploadedBytes += delta;
      updateData();

      // live number: trailing 1 s once we have it, cumulative before that
      const live = t >= 1000 ? meter.rate(t - 1000, t) : meter.rate(0, t);
      setText(kind, fmtMbps(live));

      chart.points.push({ x: chart.offset + t / 1000, v: live, kind });
      drawChart();

      updateProgress(
        progressBase + Math.min(t / cfg.minMs, 1) * progressSpan * 0.95,
        isDownload ? "Measuring download" : "Measuring upload"
      );

      const failed = errors.length >= cfg.streams;
      const timeUp = t >= cfg.maxMs;
      const capped = meter.bytes >= cfg.maxBytes;

      if (!failed && !timeUp && !capped && t >= cfg.minMs && isStable(meter, cfg, t)) {
        stable = true;
      }

      if (failed || timeUp || capped || stable) {
        clearInterval(timer);
        resolve();
      }
    }, 100);
  });

  meter.sample();
  const duration = meter.now();

  const stopProbe = probe.stop();   // stops scheduling new pings right away
  ctrl.abort();                     // tears down the streams
  const loaded = await stopProbe;
  await Promise.allSettled(workers);

  if (meter.bytes === 0) {
    throw new Error(errors[0] || "No data was transferred");
  }

  // catch any bytes counted between the last tick and the abort
  const tail = meter.bytes - lastBytes;
  if (isDownload) downloadedBytes += tail;
  else uploadedBytes += tail;
  updateData();

  // The first rampMs are TCP slow start: reported speed excludes them.
  const ramp = Math.min(cfg.rampMs, duration * 0.3);
  let steady = meter.rate(ramp, duration);
  let method = "streamed bytes";

  // Upload progress events count bytes sitting in socket/kernel buffers, which
  // inflates the rate. Bytes of requests the server has fully received cannot
  // be inflated, so use those (first to last completion after ramp-up).
  if (!isDownload) {
    const acks = meter.acked.filter((p) => p.t >= ramp);
    if (acks.length >= 3) {
      const first = acks[0];
      const last = acks[acks.length - 1];
      const v = ((last.b - first.b) * 8) / ((last.t - first.t) * 1000);
      if (v > 0 && Number.isFinite(v)) {
        steady = v;
        method = "server-acknowledged requests";
      }
    }
  }

  const slices = windowSpeeds(meter, ramp, duration, 500);
  const sliceMean = slices.length ? mean(slices) : steady;
  const cov = slices.length > 1 && sliceMean > 0 ? stddev(slices) / sliceMean : null;

  let peak = steady;
  for (let t = ramp + 1000; t <= duration; t += 250) {
    peak = Math.max(peak, meter.rate(t - 1000, t));
  }

  chart.offset += duration / 1000;

  setText(kind, fmtMbps(steady));

  return {
    mbps: steady,
    peakMbps: peak,
    averageMbps: meter.rate(0, duration),
    coefficientOfVariation: cov,
    stable,
    bytes: meter.bytes,
    durationMs: duration,
    rampMs: ramp,
    method,
    streams: cfg.streams,
    loadedLatency: loaded
  };
}

/* ------------------------------------------------------------------ *
 * Analysis (bufferbloat and quality)
 * ------------------------------------------------------------------ */
function gradeFor(addedMs) {
  if (addedMs < 5) return "A+";
  if (addedMs < 30) return "A";
  if (addedMs < 60) return "B";
  if (addedMs < 200) return "C";
  if (addedMs < 400) return "D";
  return "F";
}

function computeBufferbloat(idle, down, up) {
  const addedDown = down.loadedLatency
    ? Math.max(0, down.loadedLatency.median - idle.median)
    : null;
  const addedUp = up.loadedLatency
    ? Math.max(0, up.loadedLatency.median - idle.median)
    : null;

  const known = [addedDown, addedUp].filter((v) => v !== null);
  const worst = known.length ? Math.max(...known) : null;

  return {
    addedDownloadMs: addedDown,
    addedUploadMs: addedUp,
    worstAddedMs: worst,
    grade: worst === null ? "N/A" : gradeFor(worst)
  };
}

function assessQuality(r) {
  const ping = r.latency.idle.median;
  const jitter = r.latency.idle.jitter;
  const loss = r.latency.idle.lossPercent;
  const down = r.download.mbps;
  const up = r.upload.mbps;
  const added = r.bufferbloat.worstAddedMs ?? 0;

  const level = (good, fair) => (good ? "good" : fair ? "fair" : "poor");

  return [
    {
      label: "Online gaming",
      level: level(
        ping < 40 && jitter < 10 && loss < 1 && added < 60,
        ping < 80 && jitter < 25 && loss < 3 && added < 200
      ),
      note: fmtMs(ping, 0) + " ms ping, " + fmtMs(jitter) + " ms jitter, +" + fmtMs(added, 0) + " ms under load"
    },
    {
      label: "Video calls",
      level: level(
        down >= 5 && up >= 3 && jitter < 20 && loss < 1 && added < 100,
        down >= 2 && up >= 1.5 && jitter < 40 && loss < 3
      ),
      note: fmtMbps(up) + " Mbps up, " + fmtMs(loss) + "% loss"
    },
    {
      label: "4K streaming",
      level: level(down >= 25, down >= 10),
      note: fmtMbps(down) + " Mbps down (about 25 needed)"
    }
  ];
}

function renderQuality(items) {
  const box = $("quality");
  box.replaceChildren();

  for (const item of items) {
    const chip = document.createElement("div");
    chip.className = "chip";
    chip.dataset.level = item.level;

    const title = document.createElement("strong");
    title.textContent =
      item.label + ": " + { good: "Great", fair: "OK", poor: "Poor" }[item.level];

    const note = document.createElement("span");
    note.textContent = item.note;

    chip.append(title, note);
    box.appendChild(chip);
  }
}

/* ------------------------------------------------------------------ *
 * History and export
 * ------------------------------------------------------------------ */
let lastResult = null;

function loadHistory() {
  try {
    const raw = localStorage.getItem(CONFIG.historyKey);
    const list = raw ? JSON.parse(raw) : [];
    return Array.isArray(list) ? list : [];
  } catch {
    return [];
  }
}

function saveHistory(entry) {
  const list = [entry, ...loadHistory()].slice(0, CONFIG.historyMax);
  try {
    localStorage.setItem(CONFIG.historyKey, JSON.stringify(list));
  } catch { /* storage unavailable: history simply isn't kept */ }
  renderHistory();
}

function renderHistory() {
  const body = $("historyBody");
  const list = loadHistory();
  body.replaceChildren();

  $("historySection").hidden = list.length === 0;

  for (const e of list) {
    const tr = document.createElement("tr");
    const cells = [
      new Date(e.timestamp).toLocaleString(),
      fmtMs(e.ping) + " ms",
      fmtMbps(e.download) + " Mbps",
      fmtMbps(e.upload) + " Mbps",
      e.grade
    ];
    for (const c of cells) {
      const td = document.createElement("td");
      td.textContent = c;
      tr.appendChild(td);
    }
    body.appendChild(tr);
  }
}

function exportJson() {
  if (!lastResult) return;
  const blob = new Blob([JSON.stringify(lastResult, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "speedtest-" + lastResult.timestamp.replace(/[:.]/g, "-") + ".json";
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/* ------------------------------------------------------------------ *
 * Environment info
 * ------------------------------------------------------------------ */
async function loadServerInfo() {
  try {
    const res = await fetch("/api/info", { cache: "no-store" });
    if (res.ok) return await res.json();
  } catch { /* optional */ }
  return null;
}

function browserHints() {
  const c = navigator.connection;
  return c
    ? {
        effectiveType: c.effectiveType ?? null,
        downlinkMbps: c.downlink ?? null,
        rttMs: c.rtt ?? null
      }
    : null;
}

function pageProtocol() {
  const nav = performance.getEntriesByType("navigation")[0];
  return (nav && nav.nextHopProtocol) || "unknown";
}

/* ------------------------------------------------------------------ *
 * Test orchestration
 * ------------------------------------------------------------------ */
function resetUi() {
  downloadedBytes = 0;
  uploadedBytes = 0;
  chart.points = [];
  chart.offset = 0;
  lastResult = null;

  for (const id of ["ping", "download", "upload", "jitter", "loss", "loadedDown", "loadedUp", "bloat"]) {
    setText(id, "--");
  }
  for (const id of ["pingSub", "downloadSub", "uploadSub", "bloatNote"]) setText(id, "");

  $("bloat").dataset.grade = "";
  $("quality").replaceChildren();
  $("exportBtn").hidden = true;

  for (const id of ["infoIp", "infoMethod", "infoProtocol", "infoStreams", "infoHint"]) {
    setText(id, "--");
  }

  setText("duration", "0.00 s");
  updateData();
  updateProgress(0, "Starting test");
  drawChart();
}

async function startTest() {
  const button = $("startBtn");
  button.disabled = true;
  button.textContent = "TESTING...";

  resetUi();

  const testStart = performance.now();
  const durationTimer = setInterval(() => {
    setText("duration", ((performance.now() - testStart) / 1000).toFixed(2) + " s");
  }, 100);

  const pinger = new Pinger();

  try {
    const info = await loadServerInfo();
    await pinger.open();

    setText("infoIp", info ? info.client_ip : "--");
    setText("infoMethod", pinger.mode === "websocket" ? "WebSocket echo" : "HTTP request (fallback)");
    setText("infoProtocol", pageProtocol());

    const hints = browserHints();
    setText(
      "infoHint",
      hints ? [hints.effectiveType, hints.downlinkMbps != null ? hints.downlinkMbps + " Mbps est." : null]
        .filter(Boolean).join(", ") || "--" : "not supported"
    );

    // 1. Idle latency
    const idle = await measureIdleLatency(pinger);
    setText("ping", fmtMs(idle.median));
    setText("pingSub", "jitter " + fmtMs(idle.jitter) + " ms");
    setText("jitter", fmtMs(idle.jitter));
    setText("loss", fmtMs(idle.lossPercent) + " %");
    updateProgress(15, "Ping completed");

    // 2. Download (with loaded latency)
    const down = await runThroughput("download", pinger, 15, 40);
    setText("downloadSub", "peak " + fmtMbps(down.peakMbps) + " Mbps");
    setText("loadedDown", down.loadedLatency ? fmtMs(down.loadedLatency.median) + " ms" : "--");
    updateProgress(55, "Download completed");

    // 3. Upload (with loaded latency)
    const up = await runThroughput("upload", pinger, 55, 40);
    setText("uploadSub", "peak " + fmtMbps(up.peakMbps) + " Mbps");
    setText("loadedUp", up.loadedLatency ? fmtMs(up.loadedLatency.median) + " ms" : "--");
    updateProgress(95, "Analysing results");

    // 4. Analysis
    const bloat = computeBufferbloat(idle, down, up);
    setText("bloat", bloat.grade);
    $("bloat").dataset.grade = bloat.grade;
    setText(
      "bloatNote",
      bloat.worstAddedMs === null ? "no replies under load" : "+" + fmtMs(bloat.worstAddedMs, 0) + " ms under load"
    );
    setText("infoStreams", down.streams + " down / " + up.streams + " up");

    const result = {
      version: 1,
      timestamp: new Date().toISOString(),
      server: { clientIp: info ? info.client_ip : null, pageProtocol: pageProtocol() },
      latencyMethod: pinger.mode,
      latency: { idle, loadedDownload: down.loadedLatency, loadedUpload: up.loadedLatency },
      download: { ...down, loadedLatency: undefined },
      upload: { ...up, loadedLatency: undefined },
      bufferbloat: bloat,
      dataUsedBytes: { download: downloadedBytes, upload: uploadedBytes },
      totalDurationMs: performance.now() - testStart,
      browserConnectionHints: hints
    };

    lastResult = result;
    renderQuality(assessQuality(result));

    saveHistory({
      timestamp: result.timestamp,
      ping: idle.median,
      download: down.mbps,
      upload: up.mbps,
      grade: bloat.grade
    });

    $("exportBtn").hidden = false;
    updateProgress(100, "Test completed");
    setText("status", "Speed test completed successfully!");
  } catch (error) {
    console.error(error);
    setText("status", "Error: " + error.message);
    updateProgress(parseFloat($("percent").textContent) || 0, "Test interrupted");
  } finally {
    clearInterval(durationTimer);
    pinger.close();

    setText("duration", ((performance.now() - testStart) / 1000).toFixed(2) + " s");
    button.disabled = false;
    button.textContent = "TEST AGAIN";
  }
}

$("startBtn").addEventListener("click", startTest);
$("exportBtn").addEventListener("click", exportJson);
$("clearHistoryBtn").addEventListener("click", () => {
  try { localStorage.removeItem(CONFIG.historyKey); } catch { /* ignore */ }
  renderHistory();
});
window.addEventListener("resize", drawChart);

renderHistory();
drawChart();