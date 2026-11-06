
const $ = (id) => document.getElementById(id);

const MiB = 1024 * 1024;
const KiB = 1024;

const CONFIG = {
  pingSamples: 5,
  initialBytes: 256 * KiB,
  maxRequestBytes: 8 * MiB,
  maxPhaseBytes: 32 * MiB,
  maxPhaseMs: 8000,
  minSamples: 4,
  minPhaseMs: 1500,
  minPhaseBytes: 2 * MiB,
  stabilityThreshold: 0.10
};

let downloadedBytes = 0;
let uploadedBytes = 0;

function formatMB(bytes) {
  return (bytes / 1_000_000).toFixed(2) + " MB";
}

function toMbps(bytes, milliseconds) {
  return (bytes * 8) / (milliseconds * 1000);
}

function updateProgress(value, label) {
  $("progressBar").style.width = value + "%";
  $("percent").textContent = Math.round(value) + "%";
  $("phase").textContent = label;
}

function updateData() {
  $("downloadData").textContent = formatMB(downloadedBytes);
  $("uploadData").textContent = formatMB(uploadedBytes);
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);

  return sorted.length % 2
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

function stable(values) {
  if (values.length < CONFIG.minSamples) return false;

  const last = values.slice(-3);
  const mean = last.reduce((a, b) => a + b, 0) / last.length;

  if (mean <= 0) return false;

  const range = Math.max(...last) - Math.min(...last);
  return range / mean < CONFIG.stabilityThreshold;
}

async function measurePing() {
  $("status").textContent = "Measuring latency...";
  const samples = [];

  for (let i = 0; i < CONFIG.pingSamples; i++) {
    const start = performance.now();

    const response = await fetch(
      "/api/ping?t=" + Math.random(),
      { cache: "no-store" }
    );

    if (!response.ok) throw new Error("Ping request failed");

    await response.arrayBuffer();

    const elapsed = performance.now() - start;
    samples.push(elapsed);

    updateProgress(
      ((i + 1) / CONFIG.pingSamples) * 20,
      "Measuring ping"
    );
  }

  const result = median(samples);
  $("ping").textContent = result.toFixed(1);

  return result;
}

async function downloadSample(size) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);

  let received = 0;
  const start = performance.now();

  try {
    const response = await fetch(
      "/api/download?size=" + size + "&t=" + Math.random(),
      {
        cache: "no-store",
        signal: controller.signal
      }
    );

    if (!response.ok || !response.body) {
      throw new Error("Download request failed");
    }

    const reader = response.body.getReader();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      received += value.byteLength;
      downloadedBytes += value.byteLength;
      updateData();
    }

    const elapsed = performance.now() - start;

    if (received !== size) {
      throw new Error("Incomplete download");
    }

    return {
      bytes: received,
      elapsed,
      speed: toMbps(received, elapsed)
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function uploadSample(size) {
  const data = new Uint8Array(size);

  // Generate nontrivial payload for upload.
  // Modern browsers support crypto.getRandomValues.
  for (let offset = 0; offset < size; offset += 65536) {
    crypto.getRandomValues(
      data.subarray(offset, Math.min(offset + 65536, size))
    );
  }

  const start = performance.now();

  const response = await fetch("/api/upload", {
    method: "POST",
    headers: {
      "Content-Type": "application/octet-stream"
    },
    body: data,
    cache: "no-store",
    signal: AbortSignal.timeout(10000)
  });

  const elapsed = performance.now() - start;

  if (!response.ok) {
    throw new Error("Upload request failed");
  }

  const result = await response.json();

  if (result.received_bytes !== size) {
    throw new Error("Incomplete upload");
  }

  uploadedBytes += size;
  updateData();

  return {
    bytes: size,
    elapsed,
    speed: toMbps(size, elapsed)
  };
}


async function adaptiveTest(type) {
  const isDownload = type === "download";
  const output = $(type);

  const sampleFn = isDownload
    ? downloadSample
    : uploadSample;

  $("status").textContent = isDownload
    ? "Testing download speed..."
    : "Testing upload speed...";

  // Warm-up: excluded from final bandwidth estimate.
  await sampleFn(CONFIG.initialBytes);

  let size = CONFIG.initialBytes * 2;
  let phaseBytes = 0;
  let phaseElapsed = 0;
  let weightedBytes = 0;
  let weightedTime = 0;

  const speeds = [];
  const phaseStart = performance.now();

  while (
    phaseBytes < CONFIG.maxPhaseBytes &&
    performance.now() - phaseStart < CONFIG.maxPhaseMs
  ) {
    const remaining =
      CONFIG.maxPhaseBytes - phaseBytes;

    const requestSize = Math.min(
      size,
      remaining,
      CONFIG.maxRequestBytes
    );

    if (requestSize <= 0) break;

    const sample = await sampleFn(requestSize);

    phaseBytes += sample.bytes;
    phaseElapsed += sample.elapsed;

    weightedBytes += sample.bytes;
    weightedTime += sample.elapsed;
    speeds.push(sample.speed);

    const currentSpeed = toMbps(
      weightedBytes,
      weightedTime
    );

    output.textContent = currentSpeed.toFixed(2);

    const base = isDownload ? 20 : 60;

    const fraction = Math.max(
      phaseBytes / CONFIG.maxPhaseBytes,
      (performance.now() - phaseStart) /
        CONFIG.maxPhaseMs
    );

    updateProgress(
      Math.min(base + fraction * 40, base + 39),
      isDownload
        ? "Measuring download"
        : "Measuring upload"
    );

    const enoughSamples =
      speeds.length >= CONFIG.minSamples;

    const enoughTime =
      phaseElapsed >= CONFIG.minPhaseMs;

    const enoughData =
      phaseBytes >= CONFIG.minPhaseBytes;

    if (
      enoughSamples &&
      enoughTime &&
      enoughData &&
      stable(speeds)
    ) {
      console.log(
        type,
        "Stopped: measurements stabilized"
      );
      break;
    }

    // Increase payload when samples are too short.
    if (sample.elapsed < 500) {
      size = Math.min(
        size * 2,
        CONFIG.maxRequestBytes
      );
    }

    console.log(type, {
      requestBytes: requestSize,
      sampleMbps: sample.speed,
      averageMbps: currentSpeed,
      phaseBytes,
      phaseElapsed
    });
  }

  if (weightedTime <= 0) {
    throw new Error("No valid bandwidth samples");
  }

  const finalSpeed = toMbps(
    weightedBytes,
    weightedTime
  );

  console.log(type, "Final result:", {
    speedMbps: finalSpeed,
    measuredBytes: phaseBytes,
    measuredMs: phaseElapsed,
    samples: speeds.length,
    stable: stable(speeds)
  });

  return finalSpeed;
}


async function startTest() {
  const button = $("startBtn");
  button.disabled = true;

  downloadedBytes = 0;
  uploadedBytes = 0;

  $("ping").textContent = "--";
  $("download").textContent = "--";
  $("upload").textContent = "--";
  $("duration").textContent = "0.00 s";

  updateData();
  updateProgress(0, "Starting test");

  const testStart = performance.now();

  const durationTimer = setInterval(() => {
    $("duration").textContent =
      ((performance.now() - testStart) / 1000).toFixed(2) + " s";
  }, 100);

  try {
    await measurePing();

    updateProgress(20, "Ping completed");

    await adaptiveTest("download");

    updateProgress(60, "Download completed");

    await adaptiveTest("upload");

    updateProgress(100, "Test completed");
    $("status").textContent = "Speed test completed successfully!";
  } catch (error) {
    console.error(error);
    $("status").textContent = "Error: " + error.message;
    updateProgress(
      parseFloat($("percent").textContent) || 0,
      "Test interrupted"
    );
  } finally {
    clearInterval(durationTimer);

    $("duration").textContent =
      ((performance.now() - testStart) / 1000).toFixed(2) + " s";

    button.disabled = false;
    button.textContent = "TEST AGAIN";
  }
}

$("startBtn").addEventListener("click", startTest);
