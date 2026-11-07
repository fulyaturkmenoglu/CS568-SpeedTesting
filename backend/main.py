import asyncio
import os
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.requests import ClientDisconnect

app = FastAPI(title="Cloud Speed Test API")

# Maximum 64 MiB per request
MAX_BYTES = 64 * 1024 * 1024
CHUNK_SIZE = 64 * 1024

# A 4 MiB pool of random bytes is generated once at startup. Each download
# starts at a different offset in the pool, so streams never send the same
# byte sequence in lockstep (relevant for any compressing/dedup middlebox),
# while the full response is never held in memory.
POOL_SIZE = 4 * 1024 * 1024
RANDOM_POOL = os.urandom(POOL_SIZE)

NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, max-age=0",
    "Pragma": "no-cache",
    "X-Content-Type-Options": "nosniff",
}


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/info")
async def info(request: Request):
    """Basic server/client info shown in the test details."""
    forwarded = request.headers.get("x-forwarded-for")
    client_ip = (
        forwarded.split(",")[0].strip()
        if forwarded
        else (request.client.host if request.client else "unknown")
    )
    return JSONResponse(
        {
            "client_ip": client_ip,
            "server_time": time.time(),
            "max_bytes": MAX_BYTES,
        },
        headers=NO_CACHE_HEADERS,
    )


@app.get("/api/ping")
async def ping():
    """HTTP latency probe (fallback when WebSockets are unavailable)."""
    return Response(
        content=b"pong",
        media_type="text/plain",
        headers=NO_CACHE_HEADERS,
    )


@app.websocket("/ws/ping")
async def ws_ping(ws: WebSocket):
    """
    Echo endpoint for latency measurement.

    A WebSocket round trip has far less per-request overhead than an HTTP
    request (no headers, no new request/response cycle), so it is much closer
    to the real network RTT and can be repeated cheaply while a download or
    upload is running (loaded latency / bufferbloat).
    """
    await ws.accept()
    try:
        while True:
            message = await ws.receive_text()
            await ws.send_text(message)
    except WebSocketDisconnect:
        pass


async def generate_data(size: int):
    remaining = size
    # Random starting offset (multiple of 16 bytes) inside the pool.
    offset = (int.from_bytes(os.urandom(4), "big") % (POOL_SIZE - CHUNK_SIZE)) & ~0xF

    while remaining > 0:
        amount = min(CHUNK_SIZE, remaining)
        yield RANDOM_POOL[offset : offset + amount]
        remaining -= amount

        offset += CHUNK_SIZE
        if offset + CHUNK_SIZE > POOL_SIZE:
            offset = 0

        # Yield to the event loop so that latency probes (WebSocket echo)
        # are still served promptly while several downloads are streaming.
        await asyncio.sleep(0)


@app.get("/api/download")
async def download(size: int = 1024 * 1024):
    if size < 1 or size > MAX_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"Size must be between 1 and {MAX_BYTES} bytes",
        )

    headers = {
        **NO_CACHE_HEADERS,
        "Content-Length": str(size),
        "Content-Encoding": "identity",
    }

    return StreamingResponse(
        generate_data(size),
        media_type="application/octet-stream",
        headers=headers,
    )


@app.post("/api/upload")
async def upload(request: Request):
    total_bytes = 0
    start = time.perf_counter()

    try:
        async for chunk in request.stream():
            total_bytes += len(chunk)

            if total_bytes > MAX_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail="Upload exceeds maximum allowed size",
                )
    except ClientDisconnect:
        # The browser aborts in-flight uploads when the measurement window
        # ends. That is expected, not an error.
        return Response(status_code=204)

    elapsed = time.perf_counter() - start

    return JSONResponse(
        {
            "received_bytes": total_bytes,
            "server_receive_time_ms": round(elapsed * 1000, 2),
        },
        headers=NO_CACHE_HEADERS,
    )


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

# html=True serves frontend/index.html at "/", so no separate route is needed.
# This mount must stay last so it does not shadow the /api and /ws routes.
app.mount(
    "/",
    StaticFiles(directory=FRONTEND_DIR, html=True),
    name="frontend",
)