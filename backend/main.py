
import os
import time
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, Response

from pathlib import Path
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

app = FastAPI(title="Cloud Speed Test API")

# Maximum 64 MiB per request
MAX_BYTES = 64 * 1024 * 1024
CHUNK_SIZE = 64 * 1024

# Generate random data once, then reuse it in chunks.
# The complete response is not stored in memory.
RANDOM_CHUNK = os.urandom(CHUNK_SIZE)

NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, max-age=0",
    "Pragma": "no-cache",
    "X-Content-Type-Options": "nosniff",
}


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/ping")
async def ping():
    return Response(
        content=b"pong",
        media_type="text/plain",
        headers=NO_CACHE_HEADERS,
    )


def generate_data(size: int):
    remaining = size

    while remaining > 0:
        amount = min(CHUNK_SIZE, remaining)
        yield RANDOM_CHUNK[:amount]
        remaining -= amount


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

    async for chunk in request.stream():
        total_bytes += len(chunk)

        if total_bytes > MAX_BYTES:
            raise HTTPException(
                status_code=413,
                detail="Upload exceeds maximum allowed size",
            )

    elapsed = time.perf_counter() - start

    return {
        "received_bytes": total_bytes,
        "server_receive_time_ms": round(elapsed * 1000, 2),
    }

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

@app.get("/")
async def homepage():
    return FileResponse(FRONTEND_DIR / "index.html")

app.mount(
    "/",
    StaticFiles(directory=FRONTEND_DIR),
    name="frontend"
)
