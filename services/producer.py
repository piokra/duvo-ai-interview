import logging
import os
import time
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, make_asgi_app
from redis.asyncio import Redis


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"producer","message":"%(message)s"}',
)
log = logging.getLogger("producer")
app = FastAPI(title="Sandbox job producer")
app.mount("/metrics", make_asgi_app())
redis = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)

jobs_created = Counter("jobs_produced_total", "Jobs accepted by the producer", ["type"])
publish_seconds = Histogram("job_publish_duration_seconds", "Time required to enqueue a job")


class JobRequest(BaseModel):
    jobId: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9-]{1,40}$")
    type: str = Field(default="http", pattern=r"^http$")


@app.get("/healthz")
async def healthz():
    try:
        return {"ok": bool(await redis.ping())}
    except Exception as exc:
        raise HTTPException(status_code=503, detail="redis unavailable") from exc


@app.post("/api/jobs", status_code=202)
async def create_job(request: JobRequest):
    job_id = request.jobId or uuid.uuid4().hex[:12]
    fields = {
        "jobId": job_id,
        "type": request.type,
        "createdAt": str(time.time()),
        "attempt": "0",
    }
    with publish_seconds.time():
        stream_id = await redis.xadd("sandbox:jobs", fields)
    jobs_created.labels(type=request.type).inc()
    log.info("job_enqueued job_id=%s stream_id=%s", job_id, stream_id)
    return {"jobId": job_id, "streamId": stream_id, "status": "queued"}
