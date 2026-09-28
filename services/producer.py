import logging
import os
import time
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from prometheus_client import Counter, Histogram, make_asgi_app
from redis.asyncio import Redis

from workload_catalog import CatalogError, load_catalog, resolve_workload


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"producer","message":"%(message)s"}',
)
log = logging.getLogger("producer")
app = FastAPI(title="Sandbox job producer")
app.mount("/metrics", make_asgi_app())
redis = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
catalog = load_catalog()

jobs_created = Counter(
    "jobs_produced_total", "Jobs accepted by the producer", ["workload", "workload_version"]
)
publish_seconds = Histogram("job_publish_duration_seconds", "Time required to enqueue a job")


class WorkloadReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(default="http-echo", pattern=r"^[a-z][a-z0-9-]{0,39}$")
    version: str = Field(default="1.0.0", pattern=r"^[0-9]+\.[0-9]+\.[0-9]+$")


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    jobId: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9-]{0,39}$")
    workload: WorkloadReference = Field(default_factory=WorkloadReference)


@app.get("/healthz")
async def healthz():
    try:
        return {"ok": bool(await redis.ping())}
    except Exception as exc:
        raise HTTPException(status_code=503, detail="redis unavailable") from exc


@app.get("/api/workloads")
async def list_workloads():
    return {
        "workloads": [
            {
                "name": workload.name,
                "version": workload.version,
                "description": workload.description,
                "manifestDigest": workload.digest,
            }
            for workload in sorted(catalog.values(), key=lambda item: (item.name, item.version))
        ]
    }


@app.post("/api/jobs", status_code=202)
async def create_job(request: JobRequest):
    job_id = request.jobId or uuid.uuid4().hex[:12]
    try:
        workload = resolve_workload(catalog, request.workload.name, request.workload.version)
    except CatalogError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    fields = {
        "schemaVersion": "1",
        "jobId": job_id,
        "workloadName": workload.name,
        "workloadVersion": workload.version,
        "manifestDigest": workload.digest,
        "createdAt": str(time.time()),
        "attempt": "0",
    }
    with publish_seconds.time():
        stream_id = await redis.xadd("sandbox:jobs", fields)
    jobs_created.labels(workload=workload.name, workload_version=workload.version).inc()
    log.info(
        "job_enqueued job_id=%s workload=%s version=%s digest=%s stream_id=%s",
        job_id, workload.name, workload.version, workload.digest, stream_id,
    )
    return {
        "jobId": job_id,
        "streamId": stream_id,
        "status": "queued",
        "workload": {"name": workload.name, "version": workload.version},
        "manifestDigest": workload.digest,
    }
