"""Generate versioned synthetic jobs at a stable, configurable rate."""

import json
import logging
import os
import time
import urllib.error
import urllib.request

from redis import Redis


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"load-generator","message":"%(message)s"}',
)
log = logging.getLogger("load-generator")

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://producer/api/jobs")
WORKLOAD_NAME = os.getenv("WORKLOAD_NAME", "dummy-job")
STABLE_VERSION = os.getenv("STABLE_VERSION", "1.1.0")
CANARY_VERSION = os.getenv("CANARY_VERSION", "2.0.0")
WEIGHT_KEY = os.getenv("WEIGHT_KEY", "rollout:dummy-job:canary-weight")
RATE = float(os.getenv("RATE_PER_SECOND", "10"))
redis = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)


def submit(sequence: int) -> None:
    weight = int(redis.get(WEIGHT_KEY) or 0)
    version = CANARY_VERSION if (sequence * 37) % 100 < weight else STABLE_VERSION
    job_id = f"load-{time.time_ns()}-{sequence % 1_000_000:06d}"
    payload = json.dumps({
        "jobId": job_id,
        "workload": {"name": WORKLOAD_NAME, "version": version},
    }).encode()
    request = urllib.request.Request(
        PRODUCER_URL,
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        if response.status != 202:
            raise RuntimeError(f"producer returned {response.status}")


def main() -> None:
    if RATE <= 0:
        raise ValueError("RATE_PER_SECOND must be positive")
    interval = 1.0 / RATE
    sequence = 1
    deadline = time.monotonic()
    log.info(
        "generator_started rate=%s workload=%s stable=%s canary=%s",
        RATE, WORKLOAD_NAME, STABLE_VERSION, CANARY_VERSION,
    )
    while True:
        delay = deadline - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        try:
            submit(sequence)
            sequence += 1
            if sequence % 100 == 0:
                log.info("jobs_submitted total=%s", sequence)
        except (OSError, urllib.error.HTTPError, RuntimeError):
            log.exception("job_submission_failed sequence=%s", sequence)
        deadline = max(deadline + interval, time.monotonic())


if __name__ == "__main__":
    main()
