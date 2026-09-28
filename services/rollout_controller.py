"""Metric-driven progressive rollout controller for the synthetic workload."""

import json
import logging
import os
import time
import urllib.parse
import urllib.request

from prometheus_client import Gauge, start_http_server
from redis import Redis


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"rollout-controller","message":"%(message)s"}',
)
log = logging.getLogger("rollout-controller")

VM_URL = os.getenv("VM_URL", "http://victoriametrics.observability.svc.cluster.local:8428")
WORKLOAD = os.getenv("WORKLOAD", "dummy-job")
CANARY_VERSION = os.getenv("CANARY_VERSION", "2.0.0")
WEIGHT_KEY = os.getenv("WEIGHT_KEY", "rollout:dummy-job:canary-weight")
STEPS = [10, 25, 50, 100]
MIN_SAMPLES = int(os.getenv("MIN_SAMPLES", "5"))
FAILURE_THRESHOLD = float(os.getenv("FAILURE_THRESHOLD", "0.05"))
EVALUATION_SECONDS = int(os.getenv("EVALUATION_SECONDS", "15"))
redis = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)

weight_metric = Gauge("rollout_canary_weight_percent", "Current canary traffic weight", ["workload", "version"])
state_metric = Gauge("rollout_state", "Rollout state (1 for current state)", ["workload", "version", "state"])
sample_metric = Gauge("rollout_evaluated_samples", "Canary samples evaluated", ["workload", "version"])
error_rate_metric = Gauge("rollout_error_ratio", "Observed canary error ratio", ["workload", "version"])


def query(result: str) -> float:
    promql = (
        'sum(jobs_processed_total{workload="%s",workload_version="%s",result="%s"})'
        % (WORKLOAD, CANARY_VERSION, result)
    )
    url = f"{VM_URL}/api/v1/query?{urllib.parse.urlencode({'query': promql})}"
    with urllib.request.urlopen(url, timeout=5) as response:
        data = json.load(response)["data"]["result"]
    return float(data[0]["value"][1]) if data else 0.0


def publish(weight: int, state: str, samples: float, error_ratio: float) -> None:
    redis.set(WEIGHT_KEY, weight)
    weight_metric.labels(WORKLOAD, CANARY_VERSION).set(weight)
    sample_metric.labels(WORKLOAD, CANARY_VERSION).set(samples)
    error_rate_metric.labels(WORKLOAD, CANARY_VERSION).set(error_ratio)
    for candidate in ("progressing", "promoted", "rolled_back"):
        state_metric.labels(WORKLOAD, CANARY_VERSION, candidate).set(candidate == state)
    log.info(
        "rollout_evaluated state=%s weight=%s samples=%s error_ratio=%.3f",
        state, weight, samples, error_ratio,
    )


def main() -> None:
    start_http_server(8000)
    step_index = 0
    weight = STEPS[step_index]
    baseline_success = query("success")
    baseline_error = query("error")
    publish(weight, "progressing", 0, 0)
    while True:
        time.sleep(EVALUATION_SECONDS)
        successes = max(0, query("success") - baseline_success)
        errors = max(0, query("error") - baseline_error)
        samples = successes + errors
        error_ratio = errors / samples if samples else 0
        if samples >= MIN_SAMPLES and error_ratio > FAILURE_THRESHOLD:
            publish(0, "rolled_back", samples, error_ratio)
            while True:
                time.sleep(EVALUATION_SECONDS)
                publish(0, "rolled_back", samples, error_ratio)
        if samples >= MIN_SAMPLES * (step_index + 1):
            if step_index == len(STEPS) - 1:
                publish(100, "promoted", samples, error_ratio)
            else:
                step_index += 1
                weight = STEPS[step_index]
                publish(weight, "progressing", samples, error_ratio)
        else:
            publish(weight, "progressing", samples, error_ratio)


if __name__ == "__main__":
    main()
