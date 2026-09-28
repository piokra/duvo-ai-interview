import asyncio
import logging
import os
import re
import time

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.asyncio import Redis


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"consumer","message":"%(message)s"}',
)
log = logging.getLogger("consumer")

STREAM = "sandbox:jobs"
GROUP = "sandbox-consumers"
CONSUMER = os.getenv("HOSTNAME", "consumer")
NAMESPACE = os.getenv("SANDBOX_NAMESPACE", "sandboxes")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost").rstrip("/")
JOB_ID = re.compile(r"^[a-zA-Z0-9-]{1,40}$")

processed = Counter("jobs_processed_total", "Jobs processed", ["result"])
startup = Histogram("sandbox_startup_duration_seconds", "Time until sandbox pod is ready")
active = Gauge("sandbox_active", "Sandboxes created by this consumer")


def ensure_sandbox(core: client.CoreV1Api, networking: client.NetworkingV1Api, job_id: str) -> None:
    name = f"sandbox-{job_id.lower()}"
    labels = {"app": "sandbox", "job-id": job_id}
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, namespace=NAMESPACE, labels=labels),
        spec=client.V1PodSpec(
            restart_policy="Never",
            automount_service_account_token=False,
            containers=[client.V1Container(
                name="http",
                image="hashicorp/http-echo:1.0",
                args=["-listen=:5678", f"-text=sandbox {job_id} is ready"],
                ports=[client.V1ContainerPort(container_port=5678)],
                resources=client.V1ResourceRequirements(
                    requests={"cpu": "10m", "memory": "16Mi"},
                    limits={"cpu": "100m", "memory": "64Mi"},
                ),
                readiness_probe=client.V1Probe(
                    http_get=client.V1HTTPGetAction(path="/", port=5678),
                    initial_delay_seconds=1,
                    period_seconds=2,
                ),
            )],
        ),
    )
    service = client.V1Service(
        metadata=client.V1ObjectMeta(name=name, namespace=NAMESPACE, labels=labels),
        spec=client.V1ServiceSpec(
            selector=labels,
            ports=[client.V1ServicePort(port=80, target_port=5678)],
        ),
    )
    ingress = client.V1Ingress(
        metadata=client.V1ObjectMeta(name=name, namespace=NAMESPACE, labels=labels),
        spec=client.V1IngressSpec(rules=[client.V1IngressRule(
            http=client.V1HTTPIngressRuleValue(paths=[client.V1HTTPIngressPath(
                path=f"/sandboxes/{job_id}",
                path_type="Prefix",
                backend=client.V1IngressBackend(service=client.V1IngressServiceBackend(
                    name=name,
                    port=client.V1ServiceBackendPort(number=80),
                )),
            )])
        )]),
    )

    for create, body in (
        (core.create_namespaced_pod, pod),
        (core.create_namespaced_service, service),
        (networking.create_namespaced_ingress, ingress),
    ):
        try:
            create(NAMESPACE, body)
        except ApiException as exc:
            if exc.status != 409:
                raise

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        current = core.read_namespaced_pod(name, NAMESPACE)
        if any(c.type == "Ready" and c.status == "True" for c in (current.status.conditions or [])):
            return
        time.sleep(2)
    raise TimeoutError(f"sandbox {name} did not become ready")


async def main() -> None:
    start_http_server(8000)
    config.load_incluster_config()
    core = client.CoreV1Api()
    networking = client.NetworkingV1Api()
    redis = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
    try:
        await redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    log.info("consumer_started name=%s", CONSUMER)
    while True:
        messages = await redis.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=1, block=5000)
        for _, entries in messages:
            for message_id, fields in entries:
                job_id = fields.get("jobId", "")
                started = time.monotonic()
                try:
                    if not JOB_ID.fullmatch(job_id):
                        raise ValueError("invalid jobId")
                    await asyncio.to_thread(ensure_sandbox, core, networking, job_id)
                    await redis.xack(STREAM, GROUP, message_id)
                    startup.observe(time.monotonic() - started)
                    active.inc()
                    processed.labels(result="success").inc()
                    log.info("sandbox_ready job_id=%s url=%s/sandboxes/%s", job_id, PUBLIC_BASE_URL, job_id)
                except Exception:
                    processed.labels(result="error").inc()
                    log.exception("job_failed job_id=%s message_id=%s", job_id, message_id)


if __name__ == "__main__":
    asyncio.run(main())
