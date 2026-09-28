import asyncio
import concurrent.futures
import logging
import os
import re
import time

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.asyncio import Redis

from workload_catalog import Workload, load_catalog, resolve_workload, validate_digest


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"consumer","message":"%(message)s"}',
)
log = logging.getLogger("consumer")

STREAM = os.getenv("JOB_STREAM", "sandbox:jobs")
GROUP = os.getenv("CONSUMER_GROUP", "sandbox-consumers")
CONSUMER = os.getenv("HOSTNAME", "consumer")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "40"))
NAMESPACE = os.getenv("SANDBOX_NAMESPACE", "sandboxes")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost").rstrip("/")
JOB_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")

processed = Counter(
    "jobs_processed_total", "Jobs processed", ["result", "workload", "workload_version"]
)
startup = Histogram(
    "sandbox_startup_duration_seconds",
    "Time until sandbox pod is ready",
    ["workload", "workload_version"],
)
active = Gauge(
    "sandbox_active", "Sandboxes created by this consumer", ["workload", "workload_version"]
)
processing = Histogram(
    "job_processing_duration_seconds",
    "End-to-end consumer processing time",
    ["workload", "workload_version"],
)


def ensure_sandbox(
    core: client.CoreV1Api,
    networking: client.NetworkingV1Api,
    job_id: str,
    workload: Workload,
) -> bool:
    name = f"sandbox-{job_id.lower()}"
    labels = {
        "app": "sandbox",
        "job-id": job_id,
        "workload": workload.name,
        "workload-version": workload.version,
    }
    annotations = {"orchestration.duvo.ai/manifest-digest": workload.digest}
    container = workload.spec["container"]
    resources = workload.spec["resources"]
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=name, namespace=NAMESPACE, labels=labels, annotations=annotations
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",
            automount_service_account_token=False,
            enable_service_links=False,
            security_context=client.V1PodSecurityContext(
                run_as_non_root=True,
                run_as_user=65532,
                run_as_group=65532,
                seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
            ),
            containers=[client.V1Container(
                name="http",
                image=container["image"],
                image_pull_policy=container.get("imagePullPolicy", "IfNotPresent"),
                args=container.get("args", []),
                ports=[client.V1ContainerPort(container_port=container["port"])],
                resources=client.V1ResourceRequirements(
                    requests=resources["requests"],
                    limits=resources["limits"],
                ),
                security_context=client.V1SecurityContext(
                    allow_privilege_escalation=False,
                    capabilities=client.V1Capabilities(drop=["ALL"]),
                    read_only_root_filesystem=True,
                ),
                readiness_probe=client.V1Probe(
                    http_get=client.V1HTTPGetAction(
                        path=workload.spec["readiness"]["path"], port=container["port"]
                    ),
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
            ports=[client.V1ServicePort(port=80, target_port=container["port"])],
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

    try:
        existing = core.read_namespaced_pod(name, NAMESPACE)
    except ApiException as exc:
        if exc.status != 404:
            raise
    else:
        existing_digest = (existing.metadata.annotations or {}).get(
            "orchestration.duvo.ai/manifest-digest"
        )
        if existing_digest != workload.digest:
            raise ValueError(
                f"jobId {job_id} already exists with a different workload manifest"
            )

    created = False
    for create, body in (
        (core.create_namespaced_pod, pod),
        (core.create_namespaced_service, service),
        (networking.create_namespaced_ingress, ingress),
    ):
        try:
            create(NAMESPACE, body)
            if isinstance(body, client.V1Pod):
                created = True
        except ApiException as exc:
            if exc.status != 409:
                raise

    current = core.read_namespaced_pod(name, NAMESPACE)
    current_digest = (current.metadata.annotations or {}).get(
        "orchestration.duvo.ai/manifest-digest"
    )
    if current_digest != workload.digest:
        raise ValueError(
            f"jobId {job_id} already exists with a different workload manifest"
        )

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        current = core.read_namespaced_pod(name, NAMESPACE)
        if any(c.type == "Ready" and c.status == "True" for c in (current.status.conditions or [])):
            return created
        time.sleep(2)
    raise TimeoutError(f"sandbox {name} did not become ready")


def ensure_batch_job(
    batch: client.BatchV1Api,
    job_id: str,
    workload: Workload,
) -> bool:
    name = f"job-{job_id.lower()}"
    labels = {
        "app": "sandbox-job",
        "job-id": job_id,
        "workload": workload.name,
        "workload-version": workload.version,
    }
    annotations = {"orchestration.duvo.ai/manifest-digest": workload.digest}
    container = workload.spec["container"]
    resources = workload.spec["resources"]
    job = client.V1Job(
        metadata=client.V1ObjectMeta(
            name=name, namespace=NAMESPACE, labels=labels, annotations=annotations
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=workload.spec["ttlSecondsAfterFinished"],
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=labels, annotations=annotations),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    automount_service_account_token=False,
                    enable_service_links=False,
                    security_context=client.V1PodSecurityContext(
                        run_as_non_root=True,
                        run_as_user=65532,
                        run_as_group=65532,
                        seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
                    ),
                    containers=[client.V1Container(
                        name="job",
                        image=container["image"],
                        image_pull_policy=container.get("imagePullPolicy", "IfNotPresent"),
                        command=container["command"],
                        args=container.get("args", []),
                        env=[client.V1EnvVar(name="DUVO_JOB_ID", value=job_id)],
                        resources=client.V1ResourceRequirements(
                            requests=resources["requests"], limits=resources["limits"]
                        ),
                        security_context=client.V1SecurityContext(
                            allow_privilege_escalation=False,
                            capabilities=client.V1Capabilities(drop=["ALL"]),
                            read_only_root_filesystem=True,
                        ),
                    )],
                ),
            ),
        ),
    )

    try:
        existing = batch.read_namespaced_job(name, NAMESPACE)
    except ApiException as exc:
        if exc.status != 404:
            raise
    else:
        existing_digest = (existing.metadata.annotations or {}).get(
            "orchestration.duvo.ai/manifest-digest"
        )
        if existing_digest != workload.digest:
            raise ValueError(f"jobId {job_id} already exists with a different workload manifest")

    created = False
    try:
        batch.create_namespaced_job(NAMESPACE, job)
        created = True
    except ApiException as exc:
        if exc.status != 409:
            raise

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        current = batch.read_namespaced_job(name, NAMESPACE)
        current_digest = (current.metadata.annotations or {}).get(
            "orchestration.duvo.ai/manifest-digest"
        )
        if current_digest != workload.digest:
            raise ValueError(f"jobId {job_id} already exists with a different workload manifest")
        for condition in current.status.conditions or []:
            if condition.type == "Complete" and condition.status == "True":
                return created
            if condition.type == "Failed" and condition.status == "True":
                raise RuntimeError(f"Kubernetes Job {name} failed")
        time.sleep(0.5)
    raise TimeoutError(f"Kubernetes Job {name} did not complete")


async def process_message(
    message_id: str,
    fields: dict[str, str],
    redis: Redis,
    core: client.CoreV1Api,
    networking: client.NetworkingV1Api,
    batch: client.BatchV1Api,
    catalog: dict[tuple[str, str], Workload],
) -> None:
    job_id = fields.get("jobId", "")
    started = time.monotonic()
    try:
        if not JOB_ID.fullmatch(job_id):
            raise ValueError("invalid jobId")
        if fields.get("schemaVersion") != "1":
            raise ValueError("unsupported job schemaVersion")
        manifest_digest = fields.get("manifestDigest", "")
        if not validate_digest(manifest_digest):
            raise ValueError("invalid manifestDigest")
        workload = resolve_workload(
            catalog,
            fields.get("workloadName", ""),
            fields.get("workloadVersion", ""),
        )
        if workload.digest != manifest_digest:
            raise ValueError("manifest digest mismatch; producer and consumer catalogs differ")

        if workload.spec["execution"] == "batch":
            await asyncio.to_thread(ensure_batch_job, batch, job_id, workload)
            log.info(
                "batch_job_completed job_id=%s workload=%s version=%s",
                job_id, workload.name, workload.version,
            )
        else:
            created = await asyncio.to_thread(
                ensure_sandbox, core, networking, job_id, workload
            )
            startup.labels(
                workload=workload.name, workload_version=workload.version
            ).observe(time.monotonic() - started)
            if created:
                active.labels(
                    workload=workload.name, workload_version=workload.version
                ).inc()
            log.info(
                "sandbox_ready job_id=%s workload=%s version=%s url=%s/sandboxes/%s",
                job_id, workload.name, workload.version, PUBLIC_BASE_URL, job_id,
            )

        await redis.xack(STREAM, GROUP, message_id)
        processing.labels(
            workload=workload.name, workload_version=workload.version
        ).observe(time.monotonic() - started)
        processed.labels(
            result="success", workload=workload.name, workload_version=workload.version
        ).inc()
    except Exception:
        processed.labels(
            result="error",
            workload=fields.get("workloadName", "unknown"),
            workload_version=fields.get("workloadVersion", "unknown"),
        ).inc()
        log.exception("job_failed job_id=%s message_id=%s", job_id, message_id)


async def main() -> None:
    start_http_server(8000)
    asyncio.get_running_loop().set_default_executor(
        concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENCY)
    )
    config.load_incluster_config()
    core = client.CoreV1Api()
    networking = client.NetworkingV1Api()
    batch = client.BatchV1Api()
    redis = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
    catalog = load_catalog()
    try:
        await redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    log.info("consumer_started name=%s", CONSUMER)
    tasks: set[asyncio.Task[None]] = set()
    while True:
        completed = {task for task in tasks if task.done()}
        if completed:
            await asyncio.gather(*completed)
            tasks -= completed
        if len(tasks) >= MAX_CONCURRENCY:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            await asyncio.gather(*done)
            tasks -= done
            continue
        messages = await redis.xreadgroup(
            GROUP,
            CONSUMER,
            {STREAM: ">"},
            count=MAX_CONCURRENCY - len(tasks),
            block=1000,
        )
        for _, entries in messages:
            for message_id, fields in entries:
                tasks.add(asyncio.create_task(process_message(
                    message_id, fields, redis, core, networking, batch, catalog
                )))


if __name__ == "__main__":
    asyncio.run(main())
