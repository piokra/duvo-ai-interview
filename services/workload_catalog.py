"""Load and validate the trusted workload catalog shared by producer and consumer."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


NAME = re.compile(r"^[a-z][a-z0-9-]{0,39}$")
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
ALLOWED_PULL_POLICIES = {"IfNotPresent", "Always", "Never"}
REQUIRED_RESOURCES = {"cpu", "memory"}
DEFAULT_CATALOG_PATH = "/manifests/workloads.json"


class CatalogError(ValueError):
    pass


@dataclass(frozen=True)
class Workload:
    name: str
    version: str
    description: str
    spec: dict[str, Any]
    digest: str


def _canonical_digest(document: dict[str, Any]) -> str:
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _validate_manifest(document: Any, source: str) -> Workload:
    if not isinstance(document, dict):
        raise CatalogError(f"{source}: manifest must be an object")
    if document.get("apiVersion") != "orchestration.duvo.ai/v1alpha1":
        raise CatalogError(f"{source}: unsupported apiVersion")
    if document.get("kind") != "SandboxWorkload":
        raise CatalogError(f"{source}: unsupported kind")

    metadata = document.get("metadata")
    spec = document.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        raise CatalogError(f"{source}: metadata and spec must be objects")
    name, version = metadata.get("name"), metadata.get("version")
    if not isinstance(name, str) or not NAME.fullmatch(name):
        raise CatalogError(f"{source}: invalid metadata.name")
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise CatalogError(f"{source}: metadata.version must be semver x.y.z")
    if not isinstance(metadata.get("description"), str):
        raise CatalogError(f"{source}: metadata.description must be a string")
    if set(metadata) - {"name", "version", "description"}:
        raise CatalogError(f"{source}: unsupported metadata fields")

    container = spec.get("container")
    if not isinstance(container, dict):
        raise CatalogError(f"{source}: spec.container must be an object")
    image, port = container.get("image"), container.get("port")
    if (
        not isinstance(image, str)
        or not image
        or image.endswith(":latest")
        or not re.search(r"(?:@sha256:[0-9a-f]{64}|:[^/:]+)$", image)
    ):
        raise CatalogError(f"{source}: container.image must use an explicit non-latest tag")
    if not isinstance(port, int) or not 1 <= port <= 65535:
        raise CatalogError(f"{source}: container.port must be a valid TCP port")
    args = container.get("args", [])
    if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
        raise CatalogError(f"{source}: container.args must contain strings")
    pull_policy = container.get("imagePullPolicy", "IfNotPresent")
    if pull_policy not in ALLOWED_PULL_POLICIES:
        raise CatalogError(f"{source}: invalid imagePullPolicy")
    if set(container) - {"image", "imagePullPolicy", "port", "args"}:
        raise CatalogError(f"{source}: unsupported container fields")

    resources = spec.get("resources")
    if not isinstance(resources, dict) or set(resources) != {"requests", "limits"}:
        raise CatalogError(f"{source}: resources must define requests and limits")
    for boundary in ("requests", "limits"):
        values = resources[boundary]
        if not isinstance(values, dict) or set(values) != REQUIRED_RESOURCES:
            raise CatalogError(f"{source}: {boundary} must contain only cpu and memory")
        if not all(isinstance(value, str) and value for value in values.values()):
            raise CatalogError(f"{source}: resource values must be non-empty strings")

    readiness = spec.get("readiness")
    if not isinstance(readiness, dict) or set(readiness) != {"path"}:
        raise CatalogError(f"{source}: readiness must contain only path")
    if not isinstance(readiness["path"], str) or not readiness["path"].startswith("/"):
        raise CatalogError(f"{source}: readiness.path must start with /")

    allowed_spec_fields = {"container", "resources", "readiness"}
    if set(spec) - allowed_spec_fields:
        raise CatalogError(f"{source}: unsupported spec fields: {sorted(set(spec) - allowed_spec_fields)}")

    return Workload(
        name=name,
        version=version,
        description=metadata["description"],
        spec=spec,
        digest=_canonical_digest(document),
    )


def load_catalog(path: str | None = None) -> dict[tuple[str, str], Workload]:
    catalog_path = Path(path or os.getenv("WORKLOAD_CATALOG_PATH", DEFAULT_CATALOG_PATH))
    try:
        documents = json.loads(catalog_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogError(f"cannot load workload catalog {catalog_path}: {exc}") from exc
    if not isinstance(documents, list) or not documents:
        raise CatalogError("workload catalog must be a non-empty array")

    catalog: dict[tuple[str, str], Workload] = {}
    for index, document in enumerate(documents):
        workload = _validate_manifest(document, f"{catalog_path}[{index}]")
        key = (workload.name, workload.version)
        if key in catalog:
            raise CatalogError(f"duplicate workload {workload.name}@{workload.version}")
        catalog[key] = workload
    return catalog


def resolve_workload(
    catalog: dict[tuple[str, str], Workload], name: str, version: str
) -> Workload:
    try:
        return catalog[(name, version)]
    except KeyError as exc:
        raise CatalogError(f"unknown workload {name}@{version}") from exc


def validate_digest(value: str) -> bool:
    return bool(SHA256.fullmatch(value))
