import copy
import json
import tempfile
import unittest
from pathlib import Path

from services.workload_catalog import CatalogError, load_catalog, resolve_workload


CATALOG_PATH = Path(__file__).parents[1] / "manifests" / "workloads.json"


class WorkloadCatalogTest(unittest.TestCase):
    def test_repository_catalog_is_valid_and_versioned(self):
        catalog = load_catalog(str(CATALOG_PATH))

        self.assertEqual(
            set(catalog),
            {("http-echo", "1.0.0"), ("request-inspector", "1.0.0")},
        )
        self.assertEqual(len(resolve_workload(catalog, "http-echo", "1.0.0").digest), 64)

    def test_rejects_duplicate_identity(self):
        documents = json.loads(CATALOG_PATH.read_text())
        documents.append(copy.deepcopy(documents[0]))

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as catalog_file:
            json.dump(documents, catalog_file)
            catalog_file.flush()
            with self.assertRaisesRegex(CatalogError, "duplicate workload"):
                load_catalog(catalog_file.name)

    def test_rejects_untrusted_container_capabilities(self):
        documents = json.loads(CATALOG_PATH.read_text())
        documents[0]["spec"]["container"]["privileged"] = True

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as catalog_file:
            json.dump(documents, catalog_file)
            catalog_file.flush()
            with self.assertRaisesRegex(CatalogError, "unsupported container fields"):
                load_catalog(catalog_file.name)

    def test_digest_changes_when_manifest_changes(self):
        original = load_catalog(str(CATALOG_PATH))[("http-echo", "1.0.0")]
        documents = json.loads(CATALOG_PATH.read_text())
        documents[0]["spec"]["container"]["args"][-1] = "-text=changed"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as catalog_file:
            json.dump(documents, catalog_file)
            catalog_file.flush()
            changed = load_catalog(catalog_file.name)[("http-echo", "1.0.0")]

        self.assertNotEqual(original.digest, changed.digest)


if __name__ == "__main__":
    unittest.main()
