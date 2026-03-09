from __future__ import annotations

import unittest

from sqlalchemy import create_engine

from app.infra.db.health import collect_sql_asset_health
from app.infra.db.pipeline import review_sql_assets
from app.infra.db.sql_assets import get_sql_asset_registry
from app.services.db_platform_flow import get_database_platform_status


class DbPlatformAssetsTest(unittest.TestCase):
    def test_registry_points_to_existing_sql_files(self):
        registry = get_sql_asset_registry()
        self.assertGreaterEqual(len(registry), 3)
        for spec in registry:
            self.assertTrue(spec.path.exists(), f"Falta SQL asset: {spec.path}")
            sql = spec.path.read_text(encoding="utf-8").lower()
            self.assertIn("create", sql if "alter" not in sql else sql)

    def test_registry_contains_expected_tokens(self):
        by_id = {spec.asset_id: spec for spec in get_sql_asset_registry()}
        self.assertIn("materialized view", by_id["materialized_views"].path.read_text(encoding="utf-8").lower())
        self.assertIn("partition", by_id["partition_vitals"].path.read_text(encoding="utf-8").lower())
        self.assertIn("vector", by_id["migration_vector"].path.read_text(encoding="utf-8").lower())

    def test_sql_assets_review_gracefully_on_sqlite(self):
        review = review_sql_assets(dsn="sqlite:///:memory:")
        self.assertEqual(review["mode"], "sqlite")
        self.assertEqual(review["database_scope"], "clinical")
        self.assertEqual(review["total"], 0)
        self.assertTrue(review["ready"])

    def test_asset_health_collects_not_applicable_for_sqlite(self):
        engine = create_engine("sqlite:///:memory:")
        try:
            health = collect_sql_asset_health(database_scope="clinical", url="sqlite:///:memory:", bind=engine)
        finally:
            engine.dispose()
        self.assertEqual(health["mode"], "sqlite")
        self.assertEqual(health["total"], 0)
        self.assertTrue(all(item["status"] == "not_applicable" for item in health["items"]))

    def test_database_platform_status_exposes_assets_readiness(self):
        status = get_database_platform_status()
        self.assertIn("platform_assets_ready", status)
        self.assertIn("platform_assets", status["clinical"])
        self.assertIn("platform_assets", status["surgical"])


if __name__ == "__main__":
    unittest.main()
