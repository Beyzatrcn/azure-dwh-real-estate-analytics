import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from data_quality_checks import run_all_checks


class DataQualityChecksTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.results = run_all_checks(PROJECT_ROOT / "data" / "raw")

    def test_missing_business_keys_is_empty_for_demo_sources(self) -> None:
        self.assertEqual([], self.results["missing_business_keys"])

    def test_duplicate_checks_find_expected_demo_duplicates(self) -> None:
        duplicates = self.results["duplicates"]
        duplicate_entities = {(item["entity"], item["key_values"]) for item in duplicates}

        self.assertIn(("budgets", "PRJ-1013,V1,CAT-500,2025-03,6240000"), duplicate_entities)
        self.assertIn(("costs", "PRJ-1013,SUP-433,INV-2025-13003"), duplicate_entities)

    def test_negative_cost_values_detect_demo_issue(self) -> None:
        negative_cost_ids = {item["record_id"] for item in self.results["negative_cost_values"]}
        self.assertIn("CST-70050", negative_cost_ids)

    def test_missing_foreign_keys_detect_expected_demo_issues(self) -> None:
        missing_fk_pairs = {
            (item["record_id"], item["foreign_key_name"], item["foreign_key_value"])
            for item in self.results["missing_foreign_keys"]
        }

        self.assertIn(("BUD-50026", "project_id", "PRJ-9999"), missing_fk_pairs)
        self.assertIn(("CST-70046", "supplier_id", "SUP-999"), missing_fk_pairs)
        self.assertIn(("CST-70047", "employee_id", "EMP-999"), missing_fk_pairs)

    def test_negative_budget_values_is_empty_for_demo_sources(self) -> None:
        self.assertEqual([], self.results["negative_budget_values"])


if __name__ == "__main__":
    unittest.main()
