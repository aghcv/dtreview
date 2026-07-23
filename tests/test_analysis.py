import importlib.util
from pathlib import Path
import sys
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "analysis" / "generate_outputs.py"
SPEC = importlib.util.spec_from_file_location("generate_outputs", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class AnalysisHelpersTest(unittest.TestCase):
    def test_normalize_removes_null_phrases(self):
        self.assertEqual(MODULE.normalize_text("Not reported; FHIR"), "; fhir")

    def test_match_categories_counts_once_per_record(self):
        frame = pd.DataFrame({"field": ["FHIR and FHIR", "HL7", None]})
        mask = MODULE.match_categories(frame, ["field"], {"FHIR": r"\bfhir\b", "HL7": r"\bhl7\b"})
        self.assertEqual(int(mask["FHIR"].sum()), 1)
        self.assertEqual(int(mask["HL7"].sum()), 1)

    def test_number_parser(self):
        self.assertEqual(MODULE.parse_first_number("152 publications"), 152.0)
        self.assertTrue(pd.isna(MODULE.parse_first_number("Not reported")))

    def test_latex_escape(self):
        self.assertEqual(MODULE.latex_escape("A_B & 5%"), r"A\_B \& 5\%")

    def test_prisma_inconsistency_detected(self):
        counts = {
            "identified_databases": 2898,
            "identified_registers": 416,
            "duplicates_removed": 752,
            "automation_removed": 0,
            "other_removed": 5,
            "screened": 2557,
            "screening_excluded": 2307,
            "reports_sought": 250,
            "reports_not_retrieved": 3,
            "reports_assessed": 247,
            "excluded_reason_1": 179,
            "excluded_reason_2": 9,
            "excluded_reason_3": 0,
            "studies_included": 64,
            "reports_included": 64,
        }
        warnings = MODULE.prisma_checks(counts)
        self.assertEqual(len(warnings), 1)
        self.assertIn("imply 59", warnings[0])

    def test_categorical_cell_offsets_are_unique_and_bounded(self):
        offsets = MODULE.categorical_cell_offsets(14)
        self.assertEqual(len(offsets), 14)
        self.assertEqual(len(set(offsets)), 14)
        self.assertTrue(all(abs(x) <= 0.38 for x, _ in offsets))
        self.assertTrue(all(abs(y) <= 0.30 for _, y in offsets))

    def test_spread_points_remain_in_their_original_cells(self):
        frame = pd.DataFrame(
            {
                "article id": range(1, 15),
                "included_studies_numeric": range(14, 0, -1),
                "coupling_score": [2.0] * 14,
                "hierarchy_score": [3.0] * 14,
            }
        )
        first = MODULE.spread_points_within_cells(frame)
        second = MODULE.spread_points_within_cells(frame)
        self.assertEqual(len(set(zip(first["x_plot"], first["y_plot"]))), 14)
        self.assertTrue(first["x_plot"].between(1.5, 2.5, inclusive="neither").all())
        self.assertTrue(first["y_plot"].between(2.5, 3.5, inclusive="neither").all())
        pd.testing.assert_series_equal(first["x_plot"], second["x_plot"])
        pd.testing.assert_series_equal(first["y_plot"], second["y_plot"])


if __name__ == "__main__":
    unittest.main()
