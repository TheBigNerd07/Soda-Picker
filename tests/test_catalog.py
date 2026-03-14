from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from app.catalog import SodaCatalog


class SodaCatalogTests(unittest.TestCase):
    def test_parser_skips_disabled_bad_rows_and_reports_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sodas.csv"
            csv_path.write_text(
                textwrap.dedent(
                    """\
                    name,brand,caffeine_mg,is_diet,priority,enabled,tags
                    Cola Prime,Example,38,false,3,true,classic|cola
                    Missing Caffeine,Example,abc,false,1,true,oops
                    Disabled Drink,Example,0,false,2,false,skip
                    """
                ),
                encoding="utf-8",
            )

            catalog = SodaCatalog(str(csv_path))
            sodas = catalog.list_sodas()
            diagnostics = catalog.diagnostics

            self.assertEqual(len(sodas), 1)
            self.assertEqual(sodas[0].name, "Cola Prime")
            self.assertEqual(diagnostics.loaded_rows, 1)
            self.assertEqual(diagnostics.disabled_rows, 1)
            self.assertEqual(diagnostics.invalid_rows, 1)
            self.assertIn("category", diagnostics.missing_optional_columns)
            self.assertTrue(any("Row 3" in warning for warning in catalog.warnings))

    def test_missing_optional_columns_are_handled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sodas.csv"
            csv_path.write_text(
                textwrap.dedent(
                    """\
                    name
                    Mystery Fizz
                    """
                ),
                encoding="utf-8",
            )

            catalog = SodaCatalog(str(csv_path))
            sodas = catalog.list_sodas()

            self.assertEqual(len(sodas), 1)
            self.assertEqual(sodas[0].name, "Mystery Fizz")
            self.assertEqual(sodas[0].caffeine_mg, 0.0)
            self.assertTrue(sodas[0].is_caffeine_free)

    def test_duplicate_names_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sodas.csv"
            csv_path.write_text(
                textwrap.dedent(
                    """\
                    name,brand,caffeine_mg,enabled
                    Cola Prime,Example,38,true
                    Cola Prime,Example,12,true
                    """
                ),
                encoding="utf-8",
            )

            catalog = SodaCatalog(str(csv_path))
            catalog.list_sodas()

            self.assertIn("Example Cola Prime", catalog.diagnostics.duplicate_names)


if __name__ == "__main__":
    unittest.main()
