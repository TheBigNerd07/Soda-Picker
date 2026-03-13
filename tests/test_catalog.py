from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from app.catalog import SodaCatalog


class SodaCatalogTests(unittest.TestCase):
    def test_parser_skips_disabled_and_bad_rows(self) -> None:
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

            self.assertEqual(len(sodas), 1)
            self.assertEqual(sodas[0].name, "Cola Prime")
            self.assertEqual(sodas[0].priority, 3)
            self.assertIn("Row 3", catalog.warnings[0])

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


if __name__ == "__main__":
    unittest.main()
