import unittest
from io import BytesIO

import pandas as pd

from santillana_format.pegasus.censo import (
    CENSO_ALUMNOS_EXPORT_COLUMNS,
    build_censo_colegio_filename,
    build_flat_censo_zip_path,
    export_censo_alumnos_excel,
    normalize_censo_alumnos_export_rows,
)


class CensoPegasusTests(unittest.TestCase):
    def test_normalizes_persona_id_and_both_student_states(self) -> None:
        rows = normalize_censo_alumnos_export_rows(
            [
                {
                    "persona_id": 22526547,
                    "activo": False,
                    "nivel": "Secundaria",
                    "grado": "5",
                    "seccion": "A",
                    "nombre_completo": "Mathias Barrenechea",
                    "id_oficial": "81855389",
                },
                {
                    "personaId": 123,
                    "Estado": "Activo",
                    "Nombre del alumno": "Ana Torres",
                },
            ]
        )

        self.assertEqual(
            CENSO_ALUMNOS_EXPORT_COLUMNS[:2],
            ("personaId", "Estado"),
        )
        self.assertEqual(
            {row["personaId"]: row["Estado"] for row in rows},
            {
                "22526547": "Inactivo",
                "123": "Activo",
            },
        )

        with pd.ExcelFile(
            BytesIO(export_censo_alumnos_excel(rows)),
            engine="openpyxl",
        ) as workbook:
            self.assertEqual(workbook.sheet_names, ["alumnos"])
            frame = pd.read_excel(workbook, sheet_name="alumnos", dtype=str)
            self.assertEqual(
                list(frame.columns),
                list(CENSO_ALUMNOS_EXPORT_COLUMNS),
            )
            self.assertEqual(
                set(frame["Estado"].tolist()),
                {"Activo", "Inactivo"},
            )
            self.assertEqual(
                set(frame["personaId"].tolist()),
                {"22526547", "123"},
            )

    def test_builds_crm_and_school_filename_preserving_leading_zeroes(self) -> None:
        file_name = build_censo_colegio_filename(
            {
                "crm_id": "00018662",
                "colegio": "LICEO NAVAL JUAN MANUEL FANNING",
            },
            999,
        )

        self.assertEqual(
            file_name,
            "00018662 - LICEO NAVAL JUAN MANUEL FANNING.xlsx",
        )

    def test_multiple_schools_use_unique_files_in_one_zip_folder(self) -> None:
        used_names: set[str] = set()
        colegios = [
            (
                {"crm_id": "00018662", "colegio": "LICEO NAVAL"},
                10,
            ),
            (
                {"crm_id": "00020001", "colegio": "COLEGIO CENTRAL"},
                11,
            ),
            (
                {"crm_id": "00018662", "colegio": "LICEO NAVAL"},
                12,
            ),
        ]
        paths = [
            build_flat_censo_zip_path(
                "censo_alumnos_colegios_2026-06-15",
                build_censo_colegio_filename(
                    colegio,
                    colegio_id,
                    used_names=used_names,
                ),
            )
            for colegio, colegio_id in colegios
        ]

        self.assertEqual(len(paths), len(set(paths)))
        self.assertTrue(all(path.count("/") == 1 for path in paths))
        self.assertEqual(
            paths[0],
            "censo_alumnos_colegios_2026-06-15/"
            "00018662 - LICEO NAVAL.xlsx",
        )
        self.assertEqual(
            paths[2],
            "censo_alumnos_colegios_2026-06-15/"
            "00018662 - LICEO NAVAL - 12.xlsx",
        )


if __name__ == "__main__":
    unittest.main()
