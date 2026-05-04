from io import BytesIO
import unittest

from openpyxl import Workbook, load_workbook

from santillana_format.sumun import (
    generate_sumun_template_from_excel,
    inspect_sumun_workbook_sheets,
)


def _build_sumun_workbook(station_value: str) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "1-Ma4_Iti1"
    ws.append(
        [
            "ITINERARIO",
            "COMPETENCIA",
            "MACROHABILIDAD",
            "MICROHABILIDAD",
            "ESTACIÓN",
            "CONOCIMIENTOS",
            "RECORDAR",
            "COMPRENDER",
            "APLICAR",
            "ANALIZAR",
            "EVALUAR",
            "CREAR",
        ]
    )
    ws.append(
        [
            "Itinerario 1. La célula",
            "Competencia base",
            "Macro base",
            "Micro base",
            station_value,
            "Texto: conocimiento base",
            "Primer skill\n\nSegundo skill",
            None,
            None,
            None,
            None,
            None,
        ]
    )
    output = BytesIO()
    wb.save(output)
    return output.getvalue()


def _build_sumun_workbook_with_repeated_skills() -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "1-Ma4_Iti1"
    ws.append(
        [
            "ITINERARIO",
            "COMPETENCIA",
            "MACROHABILIDAD",
            "MICROHABILIDAD",
            "ESTACIÓN",
            "CONOCIMIENTOS",
            "RECORDAR",
            "COMPRENDER",
            "APLICAR",
            "ANALIZAR",
            "EVALUAR",
            "CREAR",
        ]
    )
    ws.append(
        [
            "Itinerario 1. La célula",
            "Competencia base",
            "Macro compartida",
            "Micro compartida",
            "E1 - Estación 1",
            "Texto: conocimiento base",
            "Skill 1",
            None,
            None,
            None,
            None,
            None,
        ]
    )
    ws.append(
        [
            "Itinerario 1. La célula",
            "Competencia base",
            "Macro compartida",
            "Micro compartida",
            "E2 - Estación 2",
            "Texto: conocimiento base",
            "Skill 2",
            None,
            None,
            None,
            None,
            None,
        ]
    )
    output = BytesIO()
    wb.save(output)
    return output.getvalue()


def _generated_rows(workbook_bytes: bytes) -> list[tuple]:
    wb = load_workbook(BytesIO(workbook_bytes))
    ws = wb[wb.sheetnames[0]]
    return [tuple(row) for row in ws.iter_rows(min_row=2, values_only=True)]


class SumunStationParsingTests(unittest.TestCase):
    def test_generate_template_accepts_common_station_formats(self) -> None:
        for station_value in ("1. Celula", "E1 - Celula", "Estación 1 - Celula"):
            with self.subTest(station_value=station_value):
                output_bytes, summary = generate_sumun_template_from_excel(
                    _build_sumun_workbook(station_value),
                    source_name="MA4.xlsx",
                )
                self.assertTrue(output_bytes)
                self.assertEqual(summary.generated_rows, 2)

    def test_inspection_reports_invalid_station_format(self) -> None:
        sheets = inspect_sumun_workbook_sheets(_build_sumun_workbook("Primera estacion"))
        self.assertEqual(len(sheets), 1)
        self.assertFalse(sheets[0].detected)
        self.assertEqual(sheets[0].estimated_rows, 0)
        self.assertIn("estaciones validas", sheets[0].reason)

    def test_macro_and_micro_ids_are_reused_across_station_changes(self) -> None:
        output_bytes, summary = generate_sumun_template_from_excel(
            _build_sumun_workbook_with_repeated_skills(),
            source_name="MA4.xlsx",
        )
        rows = _generated_rows(output_bytes)

        self.assertEqual(summary.generated_rows, 2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][5], None)
        self.assertEqual(rows[1][5], None)
        self.assertEqual(rows[0][8], 1)
        self.assertEqual(rows[1][8], 1)
        self.assertEqual(rows[0][10], 1)
        self.assertEqual(rows[1][10], 1)


if __name__ == "__main__":
    unittest.main()
