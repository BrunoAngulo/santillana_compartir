import unittest
from datetime import date
from io import BytesIO

import pandas as pd

from santillana_format.pegasus.reportes import (
    build_alumno_row,
    build_clase_row,
    build_report_filename,
    clean_report_rows,
    export_report_workbook,
    filter_report_rows,
)


class ReportesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.colegio = {"colegio_id": 10, "colegio": "Colegio Central"}

    def test_filters_classes_by_dimensions_status_and_participants(self) -> None:
        active = build_clase_row(
            {
                "clase_id": 1,
                "clase": "Matematica",
                "nivel_id": 39,
                "grado_id": 119,
                "seccion": "A",
                "activo": True,
                "baja": False,
            },
            self.colegio,
        )
        active["_alumnos_total"] = 12
        active["_profesores_total"] = 1
        inactive = build_clase_row(
            {
                "clase_id": 2,
                "clase": "Comunicacion",
                "nivel_id": 39,
                "grado_id": 120,
                "seccion": "B",
                "activo": False,
                "baja": True,
            },
            self.colegio,
        )
        inactive["_alumnos_total"] = 0
        inactive["_profesores_total"] = 0

        result = filter_report_rows(
            [active, inactive],
            nivel_ids=[39],
            grado_ids=[119],
            secciones=["A"],
            estado="activos",
            alumnos_clase="con",
            profesores_clase="con",
            search_text="matematica central",
        )

        self.assertEqual([row["Clase ID"] for row in result], [1])

    def test_filters_students_by_login_and_payment(self) -> None:
        student = build_alumno_row(
            {
                "alumno_id": 5,
                "nombre_completo": "Ana Torres",
                "nivel_id": 39,
                "grado_id": 119,
                "seccion": "A",
                "login": "ana.torres",
                "activo": True,
                "con_pago": True,
            },
            self.colegio,
        )

        self.assertEqual(
            len(
                filter_report_rows(
                    [student],
                    login="con",
                    pago="con",
                    estado="activos",
                )
            ),
            1,
        )
        self.assertEqual(
            filter_report_rows([student], login="sin"),
            [],
        )

    def test_dimension_filter_excludes_rows_with_missing_dimension(self) -> None:
        student = build_alumno_row(
            {
                "alumno_id": 5,
                "nombre_completo": "Ana Torres",
                "activo": True,
            },
            self.colegio,
        )

        self.assertEqual(
            filter_report_rows(
                [student],
                nivel_ids=[39],
                grado_ids=[119],
                secciones=["A"],
            ),
            [],
        )

    def test_export_creates_summary_data_and_errors_sheets(self) -> None:
        class_row = build_clase_row(
            {
                "clase_id": 1,
                "clase": "Matematica",
                "nivel_id": 39,
                "grado_id": 119,
                "seccion": "A",
                "activo": True,
            },
            self.colegio,
        )
        excel_bytes = export_report_workbook(
            {"Clases": [class_row]},
            summary_rows=[{"Entidad": "Clases", "Filas": 1}],
            errors=[{"Entidad": "Alumnos", "Detalle": "Sin acceso"}],
            config_rows=[{"Campo": "Colegios", "Valor": "1"}],
        )

        with pd.ExcelFile(BytesIO(excel_bytes), engine="openpyxl") as workbook:
            self.assertEqual(
                workbook.sheet_names,
                ["Resumen", "Configuracion", "Clases", "Errores"],
            )
            class_frame = pd.read_excel(workbook, sheet_name="Clases")
            self.assertNotIn("_clase_id", class_frame.columns)
            self.assertEqual(class_frame.loc[0, "Clase"], "Matematica")

    def test_clean_rows_removes_internal_fields(self) -> None:
        self.assertEqual(
            clean_report_rows([{"Visible": 1, "_private": 2}]),
            [{"Visible": 1}],
        )

    def test_filename_contains_scope_and_date(self) -> None:
        self.assertEqual(
            build_report_filename(3, generated_on=date(2026, 6, 14)),
            "reporte_pegasus_3_colegios_2026-06-14.xlsx",
        )


if __name__ == "__main__":
    unittest.main()
