import unittest
from io import BytesIO
from unittest.mock import patch

import pandas as pd

from santillana_format.pegasus.alumnos import (
    EXCEL_MASIVO_ESTUDIANTES_COLUMNS,
    descargar_excel_masivo_estudiantes,
    transformar_excel_masivo_estudiantes,
)


def _build_plantilla_bd_excel() -> bytes:
    frame = pd.DataFrame(
        [
            {
                "Nivel": "Secundaria",
                "Grado": "5to secundaria",
                "Grupo": "A",
                "NUI": 22526547,
                "Activo": "No",
                "Nombre": "Mathias Fernando",
                "Apellido Paterno": "Barrenechea",
                "Apellido materno": "Melgarejo",
                "NUIP": "81855389",
                "Login": "mathias.b",
                "Password": "clave",
            },
            {
                "Nivel": "Primaria",
                "Grado": "1ro primaria",
                "Grupo": "B",
                "NUI": 100,
                "Activo": "Si",
                "Nombre": "Ana",
                "Apellido Paterno": "Torres",
                "Apellido materno": "",
                "NUIP": "00001234",
                "Login": "ana.t",
                "Password": "",
            },
        ]
    )
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="Plantilla_BD")
        pd.DataFrame().to_excel(
            writer,
            index=False,
            sheet_name="Plantilla_Actualizada",
        )
    return output.getvalue()


class _ExcelResponse:
    ok = True
    status_code = 200
    headers = {
        "Content-Type": (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    }

    def __init__(self, content: bytes) -> None:
        self.content = content

    def json(self):
        raise ValueError("binary response")


class ExcelMasivoEstudiantesTests(unittest.TestCase):
    def test_transforms_downloaded_workbook_to_requested_columns(self) -> None:
        output_bytes, summary = transformar_excel_masivo_estudiantes(
            _build_plantilla_bd_excel()
        )

        with pd.ExcelFile(BytesIO(output_bytes), engine="openpyxl") as workbook:
            self.assertEqual(workbook.sheet_names, ["estudiantes"])
            frame = pd.read_excel(
                workbook,
                sheet_name="estudiantes",
                dtype=str,
            ).fillna("")

        self.assertEqual(
            list(frame.columns),
            EXCEL_MASIVO_ESTUDIANTES_COLUMNS,
        )
        self.assertEqual(summary["alumnos_total"], 2)
        self.assertEqual(frame.loc[0, "NUI"], "22526547")
        self.assertEqual(frame.loc[0, "Estado"], "Inactivo")
        self.assertEqual(
            frame.loc[0, "Nombre del alumno"],
            "Mathias Fernando Barrenechea Melgarejo",
        )
        self.assertEqual(frame.loc[0, "DNI"], "81855389")
        self.assertEqual(frame.loc[1, "Estado"], "Activo")
        self.assertEqual(frame.loc[1, "DNI"], "00001234")

    @patch("santillana_format.pegasus.alumnos.requests.get")
    def test_download_uses_descargar_one(self, mock_get) -> None:
        mock_get.return_value = _ExcelResponse(_build_plantilla_bd_excel())

        output_bytes, summary = descargar_excel_masivo_estudiantes(
            token="token-demo",
            colegio_id=9039,
            empresa_id=11,
            ciclo_id=207,
            timeout=45,
        )

        self.assertTrue(output_bytes)
        self.assertEqual(summary["alumnos_total"], 2)
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs["params"], {"descargar": 1})
        self.assertEqual(kwargs["timeout"], 45)
        self.assertEqual(
            kwargs["headers"]["Authorization"],
            "Bearer token-demo",
        )


if __name__ == "__main__":
    unittest.main()
