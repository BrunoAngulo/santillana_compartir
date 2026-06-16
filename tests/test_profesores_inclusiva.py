import unittest
from unittest.mock import patch

from santillana_format.pegasus.profesores_manual import (
    asignar_santillana_inclusiva_profesores,
    build_santillana_inclusiva_sin_docente_report_rows,
    build_santillana_inclusiva_profesores_plan,
    export_santillana_inclusiva_sin_docente_excel,
)


class ProfesoresInclusivaPlanTests(unittest.TestCase):
    def test_reports_inclusive_classes_without_any_teacher(self) -> None:
        clases = [
            self._class(
                301,
                39,
                1,
                11,
                "1 Primaria",
                "A",
                "Santillana Inclusiva 1PA",
                inclusiva=True,
                staff_count=0,
            ),
            self._class(
                302,
                39,
                1,
                12,
                "1 Primaria",
                "B",
                "Santillana Inclusiva 1PB",
                inclusiva=True,
                staff_count=1,
                staff_persona_ids=[10],
            ),
            self._class(101, 39, 1, 11, "1 Primaria", "A", "Matematica"),
            self._class(
                303,
                39,
                2,
                22,
                "2 Primaria",
                "B",
                "Santillana Inclusiva 2PB",
                inclusiva=True,
                staff_count=0,
            ),
        ]

        rows = build_santillana_inclusiva_sin_docente_report_rows(
            clases,
            colegio_label="Colegio Demo",
            exclude_clase_ids=[303],
        )

        self.assertEqual(
            rows,
            [
                {
                    "Colegio": "Colegio Demo",
                    "Clase": "Santillana Inclusiva 1PA",
                    "Grado": "1 Primaria",
                    "Seccion": "A",
                }
            ],
        )

    def test_exports_empty_inclusive_without_teacher_report_with_headers(self) -> None:
        output = export_santillana_inclusiva_sin_docente_excel([])
        import pandas as pd
        from io import BytesIO

        frame = pd.read_excel(
            BytesIO(output),
            sheet_name="sin_docente",
            dtype=str,
            engine="openpyxl",
        )

        self.assertEqual(
            list(frame.columns),
            ["Colegio", "Clase", "Grado", "Seccion"],
        )

    def test_assigns_only_matching_primary_grade_and_section(self) -> None:
        profesores = [
            {
                "persona_id": 10,
                "nombre": "Docente Primaria",
                "niveles_presentes": [39],
                "clase_ids_actuales": [101, 102],
            },
            {
                "persona_id": 20,
                "nombre": "Docente Secundaria",
                "niveles_presentes": [40],
                "clase_ids_actuales": [201],
            },
        ]
        clases = [
            self._class(101, 39, 1, 11, "1 Primaria", "A", "Matematica"),
            self._class(102, 39, 2, 22, "2 Primaria", "B", "Comunicacion"),
            self._class(201, 40, 1, 31, "1 Secundaria", "A", "Matematica"),
            self._class(
                301,
                39,
                1,
                11,
                "1 Primaria",
                "A",
                "Santillana Inclusiva 1PA",
                inclusiva=True,
            ),
            self._class(
                302,
                39,
                1,
                12,
                "1 Primaria",
                "B",
                "Santillana Inclusiva 1PB",
                inclusiva=True,
            ),
            self._class(
                303,
                39,
                2,
                22,
                "2 Primaria",
                "B",
                "Santillana Inclusiva 2PB",
                inclusiva=True,
            ),
            self._class(
                304,
                40,
                1,
                31,
                "1 Secundaria",
                "A",
                "Santillana Inclusiva 1SA",
                inclusiva=True,
            ),
        ]

        plan, summary = build_santillana_inclusiva_profesores_plan(
            profesores,
            clases,
        )

        self.assertEqual(len(plan), 1)
        self.assertEqual(
            [row["clase_id"] for row in plan[0]["clases_pendientes"]],
            [301, 303],
        )
        self.assertEqual(summary["docentes_primaria"], 1)
        self.assertEqual(summary["asignaciones_pendientes"], 2)

    def test_keeps_primary_teacher_without_base_classes_visible(self) -> None:
        profesores = [
            {
                "persona_id": 10,
                "nombre": "Docente sin clases",
                "niveles_activos": {39: True},
                "clase_ids_actuales": [],
            }
        ]

        plan, summary = build_santillana_inclusiva_profesores_plan(
            profesores,
            [],
        )

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]["contextos"], [])
        self.assertEqual(plan[0]["clases_pendientes"], [])
        self.assertEqual(summary["docentes_con_contexto"], 0)

    def test_does_not_repeat_already_assigned_inclusive_class(self) -> None:
        profesores = [
            {
                "persona_id": 10,
                "nombre": "Docente Primaria",
                "niveles_presentes": [39],
                "clase_ids_actuales": [101, 301],
            }
        ]
        clases = [
            self._class(101, 39, 1, 11, "1 Primaria", "A", "Matematica"),
            self._class(
                301,
                39,
                1,
                11,
                "1 Primaria",
                "A",
                "Santillana Inclusiva 1PA",
                inclusiva=True,
            ),
        ]

        plan, summary = build_santillana_inclusiva_profesores_plan(
            profesores,
            clases,
        )

        self.assertEqual(plan[0]["clases_pendientes"], [])
        self.assertEqual(
            [row["clase_id"] for row in plan[0]["clases_ya_asignadas"]],
            [301],
        )
        self.assertEqual(plan[0]["clases_a_retirar"], [])
        self.assertEqual(summary["asignaciones_pendientes"], 0)

    def test_removes_inclusive_class_when_it_is_the_only_course_in_context(
        self,
    ) -> None:
        profesores = [
            {
                "persona_id": 10,
                "nombre": "Docente Primaria",
                "niveles_presentes": [39],
                "clase_ids_actuales": [101, 301, 302],
            }
        ]
        clases = [
            self._class(101, 39, 1, 11, "1 Primaria", "A", "Matematica"),
            self._class(
                301,
                39,
                1,
                11,
                "1 Primaria",
                "A",
                "Santillana Inclusiva 1PA",
                inclusiva=True,
            ),
            self._class(
                302,
                39,
                2,
                22,
                "2 Primaria",
                "B",
                "Santillana Inclusiva 2PB",
                inclusiva=True,
            ),
        ]

        plan, summary = build_santillana_inclusiva_profesores_plan(
            profesores,
            clases,
        )

        self.assertEqual(
            [row["clase_id"] for row in plan[0]["clases_ya_asignadas"]],
            [301],
        )
        self.assertEqual(
            [row["clase_id"] for row in plan[0]["clases_a_retirar"]],
            [302],
        )
        self.assertEqual(summary["asignaciones_pendientes"], 0)
        self.assertEqual(summary["retiros_pendientes"], 1)
        self.assertEqual(summary["cambios_pendientes"], 1)

    def test_does_not_remove_inclusive_class_without_section_context(
        self,
    ) -> None:
        profesores = [
            {
                "persona_id": 10,
                "nombre": "Docente Primaria",
                "niveles_presentes": [39],
                "clase_ids_actuales": [301],
            }
        ]
        clases = [
            self._class(
                301,
                39,
                1,
                None,
                "1 Primaria",
                "",
                "Santillana Inclusiva",
                inclusiva=True,
            )
        ]

        plan, summary = build_santillana_inclusiva_profesores_plan(
            profesores,
            clases,
        )

        self.assertEqual(plan[0]["clases_a_retirar"], [])
        self.assertEqual(summary["retiros_pendientes"], 0)

    @patch(
        "santillana_format.pegasus.profesores_manual._assign_staff_profesor",
        return_value=(True, None),
    )
    @patch(
        "santillana_format.pegasus.profesores_manual._fetch_staff_profesores_detalle",
        return_value=([], None),
    )
    def test_apply_only_adds_pending_assignments(
        self,
        fetch_staff,
        assign_staff,
    ) -> None:
        plan = [
            {
                "persona_id": 10,
                "nombre": "Docente Primaria",
                "clases_pendientes": [
                    {"clase_id": 301, "clase_label": "Santillana Inclusiva 1PA"}
                ],
            }
        ]

        summary, results = asignar_santillana_inclusiva_profesores(
            token="token",
            plan_rows=plan,
        )

        self.assertEqual(summary["asignadas"], 1)
        self.assertEqual(results[0]["estado"], "asignada")
        fetch_staff.assert_called_once()
        assign_staff.assert_called_once()
        self.assertEqual(assign_staff.call_args.kwargs["persona_id"], 10)
        self.assertEqual(assign_staff.call_args.kwargs["clase_id"], 301)

    @patch(
        "santillana_format.pegasus.profesores_manual._unassign_staff_profesor",
        return_value=(True, None),
    )
    @patch(
        "santillana_format.pegasus.profesores_manual._fetch_staff_profesores_detalle",
        return_value=([{"persona_id": 10}], None),
    )
    def test_apply_removes_orphan_inclusive_assignment(
        self,
        fetch_staff,
        unassign_staff,
    ) -> None:
        plan = [
            {
                "persona_id": 10,
                "nombre": "Docente Primaria",
                "clases_pendientes": [],
                "clases_a_retirar": [
                    {"clase_id": 302, "clase_label": "Santillana Inclusiva 2PB"}
                ],
            }
        ]

        summary, results = asignar_santillana_inclusiva_profesores(
            token="token",
            plan_rows=plan,
        )

        self.assertEqual(summary["retiradas"], 1)
        self.assertEqual(summary["asignadas"], 0)
        self.assertEqual(results[0]["estado"], "retirada")
        fetch_staff.assert_called_once()
        unassign_staff.assert_called_once()
        self.assertEqual(unassign_staff.call_args.kwargs["persona_id"], 10)
        self.assertEqual(unassign_staff.call_args.kwargs["clase_id"], 302)

    @staticmethod
    def _class(
        clase_id,
        nivel_id,
        grado_id,
        grupo_id,
        grado,
        seccion,
        nombre,
        inclusiva=False,
        staff_count=0,
        staff_persona_ids=None,
    ):
        return {
            "clase_id": clase_id,
            "nivel_id": nivel_id,
            "grado_id": grado_id,
            "grupo_id": grupo_id,
            "grado": grado,
            "seccion": seccion,
            "clase": nombre,
            "clase_label": nombre,
            "es_santillana_inclusiva": inclusiva,
            "staff_count": staff_count,
            "staff_persona_ids": staff_persona_ids or [],
        }


if __name__ == "__main__":
    unittest.main()
