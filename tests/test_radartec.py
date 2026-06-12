import unittest

from santillana_format.pegasus.profesores import _extract_login_activo
from santillana_format.pegasus.profesores_manual import (
    build_radartec_profesores_groups,
)


class RadartecTests(unittest.TestCase):
    def test_groups_professors_by_class_assignment(self) -> None:
        profesores = [
            {
                "persona_id": 10,
                "nombre": "Docente Vinculado",
                "login": "docente.activo",
                "login_activo": True,
                "clase_ids_actuales": [101],
                "clases_actuales": ["Matematica 1A"],
            },
            {
                "persona_id": 20,
                "nombre": "Docente Sin Clase",
                "login": "docente.inactivo",
                "login_activo": False,
                "clase_ids_actuales": [],
            },
        ]

        vinculados, no_vinculados, summary = build_radartec_profesores_groups(
            profesores
        )

        self.assertEqual([row["persona_id"] for row in vinculados], [10])
        self.assertEqual([row["persona_id"] for row in no_vinculados], [20])
        self.assertEqual(vinculados[0]["estado_login"], "Activo")
        self.assertEqual(no_vinculados[0]["estado_login"], "Inactivo")
        self.assertEqual(summary["vinculados_total"], 1)
        self.assertEqual(summary["no_vinculados_total"], 1)

    def test_explicit_login_status_overrides_teacher_status(self) -> None:
        profesores = [
            {
                "persona_id": 10,
                "nombre": "Docente",
                "login": "docente",
                "estado": "Activo",
                "login_activo": False,
                "clase_ids_actuales": [101],
            }
        ]

        vinculados, _no_vinculados, summary = (
            build_radartec_profesores_groups(profesores)
        )

        self.assertFalse(vinculados[0]["login_activo"])
        self.assertEqual(summary["vinculados_inactivos"], 1)

    def test_teacher_status_is_used_when_login_status_is_missing(self) -> None:
        profesores = [
            {
                "persona_id": 10,
                "nombre": "Docente",
                "login": "docente",
                "estado": "Activo",
                "clase_ids_actuales": [],
            }
        ]

        _vinculados, no_vinculados, summary = (
            build_radartec_profesores_groups(profesores)
        )

        self.assertTrue(no_vinculados[0]["login_activo"])
        self.assertEqual(summary["no_vinculados_activos"], 1)

    def test_extracts_explicit_persona_login_status(self) -> None:
        self.assertTrue(
            _extract_login_activo(
                {"personaLogin": {"login": "docente", "activo": True}}
            )
        )
        self.assertFalse(
            _extract_login_activo(
                {"personaLogin": {"login": "docente", "estado": "Inactivo"}}
            )
        )


if __name__ == "__main__":
    unittest.main()
