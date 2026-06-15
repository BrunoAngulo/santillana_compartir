from __future__ import annotations

import re
from collections.abc import Mapping, MutableSet, Sequence
from io import BytesIO
from typing import Any

import pandas as pd


CENSO_ALUMNOS_EXPORT_COLUMNS = (
    "personaId",
    "Estado",
    "Nivel",
    "Grado",
    "Grupo",
    "Nombre del alumno",
    "DNI",
    "Login",
    "Password",
)


def normalize_censo_alumnos_export_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        normalized.append(
            {
                "personaId": str(
                    row.get("personaId")
                    or row.get("Persona ID")
                    or row.get("persona_id")
                    or ""
                ).strip(),
                "Estado": _normalize_estado(
                    row.get("Estado")
                    if row.get("Estado") is not None
                    else row.get("activo")
                ),
                "Nivel": str(row.get("Nivel") or row.get("nivel") or "").strip(),
                "Grado": str(row.get("Grado") or row.get("grado") or "").strip(),
                "Grupo": str(
                    row.get("Grupo")
                    or row.get("Seccion")
                    or row.get("seccion")
                    or ""
                ).strip(),
                "Nombre del alumno": str(
                    row.get("Nombre del alumno")
                    or row.get("Nombre completo")
                    or row.get("nombre_completo")
                    or ""
                ).strip(),
                "DNI": str(
                    row.get("DNI")
                    or row.get("dni")
                    or row.get("id_oficial")
                    or ""
                ).strip(),
                "Login": str(row.get("Login") or row.get("login") or "").strip(),
                "Password": str(
                    row.get("Password") or row.get("password") or ""
                ).strip(),
            }
        )
    normalized.sort(
        key=lambda row: (
            str(row.get("Estado") or ""),
            str(row.get("Nivel") or ""),
            str(row.get("Grado") or ""),
            str(row.get("Grupo") or ""),
            str(row.get("Nombre del alumno") or ""),
            str(row.get("personaId") or ""),
        )
    )
    return normalized


def export_censo_alumnos_excel(
    rows: Sequence[Mapping[str, Any]],
) -> bytes:
    normalized_rows = normalize_censo_alumnos_export_rows(rows)
    frame = pd.DataFrame(normalized_rows)
    frame = frame.reindex(columns=CENSO_ALUMNOS_EXPORT_COLUMNS)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="alumnos")
    output.seek(0)
    return output.getvalue()


def build_censo_colegio_filename(
    colegio_row: Mapping[str, Any] | None,
    colegio_id: int,
    *,
    used_names: MutableSet[str] | None = None,
) -> str:
    row = colegio_row or {}
    crm_id = str(row.get("crm_id") or row.get("crmId") or "").strip()
    colegio = str(row.get("colegio") or row.get("label") or "").strip()
    if not crm_id:
        crm_id = str(int(colegio_id))
    if not colegio:
        colegio = f"Colegio {int(colegio_id)}"

    base_name = sanitize_archive_component(
        f"{crm_id} - {colegio}",
        fallback=f"{crm_id} - Colegio {int(colegio_id)}",
    )
    candidate = f"{base_name}.xlsx"
    if used_names is None:
        return candidate

    normalized_candidate = candidate.casefold()
    if normalized_candidate not in used_names:
        used_names.add(normalized_candidate)
        return candidate

    candidate = f"{base_name} - {int(colegio_id)}.xlsx"
    normalized_candidate = candidate.casefold()
    suffix = 2
    while normalized_candidate in used_names:
        candidate = f"{base_name} - {int(colegio_id)} - {suffix}.xlsx"
        normalized_candidate = candidate.casefold()
        suffix += 1
    used_names.add(normalized_candidate)
    return candidate


def build_flat_censo_zip_path(root_folder: str, file_name: str) -> str:
    root = sanitize_archive_component(root_folder, fallback="censo_colegios")
    name = sanitize_archive_component(file_name, fallback="colegio.xlsx")
    if not name.lower().endswith(".xlsx"):
        name = f"{name}.xlsx"
    return f"{root}/{name}"


def sanitize_archive_component(text: object, *, fallback: str) -> str:
    raw = str(text or "").strip() or str(fallback or "").strip()
    raw = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip(" .")
    return raw or str(fallback or "archivo").strip() or "archivo"


def _normalize_estado(value: object) -> str:
    if isinstance(value, bool):
        return "Activo" if value else "Inactivo"
    if isinstance(value, (int, float)):
        return "Activo" if int(value) != 0 else "Inactivo"
    text = str(value or "").strip()
    normalized = text.casefold()
    if normalized in {"activo", "activa", "true", "1", "si", "sí", "yes"}:
        return "Activo"
    if normalized in {
        "inactivo",
        "inactiva",
        "false",
        "0",
        "no",
        "disabled",
    }:
        return "Inactivo"
    return text
