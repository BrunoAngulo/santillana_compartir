import re
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pandas as pd
from openpyxl.utils import get_column_letter

DEFAULT_SHEET_BD = "ProfesoresBD"
DEFAULT_SHEET_ACTUALIZADA = "Plantilla_Actualizada"
PROFESORES_COMPARE_COLUMNS = [
    "Nombre",
    "Apellido Paterno",
    "Apellido Materno",
    "DNI",
    "E-mail",
    "Login",
]
PROFESORES_CREAR_COLUMNS = [
    "Nombre",
    "Apellido Paterno",
    "Apellido Materno",
    "Sexo",
    "DNI",
    "E-mail",
    "Login",
    "Password",
    "Inicial",
    "Primaria",
    "Secundaria",
]

HEADER_ALIASES = {
    "nombre": "nombre",
    "apellido paterno": "apellido_paterno",
    "apellido materno": "apellido_materno",
    "dni": "dni",
    "documento": "dni",
    "e mail": "email",
    "email": "email",
    "e-mail": "email",
    "login": "login",
}


def compare_profesores_bd_excel(
    excel_path: Path,
    sheet_bd: str = DEFAULT_SHEET_BD,
    sheet_actualizada: str = DEFAULT_SHEET_ACTUALIZADA,
) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    df_bd = _canonicalize_columns(_read_sheet(excel_path, sheet_bd))
    df_act = _canonicalize_columns(_read_sheet(excel_path, sheet_actualizada))

    df_bd = _filter_non_empty_rows(df_bd)
    df_act = _filter_non_empty_rows(df_act)
    indexes = _build_reference_indexes(df_bd)

    rows: List[Dict[str, object]] = []
    coincidencias_total = 0
    for _, row in df_act.iterrows():
        colegio_row = _row_to_profesor_record(row)
        if _record_is_empty(colegio_row):
            continue
        ref_row, criterio = _match_reference(colegio_row, df_bd, indexes)
        has_reference = ref_row is not None
        if has_reference:
            coincidencias_total += 1
        rows.append(
            {
                **colegio_row,
                "Profesor Colegio": _format_profesor_label(colegio_row),
                "Profesor referencia de la BD": _format_profesor_label(ref_row)
                if ref_row
                else "",
                "Coincidencia por": criterio,
                "Usar referencia BD": bool(has_reference),
                "_tiene_referencia": bool(has_reference),
            }
        )

    rows.sort(
        key=lambda row: (
            str(row.get("Apellido Paterno") or "").upper(),
            str(row.get("Apellido Materno") or "").upper(),
            str(row.get("Nombre") or "").upper(),
            str(row.get("DNI") or ""),
        )
    )
    summary = {
        "bd_total": len(df_bd.index),
        "actualizada_total": len(df_act.index),
        "coincidencias_total": coincidencias_total,
        "sin_referencia_total": max(len(rows) - coincidencias_total, 0),
    }
    return rows, summary


def export_profesores_crear_excel(rows: List[Dict[str, object]]) -> bytes:
    output = BytesIO()
    df = pd.DataFrame(_exportable_rows(rows), columns=PROFESORES_CREAR_COLUMNS)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="ProfesoresCrear")
        ws = writer.book["ProfesoresCrear"]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        for idx, col in enumerate(df.columns, start=1):
            sample = df[col].astype(str).head(200).tolist() if not df.empty else []
            max_len = max([len(str(col))] + [len(val) for val in sample])
            ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)
    output.seek(0)
    return output.getvalue()


def build_profesores_crear_filename(source_name: str) -> str:
    stem = Path(str(source_name or "profesores")).stem.strip() or "profesores"
    return f"profesores_crear_{stem}.xlsx"


def _read_sheet(excel_path: Path, desired: str) -> pd.DataFrame:
    if not excel_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {excel_path}")
    with pd.ExcelFile(excel_path, engine="openpyxl") as excel:
        resolved = _resolve_sheet_name(excel.sheet_names, desired)
        return pd.read_excel(excel, sheet_name=resolved, dtype=str).fillna("")


def _resolve_sheet_name(available: Sequence[str], desired: str) -> str:
    if desired in available:
        return desired
    desired_lower = str(desired).strip().lower()
    for sheet in available:
        if str(sheet).strip().lower() == desired_lower:
            return sheet
    desired_norm = _normalize_header(desired)
    for sheet in available:
        if _normalize_header(sheet) == desired_norm:
            return sheet
    available_txt = ", ".join(str(item) for item in available) if available else "(sin hojas)"
    raise ValueError(
        f"No se encontro la hoja '{desired}'. Hojas disponibles: {available_txt}."
    )


def _normalize_header(value: object) -> str:
    text = str(value or "")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text)
    return text.strip().lower()


def _normalize_text(value: object) -> str:
    text = str(value or "").strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def _normalize_dni(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping: Dict[str, str] = {}
    used = set()
    for col in df.columns:
        canonical = HEADER_ALIASES.get(_normalize_header(col))
        if canonical and canonical not in used:
            mapping[col] = canonical
            used.add(canonical)
    return df.rename(columns=mapping)


def _filter_non_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = []
    for _, row in df.iterrows():
        record = _row_to_profesor_record(row)
        mask.append(not _record_is_empty(record))
    return df.loc[mask].reset_index(drop=True)


def _row_to_profesor_record(row: pd.Series) -> Dict[str, str]:
    return {
        "Nombre": str(row.get("nombre") or row.get("Nombre") or "").strip(),
        "Apellido Paterno": str(
            row.get("apellido_paterno") or row.get("Apellido Paterno") or ""
        ).strip(),
        "Apellido Materno": str(
            row.get("apellido_materno") or row.get("Apellido Materno") or ""
        ).strip(),
        "DNI": str(row.get("dni") or row.get("DNI") or "").strip(),
        "E-mail": str(row.get("email") or row.get("E-mail") or "").strip(),
        "Login": str(row.get("login") or row.get("Login") or "").strip(),
    }


def _record_is_empty(record: Dict[str, str]) -> bool:
    return not any(str(record.get(col) or "").strip() for col in PROFESORES_COMPARE_COLUMNS)


def _record_name_key(record: Dict[str, str]) -> str:
    parts = [
        _normalize_text(record.get("Nombre")),
        _normalize_text(record.get("Apellido Paterno")),
        _normalize_text(record.get("Apellido Materno")),
    ]
    return "|".join(parts)


def _record_name_compact_key(record: Dict[str, str]) -> str:
    return "".join(
        [
            _normalize_text(record.get("Nombre")),
            _normalize_text(record.get("Apellido Paterno")),
            _normalize_text(record.get("Apellido Materno")),
        ]
    )


def _build_reference_indexes(df_bd: pd.DataFrame) -> Dict[str, Dict[str, List[int]]]:
    by_dni: Dict[str, List[int]] = {}
    by_name: Dict[str, List[int]] = {}
    by_name_compact: Dict[str, List[int]] = {}
    for idx, row in df_bd.iterrows():
        record = _row_to_profesor_record(row)
        dni = _normalize_dni(record.get("DNI"))
        if dni:
            by_dni.setdefault(dni, []).append(int(idx))
        name_key = _record_name_key(record)
        if name_key.replace("|", ""):
            by_name.setdefault(name_key, []).append(int(idx))
        name_compact_key = _record_name_compact_key(record)
        if name_compact_key:
            by_name_compact.setdefault(name_compact_key, []).append(int(idx))
    return {"dni": by_dni, "name": by_name, "name_compact": by_name_compact}


def _match_reference(
    colegio_row: Dict[str, str],
    df_bd: pd.DataFrame,
    indexes: Dict[str, Dict[str, List[int]]],
) -> Tuple[Dict[str, str] | None, str]:
    dni = _normalize_dni(colegio_row.get("DNI"))
    if dni and dni in indexes["dni"]:
        idx = indexes["dni"][dni][0]
        return _row_to_profesor_record(df_bd.iloc[int(idx)]), "DNI"

    name_key = _record_name_key(colegio_row)
    if name_key.replace("|", "") and name_key in indexes["name"]:
        idx = indexes["name"][name_key][0]
        return _row_to_profesor_record(df_bd.iloc[int(idx)]), "Nombre"

    name_compact_key = _record_name_compact_key(colegio_row)
    if name_compact_key and name_compact_key in indexes["name_compact"]:
        idx = indexes["name_compact"][name_compact_key][0]
        return _row_to_profesor_record(df_bd.iloc[int(idx)]), "Nombre"

    return None, ""


def _format_profesor_label(record: Dict[str, str] | None) -> str:
    if not record:
        return ""
    nombre = " ".join(
        part
        for part in (
            str(record.get("Nombre") or "").strip(),
            str(record.get("Apellido Paterno") or "").strip(),
            str(record.get("Apellido Materno") or "").strip(),
        )
        if part
    ).strip()
    dni = str(record.get("DNI") or "").strip()
    if nombre and dni:
        return f"{nombre} | DNI {dni}"
    return nombre or dni


def _infer_sexo(nombre: object) -> str:
    normalized = _normalize_text(nombre)
    if not normalized:
        return ""
    if normalized.endswith("a"):
        return "F"
    return "M"


def _exportable_rows(rows: List[Dict[str, object]]) -> List[Dict[str, str]]:
    export_rows: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        export_rows.append(
            {
                "Nombre": str(row.get("Nombre") or "").strip(),
                "Apellido Paterno": str(row.get("Apellido Paterno") or "").strip(),
                "Apellido Materno": str(row.get("Apellido Materno") or "").strip(),
                "Sexo": _infer_sexo(row.get("Nombre")),
                "DNI": str(row.get("DNI") or "").strip(),
                "E-mail": str(row.get("E-mail") or "").strip(),
                "Login": "",
                "Password": "",
                "Inicial": "",
                "Primaria": "",
                "Secundaria": "",
            }
        )
    return export_rows
