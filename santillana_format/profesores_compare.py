import re
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pandas as pd
from openpyxl.utils import get_column_letter

from santillana_format.profesores import PROFESOR_COLUMNS, export_profesores_excel

DEFAULT_SHEET_BD = "Profesores_BD"
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
PROFESORES_BASE_PREVIEW_COLUMNS = [
    "Id",
    "Nombre",
    "Apellido Paterno",
    "Apellido Materno",
    "Estado",
    "Sexo",
    "DNI",
    "E-mail",
    "Login",
]

HEADER_ALIASES = {
    "id": "id",
    "persona id": "id",
    "personaid": "id",
    "nombre": "nombre",
    "apellido paterno": "apellido_paterno",
    "apellido materno": "apellido_materno",
    "estado": "estado",
    "sexo": "sexo",
    "dni": "dni",
    "documento": "dni",
    "e mail": "email",
    "email": "email",
    "e-mail": "email",
    "login": "login",
    "password": "password",
    "inicial": "inicial",
    "primaria": "primaria",
    "secundaria": "secundaria",
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
        ref_row, ref_base_row, criterio = _match_reference(colegio_row, df_bd, indexes)
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
                "_reference_base_record": ref_base_row if isinstance(ref_base_row, dict) else {},
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


def build_profesores_base_filename(source_name: str) -> str:
    stem = Path(str(source_name or "profesores")).stem.strip() or "profesores"
    return f"profesores_base_{stem}.xlsx"


def export_profesores_base_excel(rows: List[Dict[str, object]]) -> bytes:
    export_rows: List[Dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ref_row = row.get("_reference_base_record")
        if not isinstance(ref_row, dict):
            continue
        export_rows.append(
            {col: str(ref_row.get(col) or "").strip() for col in PROFESOR_COLUMNS}
        )
    export_rows.sort(
        key=lambda row: (
            str(row.get("Apellido Paterno") or "").upper(),
            str(row.get("Apellido Materno") or "").upper(),
            str(row.get("Nombre") or "").upper(),
            str(row.get("DNI") or ""),
        )
    )
    return export_profesores_excel(export_rows, profesores_clases=export_rows)


def _read_sheet(excel_path: Path, desired: str) -> pd.DataFrame:
    if not excel_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {excel_path}")
    with pd.ExcelFile(excel_path, engine="openpyxl") as excel:
        if desired == DEFAULT_SHEET_BD:
            resolved = _resolve_sheet_name_fallback(excel.sheet_names, desired, ["ProfesoresBD"])
        else:
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


def _resolve_sheet_name_fallback(
    available: Sequence[str], desired: str, fallbacks: Sequence[str]
) -> str:
    try:
        return _resolve_sheet_name(available, desired)
    except ValueError:
        for fallback in fallbacks:
            try:
                return _resolve_sheet_name(available, fallback)
            except ValueError:
                continue
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


def _normalize_email(value: object) -> str:
    return str(value or "").strip().casefold()


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


def _row_to_profesor_base_record(row: pd.Series) -> Dict[str, str]:
    values = {
        "Id": str(row.get("id") or row.get("Id") or "").strip(),
        "Nombre": str(row.get("nombre") or row.get("Nombre") or "").strip(),
        "Apellido Paterno": str(
            row.get("apellido_paterno") or row.get("Apellido Paterno") or ""
        ).strip(),
        "Apellido Materno": str(
            row.get("apellido_materno") or row.get("Apellido Materno") or ""
        ).strip(),
        "Estado": str(row.get("estado") or row.get("Estado") or "").strip(),
        "Sexo": str(row.get("sexo") or row.get("Sexo") or "").strip(),
        "DNI": str(row.get("dni") or row.get("DNI") or "").strip(),
        "E-mail": str(row.get("email") or row.get("E-mail") or "").strip(),
        "Login": str(row.get("login") or row.get("Login") or "").strip(),
        "Password": str(row.get("password") or row.get("Password") or "").strip(),
        "Inicial": str(row.get("inicial") or row.get("Inicial") or "").strip(),
        "Primaria": str(row.get("primaria") or row.get("Primaria") or "").strip(),
        "Secundaria": str(row.get("secundaria") or row.get("Secundaria") or "").strip(),
        "I3": str(row.get("I3") or "").strip(),
        "I4": str(row.get("I4") or "").strip(),
        "I5": str(row.get("I5") or "").strip(),
        "P1": str(row.get("P1") or "").strip(),
        "P2": str(row.get("P2") or "").strip(),
        "P3": str(row.get("P3") or "").strip(),
        "P4": str(row.get("P4") or "").strip(),
        "P5": str(row.get("P5") or "").strip(),
        "P6": str(row.get("P6") or "").strip(),
        "S1": str(row.get("S1") or "").strip(),
        "S2": str(row.get("S2") or "").strip(),
        "S3": str(row.get("S3") or "").strip(),
        "S4": str(row.get("S4") or "").strip(),
        "S5": str(row.get("S5") or "").strip(),
        "Clases": str(row.get("Clases") or "").strip(),
        "Secciones": str(row.get("Secciones") or "").strip(),
    }
    return {col: values.get(col, "") for col in PROFESOR_COLUMNS}


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
    by_email: Dict[str, List[int]] = {}
    by_name: Dict[str, List[int]] = {}
    by_name_compact: Dict[str, List[int]] = {}
    for idx, row in df_bd.iterrows():
        record = _row_to_profesor_record(row)
        dni = _normalize_dni(record.get("DNI"))
        if dni:
            by_dni.setdefault(dni, []).append(int(idx))
        email = _normalize_email(record.get("E-mail"))
        if email:
            by_email.setdefault(email, []).append(int(idx))
        name_key = _record_name_key(record)
        if name_key.replace("|", ""):
            by_name.setdefault(name_key, []).append(int(idx))
        name_compact_key = _record_name_compact_key(record)
        if name_compact_key:
            by_name_compact.setdefault(name_compact_key, []).append(int(idx))
    return {
        "dni": by_dni,
        "email": by_email,
        "name": by_name,
        "name_compact": by_name_compact,
    }


def _match_reference(
    colegio_row: Dict[str, str],
    df_bd: pd.DataFrame,
    indexes: Dict[str, Dict[str, List[int]]],
) -> Tuple[Dict[str, str] | None, Dict[str, str] | None, str]:
    dni = _normalize_dni(colegio_row.get("DNI"))
    if dni and dni in indexes["dni"]:
        idx = indexes["dni"][dni][0]
        bd_row = df_bd.iloc[int(idx)]
        return _row_to_profesor_record(bd_row), _row_to_profesor_base_record(bd_row), "DNI"

    email = _normalize_email(colegio_row.get("E-mail"))
    email_candidates = indexes["email"].get(email) or []
    if email and len(email_candidates) == 1:
        idx = email_candidates[0]
        bd_row = df_bd.iloc[int(idx)]
        return _row_to_profesor_record(bd_row), _row_to_profesor_base_record(bd_row), "E-mail"

    name_key = _record_name_key(colegio_row)
    name_candidates = indexes["name"].get(name_key) or []
    if name_key.replace("|", "") and len(name_candidates) == 1:
        idx = name_candidates[0]
        bd_row = df_bd.iloc[int(idx)]
        return _row_to_profesor_record(bd_row), _row_to_profesor_base_record(bd_row), "Nombre"

    name_compact_key = _record_name_compact_key(colegio_row)
    compact_candidates = indexes["name_compact"].get(name_compact_key) or []
    if name_compact_key and len(compact_candidates) == 1:
        idx = compact_candidates[0]
        bd_row = df_bd.iloc[int(idx)]
        return _row_to_profesor_record(bd_row), _row_to_profesor_base_record(bd_row), "Nombre"

    return None, None, ""


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
        sexo = str(row.get("Sexo") or "").strip() or _infer_sexo(row.get("Nombre"))
        export_rows.append(
            {
                "Nombre": str(row.get("Nombre") or "").strip(),
                "Apellido Paterno": str(row.get("Apellido Paterno") or "").strip(),
                "Apellido Materno": str(row.get("Apellido Materno") or "").strip(),
                "Sexo": sexo,
                "DNI": str(row.get("DNI") or "").strip(),
                "E-mail": str(row.get("E-mail") or "").strip(),
                "Login": str(row.get("Login") or "").strip(),
                "Password": "",
                "Inicial": str(row.get("Inicial") or "").strip(),
                "Primaria": str(row.get("Primaria") or "").strip(),
                "Secundaria": str(row.get("Secundaria") or "").strip(),
            }
        )
    return export_rows
