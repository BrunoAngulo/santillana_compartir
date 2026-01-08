import re
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

BASE_SHEET_NAME = "Plantilla edici\u00f3n masiva"
NUEVO_SHEET_NAME = "Plantilla alta de alumnos"

HEADER_ALIASES = {
    "nivel": "nivel",
    "grado": "grado",
    "grupo": "grupo",
    "seccion": "grupo",
    "nui": "nui",
    "id alumno": "id_alumno",
    "idalumno": "id_alumno",
    "activo": "activo",
    "nombre": "nombre",
    "apellido paterno": "apellido_paterno",
    "apellido materno": "apellido_materno",
    "sexo": "sexo",
    "fecha de nacimiento": "fecha_nacimiento",
    "fecha nacimiento": "fecha_nacimiento",
    "extranjero": "extranjero",
    "nuip": "nuip",
    "login": "login",
    "password": "password",
    "nuevo nivel": "nuevo_nivel",
    "nuevo grado": "nuevo_grado",
    "nuevo grupo": "nuevo_grupo",
}

NAME_COLUMNS_REQUIRED = {"nombre", "apellido_paterno", "apellido_materno"}
KEY_COLUMNS_REQUIRED = NAME_COLUMNS_REQUIRED | {"fecha_nacimiento", "grado", "grupo"}
IMPORTANT_COMPARE_COLUMNS = {"nombre", "apellido paterno", "apellido materno"}
IMPORTANT_GRADE_COLUMNS = {
    "nombre",
    "apellido paterno",
    "apellido materno",
    "grado",
    "grupo",
    "seccion",
}


def read_alumnos_file(
    file_bytes: bytes,
    filename: str,
    sheet_name: str | None = None,
) -> pd.DataFrame:
    ext = Path(filename).suffix.lower()
    if ext in {".csv", ".txt"}:
        df = pd.read_csv(BytesIO(file_bytes), dtype=str, sep=None, engine="python")
    else:
        if sheet_name:
            with pd.ExcelFile(BytesIO(file_bytes), engine="openpyxl") as excel:
                resolved = _resolve_sheet_name(excel.sheet_names, sheet_name)
                df = pd.read_excel(excel, sheet_name=resolved, dtype=str)
        else:
            df = pd.read_excel(BytesIO(file_bytes), dtype=str, engine="openpyxl")
    return df.fillna("")


def export_alumnos_excel(df: pd.DataFrame, sheet_name: str = "Alumnos") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()


def compare_alumnos(
    df_base: pd.DataFrame, df_nuevo: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    base_norm = _canonicalize_columns(df_base)
    nuevo_norm = _canonicalize_columns(df_nuevo)

    _validate_key_columns(base_norm, "base")
    _validate_key_columns(nuevo_norm, "nuevo")

    base_keys, base_missing = _build_key_index(base_norm)
    nuevo_keys, nuevo_missing = _build_keys_per_row(nuevo_norm)
    if not base_keys:
        raise ValueError("No se pudieron leer claves del archivo base.")

    duplicado_mask = [
        bool(keys & base_keys) if keys else False for keys in nuevo_keys
    ]
    mask_series = pd.Series(duplicado_mask, index=df_nuevo.index)
    repetidos = df_nuevo.loc[mask_series].copy()
    filtrados = df_nuevo.loc[~mask_series].copy()

    summary = {
        "base_total": int(len(df_base)),
        "nuevo_total": int(len(df_nuevo)),
        "repetidos": int(mask_series.sum()),
        "sin_repetir": int((~mask_series).sum()),
        "base_sin_clave": int(base_missing),
        "nuevo_sin_clave": int(nuevo_missing),
    }
    return repetidos, filtrados, summary


def find_coincidencias_nombre_apellidos(
    df_base: pd.DataFrame, df_nuevo: pd.DataFrame
) -> pd.DataFrame:
    base_norm = _canonicalize_columns(df_base)
    nuevo_norm = _canonicalize_columns(df_nuevo)
    _validate_name_columns(base_norm, "base")
    _validate_name_columns(nuevo_norm, "nuevo")

    base_keys: Set[str] = set()
    for record in base_norm.to_dict("records"):
        base_keys.update(_name_prefix_keys(record))

    matches: List[bool] = []
    for record in nuevo_norm.to_dict("records"):
        row_keys = _name_prefix_keys(record)
        matches.append(bool(row_keys & base_keys) if row_keys else False)
    mask = pd.Series(matches, index=df_nuevo.index)
    return df_nuevo.loc[mask].copy()


def build_comparacion_clave(df_base: pd.DataFrame, df_nuevo: pd.DataFrame) -> pd.DataFrame:
    base_norm = _canonicalize_columns(df_base)
    nuevo_norm = _canonicalize_columns(df_nuevo)
    _validate_key_columns(base_norm, "base")
    _validate_key_columns(nuevo_norm, "nuevo")
    pairs = _pair_indices(base_norm, nuevo_norm, _row_keys)
    return _build_comparacion_frame(df_base, df_nuevo, pairs)


def build_comparacion_nombre(df_base: pd.DataFrame, df_nuevo: pd.DataFrame) -> pd.DataFrame:
    base_norm = _canonicalize_columns(df_base)
    nuevo_norm = _canonicalize_columns(df_nuevo)
    _validate_name_columns(base_norm, "base")
    _validate_name_columns(nuevo_norm, "nuevo")
    pairs = _pair_indices(base_norm, nuevo_norm, _name_prefix_keys)
    return _build_comparacion_frame(df_base, df_nuevo, pairs)


def build_comparacion_grado_seccion_diferente(
    df_base: pd.DataFrame, df_nuevo: pd.DataFrame
) -> pd.DataFrame:
    base_norm = _canonicalize_columns(df_base)
    nuevo_norm = _canonicalize_columns(df_nuevo)
    _validate_key_columns(base_norm, "base")
    _validate_key_columns(nuevo_norm, "nuevo")

    pairs = _pair_indices(base_norm, nuevo_norm, _identity_keys)
    filtered: List[Tuple[int, int]] = []
    for base_idx, nuevo_idx in pairs:
        base_row = base_norm.loc[base_idx]
        nuevo_row = nuevo_norm.loc[nuevo_idx]
        base_grado = _normalize_text(base_row.get("grado", ""))
        nuevo_grado = _normalize_text(nuevo_row.get("grado", ""))
        base_grupo = _normalize_text(base_row.get("grupo", ""))
        nuevo_grupo = _normalize_text(nuevo_row.get("grupo", ""))
        if base_grado != nuevo_grado or base_grupo != nuevo_grupo:
            filtered.append((base_idx, nuevo_idx))

    return _build_comparacion_frame(df_base, df_nuevo, filtered)


def select_comparacion_basica(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    base_cols: List[str] = []
    nuevo_cols: List[str] = []
    for col in df.columns:
        if col.startswith("base_"):
            name = col[5:]
            if _normalize_header(name) in IMPORTANT_COMPARE_COLUMNS:
                base_cols.append(col)
        elif col.startswith("nuevo_"):
            name = col[6:]
            if _normalize_header(name) in IMPORTANT_COMPARE_COLUMNS:
                nuevo_cols.append(col)
    selected = base_cols + nuevo_cols
    if not selected:
        return df
    return df.loc[:, selected].copy()


def select_comparacion_con_grado(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    base_cols: List[str] = []
    nuevo_cols: List[str] = []
    for col in df.columns:
        if col.startswith("base_"):
            name = col[5:]
            if _normalize_header(name) in IMPORTANT_GRADE_COLUMNS:
                base_cols.append(col)
        elif col.startswith("nuevo_"):
            name = col[6:]
            if _normalize_header(name) in IMPORTANT_GRADE_COLUMNS:
                nuevo_cols.append(col)
    selected = base_cols + nuevo_cols
    if not selected:
        return df
    return df.loc[:, selected].copy()


def _normalize_header(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _normalize_sheet_name(value: str) -> str:
    text = unicodedata.normalize("NFD", value)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _resolve_sheet_name(available: List[str], desired: str) -> str:
    if desired in available:
        return desired
    desired_lower = desired.lower()
    for sheet in available:
        if sheet.lower() == desired_lower:
            return sheet
    desired_norm = _normalize_sheet_name(desired)
    for sheet in available:
        if _normalize_sheet_name(sheet) == desired_norm:
            return sheet
    available_list = ", ".join(available) if available else "(sin hojas)"
    raise ValueError(
        f"No se encontro la hoja '{desired}'. Hojas disponibles: {available_list}."
    )


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping: Dict[str, str] = {}
    used: Set[str] = set()
    for col in df.columns:
        key = _normalize_header(col)
        canonical = HEADER_ALIASES.get(key)
        if canonical and canonical not in used:
            mapping[col] = canonical
            used.add(canonical)
    return df.rename(columns=mapping)


def _validate_key_columns(df: pd.DataFrame, label: str) -> None:
    available = set(df.columns)
    if KEY_COLUMNS_REQUIRED.issubset(available):
        return
    missing = ", ".join(sorted(KEY_COLUMNS_REQUIRED - available))
    raise ValueError(
        f"El archivo {label} no tiene columnas clave. Faltan: {missing}."
    )


def _validate_name_columns(df: pd.DataFrame, label: str) -> None:
    available = set(df.columns)
    if NAME_COLUMNS_REQUIRED.issubset(available):
        return
    missing = ", ".join(sorted(NAME_COLUMNS_REQUIRED - available))
    raise ValueError(
        f"El archivo {label} no tiene columnas de nombre. Faltan: {missing}."
    )


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text.strip().upper()


def _normalize_nombre_tokens(value: object) -> List[str]:
    text = _normalize_text(value)
    if not text:
        return []
    tokens = text.split()
    if len(tokens) > 4:
        tokens = tokens[:4]
    return tokens


def _normalize_nombre(value: object) -> str:
    tokens = _normalize_nombre_tokens(value)
    return " ".join(tokens)


def _normalize_login(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    if not text or text in {"nan", "none", "null"}:
        return ""
    return text


def _normalize_id(value: object) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    return re.sub(r"\D", "", text)


def _normalize_date(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}", text):
        parsed = pd.to_datetime(text, dayfirst=False, errors="coerce")
        if pd.notna(parsed):
            return parsed.date().isoformat()
    parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    if pd.notna(parsed):
        return parsed.date().isoformat()
    if re.fullmatch(r"\d+(\.\d+)?", text):
        num = pd.to_numeric(text, errors="coerce")
        if pd.notna(num):
            parsed = pd.to_datetime(num, unit="D", origin="1899-12-30", errors="coerce")
            if pd.notna(parsed):
                return parsed.date().isoformat()
    return ""


def _row_keys(record: Dict[str, object]) -> Set[str]:
    keys: Set[str] = set()
    ap_p = _normalize_text(record.get("apellido_paterno", ""))
    ap_m = _normalize_text(record.get("apellido_materno", ""))
    nombre = _normalize_nombre(record.get("nombre", ""))
    fecha = _normalize_date(record.get("fecha_nacimiento", ""))
    grado = _normalize_text(record.get("grado", ""))
    grupo = _normalize_text(record.get("grupo", ""))
    if ap_p and ap_m and nombre and fecha and grado and grupo:
        keys.add(f"persona:{ap_p}|{ap_m}|{nombre}|{fecha}|{grado}|{grupo}")
    return keys


def _name_key(record: Dict[str, object]) -> str:
    ap_p = _normalize_text(record.get("apellido_paterno", ""))
    ap_m = _normalize_text(record.get("apellido_materno", ""))
    nombre = _normalize_nombre(record.get("nombre", ""))
    if ap_p and ap_m and nombre:
        return f"{ap_p}|{ap_m}|{nombre}"
    return ""


def _identity_keys(record: Dict[str, object]) -> Set[str]:
    ap_p = _normalize_text(record.get("apellido_paterno", ""))
    ap_m = _normalize_text(record.get("apellido_materno", ""))
    fecha = _normalize_date(record.get("fecha_nacimiento", ""))
    tokens = _normalize_nombre_tokens(record.get("nombre", ""))
    if not ap_p or not ap_m or not fecha or not tokens:
        return set()
    keys: Set[str] = set()
    for idx in range(1, len(tokens) + 1):
        prefix = " ".join(tokens[:idx])
        keys.add(f"{ap_p}|{ap_m}|{prefix}|{fecha}")
    return keys


def _name_prefix_keys(record: Dict[str, object]) -> Set[str]:
    ap_p = _normalize_text(record.get("apellido_paterno", ""))
    ap_m = _normalize_text(record.get("apellido_materno", ""))
    tokens = _normalize_nombre_tokens(record.get("nombre", ""))
    if not ap_p or not ap_m or not tokens:
        return set()
    keys: Set[str] = set()
    for idx in range(1, len(tokens) + 1):
        prefix = " ".join(tokens[:idx])
        keys.add(f"{ap_p}|{ap_m}|{prefix}")
    return keys


def _build_key_index(df: pd.DataFrame) -> Tuple[Set[str], int]:
    keys: Set[str] = set()
    missing = 0
    for record in df.to_dict("records"):
        row_keys = _row_keys(record)
        if row_keys:
            keys.update(row_keys)
        else:
            missing += 1
    return keys, missing


def _build_keys_per_row(df: pd.DataFrame) -> Tuple[List[Set[str]], int]:
    keys_list: List[Set[str]] = []
    missing = 0
    for record in df.to_dict("records"):
        row_keys = _row_keys(record)
        if not row_keys:
            missing += 1
        keys_list.append(row_keys)
    return keys_list, missing


def _pair_indices(
    base_norm: pd.DataFrame,
    nuevo_norm: pd.DataFrame,
    key_builder,
) -> List[Tuple[int, int]]:
    base_map: Dict[str, List[int]] = {}
    for idx, record in zip(base_norm.index, base_norm.to_dict("records")):
        keys = key_builder(record)
        if not keys:
            continue
        for key in keys:
            base_map.setdefault(key, []).append(idx)

    pairs: Set[Tuple[int, int]] = set()
    for idx, record in zip(nuevo_norm.index, nuevo_norm.to_dict("records")):
        keys = key_builder(record)
        if not keys:
            continue
        for key in keys:
            for base_idx in base_map.get(key, []):
                pairs.add((base_idx, idx))

    return sorted(pairs)


def _build_comparacion_frame(
    df_base: pd.DataFrame,
    df_nuevo: pd.DataFrame,
    pairs: List[Tuple[int, int]],
) -> pd.DataFrame:
    if not pairs:
        return pd.DataFrame()
    rows: List[Dict[str, object]] = []
    for base_idx, nuevo_idx in pairs:
        base_row = df_base.loc[base_idx]
        nuevo_row = df_nuevo.loc[nuevo_idx]
        row: Dict[str, object] = {
            "base_index": base_idx,
            "nuevo_index": nuevo_idx,
        }
        for col in df_base.columns:
            row[f"base_{col}"] = base_row[col]
        for col in df_nuevo.columns:
            row[f"nuevo_{col}"] = nuevo_row[col]
        rows.append(row)
    return pd.DataFrame(rows)
