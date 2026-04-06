import re
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd
import requests

DEFAULT_EMPRESA_ID = 11
DEFAULT_CICLO_ID = 207

UPDATE_LOGIN_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/profesores/{persona_id}/updateLoginProfesor"
)

LEVEL_GENERAL_COLUMNS = ["Inicial", "Primaria", "Secundaria"]
LEVEL_LETTERS = {"Inicial": "I", "Primaria": "P", "Secundaria": "S"}
LEVEL_ID_BY_LETTER = {"I": 38, "P": 39, "S": 40}

GRADE_COLUMNS = {
    "I3": ("I", 3),
    "I4": ("I", 4),
    "I5": ("I", 5),
    "P1": ("P", 1),
    "P2": ("P", 2),
    "P3": ("P", 3),
    "P4": ("P", 4),
    "P5": ("P", 5),
    "P6": ("P", 6),
    "S1": ("S", 1),
    "S2": ("S", 2),
    "S3": ("S", 3),
    "S4": ("S", 4),
    "S5": ("S", 5),
}

TRUTHY_VALUES = {"SI", "S", "1", "X", "TRUE", "VERDADERO", "YES"}

HEADER_ALIASES = {
    "login": "Login",
    "usuario": "Login",
    "password": "Password",
    "contrasena": "Password",
    "contraseña": "Password",
    "persona id": "Persona ID",
    "personaid": "Persona ID",
    "persona_id": "Persona ID",
    "id persona": "Persona ID",
    "id": "Persona ID",
    "inicial": "Inicial",
    "primaria": "Primaria",
    "secundaria": "Secundaria",
}


def actualizar_passwords_docentes(
    token: str,
    colegio_id: int,
    excel_path: Path,
    sheet_name: Optional[str] = None,
    empresa_id: int = DEFAULT_EMPRESA_ID,
    ciclo_id: int = DEFAULT_CICLO_ID,
    timeout: int = 30,
    dry_run: bool = True,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
) -> Tuple[Dict[str, int], List[str], List[Dict[str, object]]]:
    df = _read_passwords_file(excel_path, sheet_name=sheet_name)
    grade_cols_present = [col for col in GRADE_COLUMNS if col in df.columns]
    level_cols_present = [col for col in LEVEL_GENERAL_COLUMNS if col in df.columns]

    warnings: List[str] = []
    errors: List[Dict[str, object]] = []

    records: Dict[int, Dict[str, object]] = {}
    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        persona_id = _parse_persona_id(row.get("Persona ID"))
        login = str(row.get("Login", "") or "").strip()
        password = str(row.get("Password", "") or "").strip()
        if not persona_id or not login or not password:
            warnings.append(f"Fila {row_num}: falta Persona ID, Login o Password.")
            continue
        level_letters = _extract_level_letters(row, grade_cols_present, level_cols_present)
        if not level_letters:
            warnings.append(f"Fila {row_num}: sin niveles/grados marcados.")
            continue

        entry = records.get(persona_id)
        if entry is None:
            records[persona_id] = {
                "login": login,
                "password": password,
                "levels": set(level_letters),
            }
        else:
            if entry["login"] != login or entry["password"] != password:
                warnings.append(
                    f"persona {persona_id} con Login/Password conflictivo; se usa el primero."
                )
            entry["levels"].update(level_letters)

    total_ops = sum(len(entry["levels"]) for entry in records.values())
    summary = {
        "docentes_total": len(records),
        "niveles_total": total_ops,
        "actualizaciones": 0,
        "errores_api": 0,
    }

    if dry_run:
        return summary, warnings, errors

    with requests.Session() as session:
        current = 0
        for persona_id, entry in records.items():
            login = entry["login"]
            password = entry["password"]
            for level_letter in sorted(entry["levels"]):
                nivel_id = LEVEL_ID_BY_LETTER.get(level_letter)
                if not nivel_id:
                    continue
                current += 1
                if on_progress:
                    on_progress(
                        current,
                        total_ops,
                        f"Actualizando persona {persona_id} nivel {nivel_id}",
                    )
                ok, err = _update_login_password(
                    session=session,
                    token=token,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    colegio_id=int(colegio_id),
                    nivel_id=int(nivel_id),
                    persona_id=int(persona_id),
                    login=login,
                    password=password,
                    timeout=timeout,
                )
                if not ok:
                    errors.append(
                        {
                            "tipo": "update_login",
                            "persona_id": persona_id,
                            "nivel_id": nivel_id,
                            "error": err,
                        }
                    )
                    summary["errores_api"] += 1
                    continue
                summary["actualizaciones"] += 1

    return summary, warnings, errors


def _read_passwords_file(
    excel_path: Path,
    sheet_name: Optional[str] = None,
) -> pd.DataFrame:
    ext = excel_path.suffix.lower()
    if ext in {".csv", ".txt"}:
        df = pd.read_csv(excel_path, dtype=str, sep=None, engine="python")
    else:
        if sheet_name:
            with pd.ExcelFile(excel_path, engine="openpyxl") as excel:
                resolved = _resolve_sheet_name(excel.sheet_names, sheet_name)
                df = pd.read_excel(excel, sheet_name=resolved, dtype=str)
        else:
            df = pd.read_excel(excel_path, dtype=str, engine="openpyxl")
    return _canonicalize_columns(df.fillna(""))


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping: Dict[str, str] = {}
    used = set()
    for col in df.columns:
        key = _normalize_header(col)
        canonical = HEADER_ALIASES.get(key)
        if canonical is None:
            if re.fullmatch(r"[ips][0-9]", key):
                canonical = key.upper()
        if canonical and canonical not in used:
            mapping[col] = canonical
            used.add(canonical)
    return df.rename(columns=mapping)


def _normalize_header(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text)
    return text.strip().lower()


def _parse_persona_id(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return int(value)
    text = re.sub(r"\D", "", str(value))
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _extract_level_letters(
    row: pd.Series,
    grade_cols: Sequence[str],
    level_cols: Sequence[str],
) -> Set[str]:
    letters: Set[str] = set()
    for col in grade_cols:
        if _is_truthy(row.get(col, "")):
            level_letter = GRADE_COLUMNS[col][0]
            letters.add(level_letter)
    if letters:
        return letters
    for col in level_cols:
        if _is_truthy(row.get(col, "")):
            letters.add(LEVEL_LETTERS[col])
    return letters


def _is_truthy(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return False
        return value != 0
    text = _normalize_value(value)
    return text in TRUTHY_VALUES


def _normalize_value(value: object) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.strip().upper()


def _update_login_password(
    session: requests.Session,
    token: str,
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    nivel_id: int,
    persona_id: int,
    login: str,
    password: str,
    timeout: int,
) -> Tuple[bool, Optional[str]]:
    url = UPDATE_LOGIN_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
        nivel_id=nivel_id,
        persona_id=persona_id,
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"login": login, "password": password}
    try:
        response = session.put(url, headers=headers, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        data = response.json() if response.content else {}
    except ValueError:
        return False, f"Respuesta no JSON (status {status_code})"

    if not response.ok:
        message = data.get("message") if isinstance(data, dict) else ""
        return False, message or f"HTTP {status_code}"

    if isinstance(data, dict) and data.get("success") is False:
        message = data.get("message") or "Respuesta invalida"
        return False, message
    return True, None


def _resolve_sheet_name(available: List[str], desired: str) -> str:
    if desired in available:
        return desired
    desired_lower = desired.lower()
    for sheet in available:
        if sheet.lower() == desired_lower:
            return sheet
    desired_norm = _normalize_header(desired)
    for sheet in available:
        if _normalize_header(sheet) == desired_norm:
            return sheet
    available_list = ", ".join(available) if available else "(sin hojas)"
    raise ValueError(
        f"No se encontro la hoja '{desired}'. Hojas disponibles: {available_list}."
    )
