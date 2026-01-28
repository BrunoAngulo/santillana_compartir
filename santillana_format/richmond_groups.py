import re
import unicodedata
from datetime import date
from io import BytesIO
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import requests


RS_BASE_URL = "https://richmondstudio.global/api"
CHANGE_INSTITUTION_URL = f"{RS_BASE_URL}/current_user/change_institution"
GROUPS_URL = f"{RS_BASE_URL}/groups"

REQUIRED_COLUMNS = ("grado", "nivel", "producto", "secciones")
LEVEL_CODES = {"Primaria": "P", "Secundaria": "S"}
NAME_BASE = "Ingl\u00E9s"


def _read_excel(excel_input, filename: str) -> pd.DataFrame:
    if isinstance(excel_input, bytes):
        raw_bytes = excel_input
    elif hasattr(excel_input, "read"):
        raw_bytes = excel_input.read()
    else:
        raise ValueError("Entrada invalida: se esperaba bytes o archivo subido.")

    if not filename.lower().endswith(".xlsx"):
        raise ValueError("El archivo debe ser .xlsx")

    return pd.read_excel(BytesIO(raw_bytes), dtype=str, engine="openpyxl")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {col: str(col).strip().lower() for col in df.columns}
    return df.rename(columns=rename)


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFD", str(value))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^0-9A-Za-z]+", " ", text)
    return " ".join(text.upper().split())


def _parse_level(value: str) -> Optional[str]:
    normalized = _normalize_text(value)
    if "PRIM" in normalized:
        return "Primaria"
    if "SEC" in normalized:
        return "Secundaria"
    return None


def _parse_grade(value: str) -> Optional[int]:
    match = re.search(r"\d+", str(value or ""))
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def _parse_sections(value: str) -> Tuple[List[str], List[str]]:
    if value is None:
        return [], ["Secciones vacias."]
    tokens = re.split(r"[\s,;/]+", str(value).strip())
    letras: List[str] = []
    invalid: List[str] = []
    for token in tokens:
        if not token:
            continue
        upper = token.strip().upper()
        if len(upper) == 1 and upper.isalpha():
            if upper not in letras:
                letras.append(upper)
        else:
            invalid.append(token)
    if not letras and not invalid:
        invalid.append("Secciones vacias.")
    return letras, invalid


def _contains_phrase(normalized: str, compact: str, tokens: Sequence[str], phrase: str) -> bool:
    phrase_norm = _normalize_text(phrase)
    if phrase_norm in tokens:
        return True
    return phrase_norm in normalized or phrase_norm.replace(" ", "") in compact


def _grade_level_primary(grade: int, normalized: str) -> Tuple[Optional[str], Optional[str]]:
    compact = normalized.replace(" ", "")
    tokens = normalized.split()

    if _contains_phrase(normalized, compact, tokens, "GO FOR IT"):
        return "lower_primary", None
    if _contains_phrase(normalized, compact, tokens, "GO FURTHER"):
        return ("lower_primary" if grade <= 4 else "upper_primary"), None
    if _contains_phrase(normalized, compact, tokens, "FLY HIGHER"):
        return "upper_primary", None
    if _contains_phrase(normalized, compact, tokens, "FLY HIGH"):
        return ("lower_primary" if grade <= 4 else "upper_primary"), None
    if "BE" in tokens:
        return "upper_primary", None
    for program in ("EVERYONE", "ORBIT", "COOL KIDS", "NEW FRIENDS"):
        if _contains_phrase(normalized, compact, tokens, program):
            return "lower_primary", None

    if _contains_phrase(normalized, compact, tokens, "LOWER PRIMARY"):
        return "lower_primary", None
    if _contains_phrase(normalized, compact, tokens, "UPPER PRIMARY"):
        return "upper_primary", None

    return None, "No se pudo determinar gradeLevel para primaria."


def _grade_level_secondary(grade: int, normalized: str) -> Tuple[Optional[str], Optional[str]]:
    compact = normalized.replace(" ", "")
    tokens = normalized.split()

    if _contains_phrase(normalized, compact, tokens, "GO FOR IT"):
        if grade <= 4:
            return "lower_secondary", None
        return None, "GO FOR IT solo aplica a secundaria 1-4."
    if _contains_phrase(normalized, compact, tokens, "GO FURTHER"):
        return ("lower_secondary" if grade <= 3 else "upper_secondary"), None
    if _contains_phrase(normalized, compact, tokens, "FLY HIGHER"):
        return "upper_secondary", None
    if _contains_phrase(normalized, compact, tokens, "FLY HIGH"):
        return ("lower_secondary" if grade <= 2 else "upper_secondary"), None
    if _contains_phrase(normalized, compact, tokens, "KEEP IT REAL"):
        return ("lower_secondary" if grade <= 2 else "upper_secondary"), None
    if _contains_phrase(normalized, compact, tokens, "DIRECTIONS"):
        return "upper_secondary", None
    for program in (
        "EVERYONE TEENS",
        "STUDENTS FOR PEACE",
        "NEW FRIENDS",
        "STOPWATCH SPLIT",
    ):
        if _contains_phrase(normalized, compact, tokens, program):
            return "lower_secondary", None

    if _contains_phrase(normalized, compact, tokens, "STOPWATCH"):
        if _contains_phrase(normalized, compact, tokens, "SPLIT"):
            return "lower_secondary", None
        if _contains_phrase(normalized, compact, tokens, "FULL"):
            if grade == 5:
                return "lower_secondary", None
            return None, "Stopwatch full solo aplica a secundaria 5."
        return "lower_secondary", "Stopwatch sin split/full; se asume lower_secondary."

    if _contains_phrase(normalized, compact, tokens, "LOWER SECONDARY"):
        return "lower_secondary", None
    if _contains_phrase(normalized, compact, tokens, "UPPER SECONDARY"):
        return "upper_secondary", None

    return None, "No se pudo determinar gradeLevel para secundaria."


def _determine_grade_level(level: str, grade: int, product: str) -> Tuple[Optional[str], Optional[str]]:
    normalized = _normalize_text(product)
    if level == "Primaria":
        if grade <= 2:
            return None, None
        return _grade_level_primary(grade, normalized)
    if level == "Secundaria":
        return _grade_level_secondary(grade, normalized)
    return None, "Nivel no soportado para gradeLevel."


def _resolve_next_url(next_url: Optional[str]) -> Optional[str]:
    if not next_url:
        return None
    if next_url.startswith("http"):
        return next_url
    return f"{RS_BASE_URL.rstrip('/')}/{next_url.lstrip('/')}"


def _change_institution(
    session: requests.Session, institution_id: str, timeout: int
) -> None:
    response = session.post(
        CHANGE_INSTITUTION_URL,
        json={"institution_id": institution_id},
        timeout=timeout,
    )
    if not response.ok:
        status_code = response.status_code
        try:
            payload = response.json()
            message = payload.get("message") or payload.get("error") or ""
        except ValueError:
            message = ""
        raise RuntimeError(message or f"HTTP {status_code} al cambiar institucion.")


def _fetch_existing_names(
    session: requests.Session, timeout: int
) -> Tuple[List[str], List[str]]:
    names: List[str] = []
    errors: List[str] = []
    url = GROUPS_URL

    while url:
        response = session.get(url, timeout=timeout)
        if not response.ok:
            status_code = response.status_code
            try:
                payload = response.json()
                message = payload.get("message") or payload.get("error") or ""
            except ValueError:
                message = ""
            errors.append(message or f"HTTP {status_code} al listar grupos.")
            break

        try:
            payload = response.json()
        except ValueError:
            errors.append("Respuesta no JSON al listar grupos.")
            break

        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                attrs = item.get("attributes") or {}
                name = attrs.get("name") if isinstance(attrs, dict) else None
                if name:
                    names.append(str(name))

        links = payload.get("links") if isinstance(payload, dict) else None
        next_url = links.get("next") if isinstance(links, dict) else None
        url = _resolve_next_url(next_url)

    return names, errors


def _build_payload(
    name: str,
    grade: str,
    grade_level: Optional[str],
    start_date: date,
    end_date: date,
) -> Dict[str, object]:
    attributes: Dict[str, object] = {
        "name": name,
        "description": name,
        "grade": grade,
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
    }
    if grade_level:
        attributes["gradeLevel"] = grade_level
    return {
        "data": {
            "type": "groups",
            "attributes": attributes,
            "relationships": {"users": {"data": []}},
        }
    }


def process_rs_groups(
    token: str,
    institution_id: str,
    excel_input,
    filename: str,
    start_date: date,
    end_date: date,
    timeout: int = 30,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> Tuple[Dict[str, int], List[Dict[str, object]]]:
    df = _read_excel(excel_input, filename)
    df = _normalize_columns(df).fillna("")

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise KeyError(
            "Faltan columnas requeridas: {missing}. Columnas disponibles: {cols}".format(
                missing=", ".join(missing),
                cols=", ".join(df.columns),
            )
        )

    entries: List[Dict[str, object]] = []
    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        nivel_raw = str(row.get("nivel", "")).strip()
        grado_raw = str(row.get("grado", "")).strip()
        producto_raw = str(row.get("producto", "")).strip()
        secciones_raw = str(row.get("secciones", "")).strip()

        if not (nivel_raw or grado_raw or producto_raw or secciones_raw):
            continue

        nivel = _parse_level(nivel_raw)
        if not nivel:
            entries.append(
                {
                    "status": "Error",
                    "detail": f"Nivel invalido: {nivel_raw}",
                    "row": row_num,
                    "nivel": nivel_raw,
                    "grado": grado_raw,
                    "producto": producto_raw,
                    "seccion": "",
                    "name": "",
                    "grade": "",
                    "grade_level": "",
                }
            )
            continue

        grade_num = _parse_grade(grado_raw)
        if grade_num is None:
            entries.append(
                {
                    "status": "Error",
                    "detail": f"Grado invalido: {grado_raw}",
                    "row": row_num,
                    "nivel": nivel,
                    "grado": grado_raw,
                    "producto": producto_raw,
                    "seccion": "",
                    "name": "",
                    "grade": "",
                    "grade_level": "",
                }
            )
            continue

        if nivel == "Primaria" and not (1 <= grade_num <= 6):
            entries.append(
                {
                    "status": "Error",
                    "detail": f"Grado fuera de rango para primaria: {grade_num}",
                    "row": row_num,
                    "nivel": nivel,
                    "grado": grade_num,
                    "producto": producto_raw,
                    "seccion": "",
                    "name": "",
                    "grade": "",
                    "grade_level": "",
                }
            )
            continue
        if nivel == "Secundaria" and not (1 <= grade_num <= 5):
            entries.append(
                {
                    "status": "Error",
                    "detail": f"Grado fuera de rango para secundaria: {grade_num}",
                    "row": row_num,
                    "nivel": nivel,
                    "grado": grade_num,
                    "producto": producto_raw,
                    "seccion": "",
                    "name": "",
                    "grade": "",
                    "grade_level": "",
                }
            )
            continue

        secciones, invalid = _parse_sections(secciones_raw)
        if invalid:
            entries.append(
                {
                    "status": "Error",
                    "detail": f"Secciones invalidas: {', '.join(invalid)}",
                    "row": row_num,
                    "nivel": nivel,
                    "grado": grade_num,
                    "producto": producto_raw,
                    "seccion": "",
                    "name": "",
                    "grade": "",
                    "grade_level": "",
                }
            )
            continue

        if not producto_raw:
            entries.append(
                {
                    "status": "Error",
                    "detail": "Producto vacio.",
                    "row": row_num,
                    "nivel": nivel,
                    "grado": grade_num,
                    "producto": producto_raw,
                    "seccion": "",
                    "name": "",
                    "grade": "",
                    "grade_level": "",
                }
            )
            continue

        grade_level, warning = _determine_grade_level(nivel, grade_num, producto_raw)
        requiere_grade_level = nivel == "Secundaria" or grade_num >= 3
        if requiere_grade_level and grade_level is None:
            entries.append(
                {
                    "status": "Error",
                    "detail": warning or "No se pudo determinar gradeLevel.",
                    "row": row_num,
                    "nivel": nivel,
                    "grado": grade_num,
                    "producto": producto_raw,
                    "seccion": "",
                    "name": "",
                    "grade": "",
                    "grade_level": "",
                }
            )
            continue

        for seccion in secciones:
            level_code = LEVEL_CODES.get(nivel, "")
            name = f"{NAME_BASE} {grade_num}{level_code}{seccion}"
            entries.append(
                {
                    "status": "",
                    "detail": warning or "",
                    "row": row_num,
                    "nivel": nivel,
                    "grado": grade_num,
                    "producto": producto_raw,
                    "seccion": seccion,
                    "name": name,
                    "grade": f"grade{grade_num}",
                    "grade_level": grade_level or "",
                }
            )

    total = len(entries)
    if total == 0:
        return {"procesados": 0, "creados": 0, "omitidos": 0, "errores": 0}, []

    results: List[Dict[str, object]] = []
    created = 0
    skipped = 0
    errors = 0

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.api+json",
    }

    with requests.Session() as session:
        session.headers.update(headers)

        _change_institution(session=session, institution_id=institution_id, timeout=timeout)

        existing_names_list, existing_errors = _fetch_existing_names(
            session=session, timeout=timeout
        )
        if existing_errors:
            raise RuntimeError(" | ".join(existing_errors))

        existing_names = {
            _normalize_text(name) for name in existing_names_list if name
        }

        for idx, entry in enumerate(entries, start=1):
            name = str(entry.get("name") or "").strip()
            grade = str(entry.get("grade") or "").strip()
            grade_level = str(entry.get("grade_level") or "").strip()

            status = ""
            detail = str(entry.get("detail") or "")
            group_id = ""

            if entry.get("status") == "Error":
                status = "Error"
                errors += 1
            elif not name:
                status = "Error"
                detail = "Nombre vacio."
                errors += 1
            elif not grade:
                status = "Error"
                detail = "Grade vacio."
                errors += 1
            elif not grade_level and (
                entry.get("nivel") == "Secundaria"
                or int(entry.get("grado") or 0) >= 3
            ):
                status = "Error"
                detail = "gradeLevel vacio."
                errors += 1
            elif _normalize_text(name) in existing_names:
                status = "Omitido - Ya existe"
                skipped += 1
            else:
                payload = _build_payload(
                    name=name,
                    grade=grade,
                    grade_level=grade_level or None,
                    start_date=start_date,
                    end_date=end_date,
                )
                try:
                    response = session.post(
                        GROUPS_URL, json=payload, timeout=timeout
                    )
                except requests.RequestException as exc:
                    status = "Error"
                    detail = f"Red: {exc}"
                    errors += 1
                else:
                    if not response.ok:
                        status_code = response.status_code
                        try:
                            payload_resp = response.json()
                            message = ""
                            if isinstance(payload_resp, dict):
                                if payload_resp.get("errors"):
                                    first = payload_resp["errors"][0]
                                    message = first.get("detail") or first.get("title") or ""
                                else:
                                    message = payload_resp.get("message") or payload_resp.get(
                                        "error", ""
                                    )
                        except ValueError:
                            message = ""
                        status = "Error"
                        detail = message or f"HTTP {status_code}"
                        errors += 1
                    else:
                        status = "Creado"
                        created += 1
                        existing_names.add(_normalize_text(name))
                        try:
                            payload_resp = response.json()
                            data = payload_resp.get("data") if isinstance(payload_resp, dict) else None
                            if isinstance(data, dict):
                                group_id = str(data.get("id") or "")
                        except ValueError:
                            group_id = ""

            results.append(
                {
                    "row": entry.get("row"),
                    "nivel": entry.get("nivel"),
                    "grado": entry.get("grado"),
                    "producto": entry.get("producto"),
                    "seccion": entry.get("seccion"),
                    "name": name,
                    "grade": grade,
                    "gradeLevel": grade_level,
                    "status": status,
                    "detail": detail,
                    "id": group_id,
                }
            )

            if on_progress:
                on_progress(idx, total)

    summary = {
        "procesados": total,
        "creados": created,
        "omitidos": skipped,
        "errores": errors,
    }
    return summary, results
