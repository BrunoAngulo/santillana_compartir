import re
import unicodedata
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd
import requests

TRUTHY_VALUES = {"SI", "S", "1", "X", "TRUE", "VERDADERO", "YES"}

LEVEL_GENERAL_COLUMNS = ["Inicial", "Primaria", "Secundaria"]
LEVEL_LETTERS = {"Inicial": "I", "Primaria": "P", "Secundaria": "S"}
ALL_GRADES_BY_LEVEL = {
    "I": {1, 2, 3, 4, 5},
    "P": {1, 2, 3, 4, 5, 6},
    "S": {1, 2, 3, 4, 5},
}

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

CLASS_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/clases"
)
STAFF_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/clases/{clase_id}/staff"
)


def asignar_profesores_clases(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    excel_path: Path,
    sheet_name: Optional[str] = None,
    timeout: int = 30,
    dry_run: bool = True,
    remove_missing: bool = False,
    on_log: Optional[Callable[[str], None]] = None,
) -> Tuple[Dict[str, int], List[str], List[Dict[str, object]]]:
    docentes, warnings, invalidos, excel_rows = _load_docentes(
        excel_path, sheet_name=sheet_name
    )

    clases, ignored = _fetch_clases(
        token=token,
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
        timeout=timeout,
    )
    _log_line(on_log, "Cursos disponibles (id, nombre):")
    for clase in clases:
        _log_line(on_log, f"{clase['id']}\t{clase['name']}")
    if ignored:
        warnings.append(f"Clases ignoradas por sufijo no reconocido: {ignored}.")
    clases_by_id = {clase["id"]: clase for clase in clases}

    if excel_rows:
        _log_line(on_log, "")
        _log_line(on_log, "Profesores del Excel:")
        for item in excel_rows:
            _log_line(
                on_log,
                "fila {fila} personaId={persona_id} nombre='{nombre}' cursos='{curso}' niveles={niveles}".format(
                    **item
                ),
            )

    docente_matches: List[Tuple[Dict[str, object], List[Dict[str, object]]]] = []
    match_groups: Dict[Tuple[str, str, int], Dict[str, object]] = {}
    for docente in docentes:
        matches = _match_clases(docente, clases)
        docente_matches.append((docente, matches))
        if not matches:
            continue
        course_norm = docente.get("curso_norm", "")
        if not course_norm:
            continue
        for clase in matches:
            key = (course_norm, clase["level"], clase["grade"])
            group = match_groups.get(key)
            if group is None:
                group = {
                    "curso": docente["curso"],
                    "level": clase["level"],
                    "grade": clase["grade"],
                    "personas": set(),
                }
                match_groups[key] = group
            group["personas"].add(docente["persona_id"])

    if match_groups:
        _log_line(on_log, "")
        _log_line(on_log, "Match por curso/grado (sin seccion):")
        for group in match_groups.values():
            nivel_grado = f"{group['level']}{group['grade']}"
            personas = ", ".join(
                str(pid) for pid in sorted(group["personas"])
            )
            _log_line(on_log, f"{group['curso']} {nivel_grado} => [{personas}]")

    summary = {
        "docentes_procesados": 0,
        "docentes_invalidos": invalidos,
        "docentes_sin_match": 0,
        "clases_encontradas": 0,
        "asignaciones_nuevas": 0,
        "asignaciones_omitidas": 0,
        "eliminaciones": 0,
        "errores_api": 0,
    }
    errors: List[Dict[str, object]] = []

    staff_cache: Dict[int, Set[int]] = {}
    planned_by_class: Dict[int, Set[int]] = {}
    desired_by_class: Dict[int, Set[int]] = {}

    show_details = not dry_run

    for docente, matches in docente_matches:
        summary["docentes_procesados"] += 1
        if show_details:
            _log_line(on_log, "")
            _log_line(
                on_log,
                "Docente personaId={persona_id} curso='{curso}' niveles={niveles} (fila {fila})".format(
                    persona_id=docente["persona_id"],
                    curso=docente["curso"],
                    niveles=docente["nivel_desc"],
                    fila=docente["row"],
                ),
            )

        if not docente["desired_by_level"]:
            summary["docentes_sin_match"] += 1
            if show_details:
                _log_line(on_log, "  - Sin niveles/grados marcados. Se omite.")
            continue

        summary["clases_encontradas"] += len(matches)
        if not matches:
            summary["docentes_sin_match"] += 1
            if show_details:
                _log_line(on_log, "  - Sin clases que coincidan.")
            continue

        for clase in matches:
            desired_by_class.setdefault(clase["id"], set()).add(docente["persona_id"])

        for clase in matches:
            clase_id = clase["id"]
            clase_name = clase["name"]
            match_info = (
                f"{clase_id}\t{clase_name} "
                f"(nivel={clase['level']} grado={clase['grade']} seccion={clase['section']})"
            )
            staff = staff_cache.get(clase_id)
            if staff is None:
                staff, err = _fetch_staff(
                    token=token,
                    empresa_id=empresa_id,
                    ciclo_id=ciclo_id,
                    clase_id=clase_id,
                    timeout=timeout,
                )
                if err:
                    errors.append(
                        {
                            "tipo": "listar_staff",
                            "persona_id": docente["persona_id"],
                            "clase_id": clase_id,
                            "clase": clase_name,
                            "error": err,
                        }
                    )
                    summary["errores_api"] += 1
                    _log_line(on_log, f"  - match {match_info} => error staff: {err}")
                    continue
                staff_cache[clase_id] = staff

            planned = planned_by_class.setdefault(clase_id, set())
            if docente["persona_id"] in staff or docente["persona_id"] in planned:
                summary["asignaciones_omitidas"] += 1
                if show_details:
                    _log_line(
                        on_log,
                        f"  - match {match_info} => ya asignado",
                    )
                continue

            if dry_run:
                planned.add(docente["persona_id"])
                summary["asignaciones_nuevas"] += 1
                url = STAFF_URL.format(
                    empresa_id=empresa_id,
                    ciclo_id=ciclo_id,
                    clase_id=clase_id,
                )
                if show_details:
                    _log_line(
                        on_log,
                        "  - match {info} => POST {url} {{rolClave:'PROF', personaId:{persona_id}}} (dry-run)".format(
                            info=match_info,
                            url=url,
                            persona_id=docente["persona_id"],
                        ),
                    )
                continue

            ok, err = _assign_profesor(
                token=token,
                empresa_id=empresa_id,
                ciclo_id=ciclo_id,
                clase_id=clase_id,
                persona_id=docente["persona_id"],
                timeout=timeout,
            )
            if not ok:
                errors.append(
                    {
                        "tipo": "asignar_profesor",
                        "persona_id": docente["persona_id"],
                        "clase_id": clase_id,
                        "clase": clase_name,
                        "error": err,
                    }
                )
                summary["errores_api"] += 1
                _log_line(on_log, f"  - match {match_info} => error: {err}")
                continue

            staff.add(docente["persona_id"])
            summary["asignaciones_nuevas"] += 1
            if show_details:
                _log_line(on_log, f"  - match {match_info} => asignado")

    if remove_missing and desired_by_class:
        _log_line(on_log, "")
        _log_line(on_log, "Eliminaciones (profesores fuera del Excel):")
        for clase_id, desired_ids in desired_by_class.items():
            staff = staff_cache.get(clase_id)
            if staff is None:
                staff, err = _fetch_staff(
                    token=token,
                    empresa_id=empresa_id,
                    ciclo_id=ciclo_id,
                    clase_id=clase_id,
                    timeout=timeout,
                )
                if err:
                    clase_name = clases_by_id.get(clase_id, {}).get("name", "")
                    errors.append(
                        {
                            "tipo": "listar_staff",
                            "persona_id": "",
                            "clase_id": clase_id,
                            "clase": clase_name,
                            "error": err,
                        }
                    )
                    summary["errores_api"] += 1
                    _log_line(on_log, f"  - {clase_id}\t{clase_name} => error staff: {err}")
                    continue
                staff_cache[clase_id] = staff

            to_remove = sorted(persona for persona in staff if persona not in desired_ids)
            if not to_remove:
                continue
            clase_name = clases_by_id.get(clase_id, {}).get("name", "")
            for persona_id in to_remove:
                url = f"{STAFF_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id, clase_id=clase_id)}/{persona_id}"
                if dry_run:
                    summary["eliminaciones"] += 1
                    _log_line(
                        on_log,
                        f"  - DELETE {url} (dry-run)",
                    )
                    continue
                ok, err = _delete_profesor(
                    token=token,
                    empresa_id=empresa_id,
                    ciclo_id=ciclo_id,
                    clase_id=clase_id,
                    persona_id=persona_id,
                    timeout=timeout,
                )
                if not ok:
                    errors.append(
                        {
                            "tipo": "eliminar_profesor",
                            "persona_id": persona_id,
                            "clase_id": clase_id,
                            "clase": clase_name,
                            "error": err,
                        }
                    )
                    summary["errores_api"] += 1
                    _log_line(on_log, f"  - {clase_id}\t{clase_name} => error delete: {err}")
                    continue
                summary["eliminaciones"] += 1
                _log_line(on_log, f"  - {clase_id}\t{clase_name} => eliminado {persona_id}")

    if summary["docentes_procesados"] == 0:
        warnings.append("No se encontraron docentes validos en el Excel.")

    return summary, warnings, errors


def _load_docentes(
    excel_path: Path,
    sheet_name: Optional[str] = None,
) -> Tuple[List[Dict[str, object]], List[str], int, List[Dict[str, object]]]:
    df = _read_docentes_file(excel_path, sheet_name=sheet_name)
    warnings: List[str] = []
    docentes: List[Dict[str, object]] = []
    invalidos = 0
    grade_cols_present = [col for col in GRADE_COLUMNS if col in df.columns]
    level_cols_present = [col for col in LEVEL_GENERAL_COLUMNS if col in df.columns]
    preview_rows = _build_preview_rows(
        df, grade_cols_present, level_cols_present, limit=None
    )

    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        persona_id = _parse_persona_id(row.get("persona_id"))
        curso_raw = str(row.get("curso", "")).strip()
        cursos = _split_courses(curso_raw)
        if not persona_id or not cursos:
            warnings.append(f"Fila {row_num}: falta personaId o CURSO.")
            invalidos += 1
            continue

        desired_by_level, grade_specific = _extract_desired_levels(
            row, grade_cols_present, level_cols_present
        )
        for curso in cursos:
            docentes.append(
                {
                    "row": row_num,
                    "persona_id": persona_id,
                    "curso": curso,
                    "curso_norm": _normalize_course_text(curso),
                    "desired_by_level": desired_by_level,
                    "grade_specific": grade_specific,
                    "nivel_desc": _format_levels(desired_by_level, grade_specific),
                }
            )

    return docentes, warnings, invalidos, preview_rows


def _read_docentes_file(
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
        if key in {"curso", "asignatura", "materia", "clase", "clases", "class"}:
            canonical = "curso"
        elif key in {"personaid", "persona_id", "idpersona", "id"}:
            canonical = "persona_id"
        elif key == "inicial":
            canonical = "Inicial"
        elif key == "primaria":
            canonical = "Primaria"
        elif key == "secundaria":
            canonical = "Secundaria"
        elif re.fullmatch(r"[ips][0-9]", key):
            canonical = key.upper()
        else:
            continue
        if canonical not in used:
            mapping[col] = canonical
            used.add(canonical)
    return df.rename(columns=mapping)


def _split_courses(value: str) -> List[str]:
    if not value:
        return []
    parts = re.split(r"[;,]+", value)
    cursos = [item.strip() for item in parts if item.strip()]
    return cursos


def _build_preview_rows(
    df: pd.DataFrame,
    grade_cols: Sequence[str],
    level_cols: Sequence[str],
    limit: Optional[int] = 5,
) -> List[Dict[str, object]]:
    preview: List[Dict[str, object]] = []
    rows = df if limit is None else df.head(limit)
    for idx, row in rows.iterrows():
        row_num = int(idx) + 2
        persona_id = _parse_persona_id(row.get("persona_id"))
        nombre = _compose_nombre(row)
        curso = str(row.get("curso", "")).strip()
        niveles = _preview_levels(row, grade_cols, level_cols)
        preview.append(
            {
                "fila": row_num,
                "persona_id": persona_id or "",
                "nombre": nombre,
                "curso": curso,
                "niveles": niveles,
            }
        )
    return preview


def _compose_nombre(row: pd.Series) -> str:
    partes = [
        str(row.get("Nombre", "")).strip(),
        str(row.get("Apellido Paterno", "")).strip(),
        str(row.get("Apellido Materno", "")).strip(),
    ]
    return " ".join(part for part in partes if part).strip()


def _preview_levels(
    row: pd.Series,
    grade_cols: Sequence[str],
    level_cols: Sequence[str],
) -> str:
    flags: List[str] = []
    for col in grade_cols:
        if _is_truthy(row.get(col, "")):
            flags.append(col)
    if not flags:
        for col in level_cols:
            if _is_truthy(row.get(col, "")):
                flags.append(col)
    return ",".join(flags) if flags else "-"


def _normalize_header(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-zA-Z0-9]+", "", text)
    return text.strip().lower()


def _normalize_course_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().upper()


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


def _extract_desired_levels(
    row: pd.Series,
    grade_cols: Sequence[str],
    level_cols: Sequence[str],
) -> Tuple[Dict[str, Set[int]], bool]:
    desired_by_level: Dict[str, Set[int]] = {}
    grade_specific = False
    for col in grade_cols:
        if _is_truthy(row.get(col, "")):
            level_letter, grade = GRADE_COLUMNS[col]
            desired_by_level.setdefault(level_letter, set()).add(grade)
            grade_specific = True

    if not grade_specific:
        for col in level_cols:
            if _is_truthy(row.get(col, "")):
                level_letter = LEVEL_LETTERS[col]
                desired_by_level[level_letter] = set(ALL_GRADES_BY_LEVEL[level_letter])

    return desired_by_level, grade_specific


def _format_levels(desired_by_level: Dict[str, Set[int]], grade_specific: bool) -> str:
    if not desired_by_level:
        return "sin niveles"
    parts: List[str] = []
    for level in sorted(desired_by_level.keys()):
        grades = desired_by_level[level]
        if not grade_specific:
            parts.append(f"{level}:*")
        else:
            parts.append(f"{level}:{','.join(str(grade) for grade in sorted(grades))}")
    return " ".join(parts)


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


def _fetch_clases(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    timeout: int,
) -> Tuple[List[Dict[str, object]], int]:
    url = CLASS_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.get(
            url, headers=headers, params={"colegioId": colegio_id}, timeout=timeout
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red al listar clases: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        raise RuntimeError(message or "Respuesta invalida")

    data = payload.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")

    clases: List[Dict[str, object]] = []
    ignored = 0
    for item in data:
        if not isinstance(item, dict):
            continue
        clase_id = item.get("geClaseId")
        name = item.get("geClase") or item.get("geClaseClave") or ""
        if not clase_id or not name:
            continue
        parsed = _parse_class_suffix(name)
        if parsed is None:
            ignored += 1
            continue
        grade, level_letter, section = parsed
        clases.append(
            {
                "id": int(clase_id),
                "name": str(name),
                "norm": _normalize_course_text(name),
                "grade": grade,
                "level": level_letter,
                "section": section,
            }
        )
    return clases, ignored


def _parse_class_suffix(name: str) -> Optional[Tuple[int, str, str]]:
    if not name:
        return None
    token = name.strip().split()[-1]
    token = re.sub(r"[^A-Za-z0-9]+", "", token).upper()
    if not token:
        return None
    match = re.match(r"^(\d{1,2})([IPS])([A-Z])$", token)
    if match:
        grade = int(match.group(1))
        level = match.group(2)
        section = match.group(3)
        return grade, level, section
    match = re.match(r"^([IPS])(\d{1,2})([A-Z])$", token)
    if match:
        level = match.group(1)
        grade = int(match.group(2))
        section = match.group(3)
        return grade, level, section
    return None


def _match_clases(docente: Dict[str, object], clases: List[Dict[str, object]]) -> List[Dict[str, object]]:
    course_norm = docente.get("curso_norm", "")
    if not course_norm:
        return []
    desired_by_level: Dict[str, Set[int]] = docente.get("desired_by_level", {})
    grade_specific = bool(docente.get("grade_specific"))
    matches: List[Dict[str, object]] = []
    for clase in clases:
        if not clase["norm"].startswith(course_norm):
            continue
        level = clase["level"]
        grade = clase["grade"]
        if level not in desired_by_level:
            continue
        if grade_specific and grade not in desired_by_level[level]:
            continue
        matches.append(clase)
    return matches


def _fetch_staff(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    clase_id: int,
    timeout: int,
) -> Tuple[Set[int], Optional[str]]:
    url = STAFF_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id, clase_id=clase_id)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.get(
            url, headers=headers, params={"rolClave": "PROF"}, timeout=timeout
        )
    except requests.RequestException as exc:
        return set(), f"Error de red: {exc}"

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError:
        return set(), f"Respuesta no JSON (status {status_code})"

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        return set(), message or f"HTTP {status_code}"

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        return set(), message or "Respuesta invalida"

    data = payload.get("data") if isinstance(payload, dict) else None
    data_list: Optional[List[object]] = None
    if isinstance(data, list):
        data_list = data
    elif isinstance(data, dict):
        for key in (
            "claseStaff",
            "staff",
            "personas",
            "personaRoles",
            "content",
            "items",
            "lista",
            "data",
        ):
            candidate = data.get(key)
            if isinstance(candidate, list):
                data_list = candidate
                break
        if data_list is None and "personaId" in data:
            data_list = [data]
    if data_list is None:
        keys = ", ".join(sorted(data.keys())) if isinstance(data, dict) else ""
        detail = f" (keys: {keys})" if keys else ""
        return set(), f"Campo data no es lista{detail}"

    personas: Set[int] = set()
    for item in data_list:
        if not isinstance(item, dict):
            continue
        persona_id = item.get("personaId")
        if persona_id is None:
            persona = item.get("persona") if isinstance(item.get("persona"), dict) else {}
            persona_id = persona.get("personaId")
        if persona_id is None:
            continue
        try:
            personas.add(int(persona_id))
        except (TypeError, ValueError):
            continue
    return personas, None


def _assign_profesor(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    clase_id: int,
    persona_id: int,
    timeout: int,
) -> Tuple[bool, Optional[str]]:
    url = STAFF_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id, clase_id=clase_id)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"rolClave": "PROF", "personaId": persona_id}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        payload = response.json() if response.content else {}
    except ValueError:
        return False, f"Respuesta no JSON (status {status_code})"

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        return False, message or f"HTTP {status_code}"

    if isinstance(payload, dict) and payload.get("success") is False:
        message = payload.get("message") or "Respuesta invalida"
        return False, message
    return True, None


def _delete_profesor(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    clase_id: int,
    persona_id: int,
    timeout: int,
) -> Tuple[bool, Optional[str]]:
    url = (
        STAFF_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id, clase_id=clase_id)
        + f"/{persona_id}"
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.delete(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    if not response.ok:
        try:
            payload = response.json()
            message = payload.get("message") if isinstance(payload, dict) else ""
        except ValueError:
            message = ""
        return False, message or f"HTTP {status_code}"

    if response.content:
        try:
            payload = response.json()
        except ValueError:
            return True, None
        if isinstance(payload, dict) and payload.get("success") is False:
            message = payload.get("message") or "Respuesta invalida"
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


def _log_line(on_log: Optional[Callable[[str], None]], line: str) -> None:
    if on_log:
        on_log(line)
