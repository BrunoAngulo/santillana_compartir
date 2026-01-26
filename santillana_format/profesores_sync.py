import re
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd
import requests

from .profesores import (
    DEFAULT_CICLO_ID,
    DEFAULT_EMPRESA_ID,
    NIVEL_MAP,
    listar_profesores_data,
)
ASIGNAR_NIVEL_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/profesores/{persona_id}/asignarNivel"
)
ACTIVAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/profesores/{persona_id}/activarInactivar"
)

LEVEL_COLUMNS = ["Inicial", "Primaria", "Secundaria"]
BASE_COLUMNS = [
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
PERSONA_ID_COLUMN = "Persona ID"
KEY_PRIORITY = ["persona_id", "dni", "login", "email", "nombre"]

HEADER_ALIASES = {
    "nombre": "Nombre",
    "apellido paterno": "Apellido Paterno",
    "apellidopaterno": "Apellido Paterno",
    "apellido materno": "Apellido Materno",
    "apellidomaterno": "Apellido Materno",
    "sexo": "Sexo",
    "dni": "DNI",
    "documento": "DNI",
    "doc identidad": "DNI",
    "id oficial": "DNI",
    "idoficial": "DNI",
    "email": "E-mail",
    "e mail": "E-mail",
    "e-mail": "E-mail",
    "correo": "E-mail",
    "correo electronico": "E-mail",
    "login": "Login",
    "usuario": "Login",
    "password": "Password",
    "contrasena": "Password",
    "inicial": "Inicial",
    "primaria": "Primaria",
    "secundaria": "Secundaria",
    "estado": "Estado",
    "persona id": "Persona ID",
    "personaid": "Persona ID",
    "persona_id": "Persona ID",
    "id persona": "Persona ID",
    "id": "Persona ID",
}


def sync_profesores(
    token: str,
    colegio_id: int,
    excel_path: Path,
    sheet_name: Optional[str] = None,
    nivel_ids: Optional[Sequence[int]] = None,
    empresa_id: int = DEFAULT_EMPRESA_ID,
    ciclo_id: int = DEFAULT_CICLO_ID,
    timeout: int = 30,
    dry_run: bool = False,
) -> Tuple[Dict[str, int], List[str], List[Dict[str, object]]]:
    excel_bytes = excel_path.read_bytes()
    df_excel = read_profesores_file(excel_bytes, excel_path.name, sheet_name=sheet_name)
    grupos_excel, resumen_excel, warnings_excel = consolidar_profesores(df_excel)

    profesores_api, resumen_api, errores_api = listar_profesores_data(
        token=token,
        colegio_id=colegio_id,
        nivel_ids=nivel_ids,
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        timeout=timeout,
    )

    api_index, warnings_api = _build_api_index(profesores_api)
    warnings: List[str] = warnings_excel + warnings_api
    warnings.extend(_format_excel_summary(resumen_excel, resumen_api))

    desired_by_persona, missing_excel, match_warnings = _match_excel_to_api(
        grupos_excel, api_index
    )
    warnings.extend(match_warnings)

    summary = {
        "excel_grupos": resumen_excel.get("grupos_total", 0),
        "api_profesores": resumen_api.get("profesores_total", 0),
        "excel_no_en_api": len(missing_excel),
        "activar": 0,
        "inactivar": 0,
        "asignar_nivel": 0,
        "errores_api": len(errores_api),
    }
    errors: List[Dict[str, object]] = list(errores_api)

    all_level_ids = list(nivel_ids) if nivel_ids else list(NIVEL_MAP.values())
    with requests.Session() as session:
        for entry in profesores_api:
            persona_id = entry["persona_id"]
            desired_levels = desired_by_persona.get(persona_id)
            if desired_levels is None:
                summary["inactivar"] += _inactivar_todos(
                    session=session,
                    token=token,
                    entry=entry,
                    empresa_id=empresa_id,
                    ciclo_id=ciclo_id,
                    colegio_id=colegio_id,
                    timeout=timeout,
                    dry_run=dry_run,
                    errors=errors,
                )
                continue

            niveles_actuales = _resolve_niveles_actuales(entry)
            if not desired_levels:
                if niveles_actuales:
                    desired_levels = niveles_actuales
                else:
                    warnings.append(
                        f"persona {persona_id} sin niveles; se omiten cambios."
                    )
                    continue

            if desired_levels:
                summary["asignar_nivel"] += _assign_levels(
                    session=session,
                    token=token,
                    persona_id=persona_id,
                    desired_levels=desired_levels,
                    current_levels=niveles_actuales,
                    empresa_id=empresa_id,
                    ciclo_id=ciclo_id,
                    colegio_id=colegio_id,
                    timeout=timeout,
                    dry_run=dry_run,
                    errors=errors,
                )

            summary["activar"] += _activar_deseados(
                session=session,
                token=token,
                entry=entry,
                desired_levels=desired_levels,
                all_level_ids=all_level_ids,
                empresa_id=empresa_id,
                ciclo_id=ciclo_id,
                colegio_id=colegio_id,
                timeout=timeout,
                dry_run=dry_run,
                errors=errors,
            )
            summary["inactivar"] += _inactivar_no_deseados(
                session=session,
                token=token,
                entry=entry,
                desired_levels=desired_levels,
                all_level_ids=all_level_ids,
                empresa_id=empresa_id,
                ciclo_id=ciclo_id,
                colegio_id=colegio_id,
                timeout=timeout,
                dry_run=dry_run,
                errors=errors,
            )

    if missing_excel:
        warnings.append(
            "Profesores en Excel sin match en API: {0}".format(
                ", ".join(str(item) for item in missing_excel[:10])
            )
        )
        restantes = len(missing_excel) - 10
        if restantes > 0:
            warnings.append(f"... y {restantes} mas.")

    return summary, warnings, errors


def _format_excel_summary(
    resumen_excel: Dict[str, int], resumen_api: Dict[str, int]
) -> List[str]:
    messages: List[str] = []
    if resumen_excel.get("sin_clave"):
        messages.append(
            "Excel sin clave: {0} filas.".format(resumen_excel.get("sin_clave", 0))
        )
    if resumen_excel.get("duplicados"):
        messages.append(
            "Excel duplicados: {0} filas unificadas.".format(
                resumen_excel.get("duplicados", 0)
            )
        )
    if resumen_api.get("niveles_error") or resumen_api.get("detalle_error"):
        messages.append(
            "Errores API listado: {0}, detalle: {1}.".format(
                resumen_api.get("niveles_error", 0),
                resumen_api.get("detalle_error", 0),
            )
        )
    return messages


def _build_api_index(
    profesores_api: List[Dict[str, object]]
) -> Tuple[Dict[str, int], List[str]]:
    key_map: Dict[str, int] = {}
    warnings: List[str] = []
    for entry in profesores_api:
        record = {
            PERSONA_ID_COLUMN: entry.get("persona_id", ""),
            "DNI": entry.get("dni", ""),
            "Login": entry.get("login", ""),
            "E-mail": entry.get("email", ""),
            "Nombre": entry.get("nombre", ""),
            "Apellido Paterno": entry.get("apellido_paterno", ""),
            "Apellido Materno": entry.get("apellido_materno", ""),
        }
        keys_by_type, keys = build_profesor_keys(record)
        entry["keys_by_type"] = keys_by_type
        entry["keys"] = keys
        for key in keys:
            existing = key_map.get(key)
            if existing is not None and existing != entry["persona_id"]:
                warnings.append(f"Clave API duplicada {key} en persona {entry['persona_id']}.")
                continue
            key_map[key] = entry["persona_id"]
    return key_map, warnings


def _match_excel_to_api(
    grupos_excel: List[Dict[str, object]],
    api_index: Dict[str, int],
) -> Tuple[Dict[int, Set[int]], List[str], List[str]]:
    desired_by_persona: Dict[int, Set[int]] = {}
    missing_excel: List[str] = []
    warnings: List[str] = []

    for group in grupos_excel:
        keys_by_type = group.get("keys_by_type", {})
        persona_id = _match_by_priority(keys_by_type, api_index)
        if persona_id is None:
            missing_excel.append(_format_group_label(group))
            continue
        if persona_id in desired_by_persona:
            warnings.append(
                f"Excel contiene multiples grupos para persona {persona_id}."
            )
            desired_by_persona[persona_id].update(_desired_levels(group.get("record", {})))
            continue
        desired_by_persona[persona_id] = _desired_levels(group.get("record", {}))

    return desired_by_persona, missing_excel, warnings


def _match_by_priority(
    keys_by_type: Dict[str, set], api_index: Dict[str, int]
) -> Optional[int]:
    for key_type in KEY_PRIORITY:
        for key in keys_by_type.get(key_type, set()):
            if key in api_index:
                return api_index[key]
    return None


def _desired_levels(record: Dict[str, object]) -> Set[int]:
    desired: Set[int] = set()
    for column in LEVEL_COLUMNS:
        value = record.get(column, "")
        if _value_is_true(value):
            desired.add(NIVEL_MAP[column])
    return desired


def _resolve_niveles_actuales(entry: Dict[str, object]) -> Set[int]:
    niveles = entry.get("niveles_detalle")
    if isinstance(niveles, set) and niveles:
        return niveles
    niveles_activos = entry.get("niveles_detalle_activos")
    if isinstance(niveles_activos, set) and niveles_activos:
        return niveles_activos
    niveles_presentes = entry.get("niveles_presentes")
    if isinstance(niveles_presentes, set) and niveles_presentes:
        return niveles_presentes
    return set()


def _activar_deseados(
    session: requests.Session,
    token: str,
    entry: Dict[str, object],
    desired_levels: Set[int],
    all_level_ids: Sequence[int],
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    timeout: int,
    dry_run: bool,
    errors: List[Dict[str, object]],
) -> int:
    count = 0
    activos = entry.get("niveles_activos") or {}
    for nivel_id in all_level_ids:
        if nivel_id not in desired_levels:
            continue
        if activos.get(nivel_id) is True:
            continue
        if _put_activar(
            session,
            token,
            empresa_id,
            ciclo_id,
            colegio_id,
            nivel_id,
            entry["persona_id"],
            activo=True,
            timeout=timeout,
            dry_run=dry_run,
            errors=errors,
        ):
            count += 1
    return count


def _inactivar_no_deseados(
    session: requests.Session,
    token: str,
    entry: Dict[str, object],
    desired_levels: Set[int],
    all_level_ids: Sequence[int],
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    timeout: int,
    dry_run: bool,
    errors: List[Dict[str, object]],
) -> int:
    count = 0
    activos = entry.get("niveles_activos") or {}
    for nivel_id in all_level_ids:
        if nivel_id in desired_levels:
            continue
        if activos.get(nivel_id) is not True:
            continue
        if _put_activar(
            session,
            token,
            empresa_id,
            ciclo_id,
            colegio_id,
            nivel_id,
            entry["persona_id"],
            activo=False,
            timeout=timeout,
            dry_run=dry_run,
            errors=errors,
        ):
            count += 1
    return count


def _inactivar_todos(
    session: requests.Session,
    token: str,
    entry: Dict[str, object],
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    timeout: int,
    dry_run: bool,
    errors: List[Dict[str, object]],
) -> int:
    count = 0
    activos = entry.get("niveles_activos") or {}
    for nivel_id, activo in activos.items():
        if activo is not True:
            continue
        if _put_activar(
            session,
            token,
            empresa_id,
            ciclo_id,
            colegio_id,
            nivel_id,
            entry["persona_id"],
            activo=False,
            timeout=timeout,
            dry_run=dry_run,
            errors=errors,
        ):
            count += 1
    return count


def _assign_levels(
    session: requests.Session,
    token: str,
    persona_id: int,
    desired_levels: Set[int],
    current_levels: Set[int],
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    timeout: int,
    dry_run: bool,
    errors: List[Dict[str, object]],
) -> int:
    if desired_levels == current_levels:
        return 0
    if dry_run:
        return 1
    url = ASIGNAR_NIVEL_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
        persona_id=persona_id,
    )
    payload = {"niveles": [{"nivelId": int(level)} for level in sorted(desired_levels)]}
    error = _request_json(
        session=session,
        method="PUT",
        url=url,
        token=token,
        payload=payload,
        timeout=timeout,
    )
    if error:
        errors.append(
            {
                "tipo": "asignarNivel",
                "persona_id": persona_id,
                "nivel_id": "",
                "url": url,
                "error": error,
            }
        )
        return 0
    return 1


def _put_activar(
    session: requests.Session,
    token: str,
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    nivel_id: int,
    persona_id: int,
    activo: bool,
    timeout: int,
    dry_run: bool,
    errors: List[Dict[str, object]],
) -> bool:
    if dry_run:
        return True
    url = ACTIVAR_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
        nivel_id=nivel_id,
        persona_id=persona_id,
    )
    payload = {"activo": 1 if activo else 0}
    error = _request_json(
        session=session,
        method="PUT",
        url=url,
        token=token,
        payload=payload,
        timeout=timeout,
    )
    if error:
        errors.append(
            {
                "tipo": "activarInactivar",
                "persona_id": persona_id,
                "nivel_id": nivel_id,
                "url": url,
                "error": error,
            }
        )
        return False
    return True


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    token: str,
    payload: Dict[str, object],
    timeout: int,
) -> Optional[str]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = session.request(
            method, url, headers=headers, json=payload, timeout=timeout
        )
    except requests.RequestException as exc:
        return f"Error de red: {exc}"
    status_code = response.status_code
    try:
        data = response.json() if response.content else {}
    except ValueError:
        return f"Respuesta no JSON (status {status_code})"
    if not response.ok:
        message = data.get("message") if isinstance(data, dict) else ""
        return message or f"HTTP {status_code}"
    if isinstance(data, dict) and data.get("success") is False:
        message = data.get("message") or "Respuesta invalida"
        return message
    return None


def _format_group_label(group: Dict[str, object]) -> str:
    record = group.get("record", {})
    nombre = record.get("Nombre", "")
    ap_p = record.get("Apellido Paterno", "")
    ap_m = record.get("Apellido Materno", "")
    filas = group.get("rows") or []
    fila_text = ",".join(str(fila) for fila in filas)
    return f"{nombre} {ap_p} {ap_m}".strip() + (f" (filas {fila_text})" if fila_text else "")


def _value_is_true(value: object) -> bool:
    text = str(value).strip() if value is not None else ""
    if not text:
        return False
    text = text.lower()
    if text in {"0", "no", "n", "false"}:
        return False
    return True


def read_profesores_file(
    file_bytes: bytes,
    filename: str,
    sheet_name: Optional[str] = None,
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
    return _canonicalize_columns(df.fillna(""))


def consolidar_profesores(
    df: pd.DataFrame,
) -> Tuple[List[Dict[str, object]], Dict[str, int], List[str]]:
    columns = list(BASE_COLUMNS)
    if "Estado" in df.columns:
        columns.append("Estado")
    if PERSONA_ID_COLUMN in df.columns:
        columns.append(PERSONA_ID_COLUMN)
    groups, missing, duplicates, warnings = _group_records(
        df.to_dict("records"),
        columns,
        "excel",
    )
    summary = {
        "filas_total": int(len(df)),
        "grupos_total": int(len(groups)),
        "duplicados": int(duplicates),
        "sin_clave": int(missing),
    }
    return groups, summary, warnings


def build_profesor_keys(record: Dict[str, object]) -> Tuple[Dict[str, set], set]:
    return _build_keys(record)


def _group_records(
    records: List[Dict[str, object]],
    columns: List[str],
    label: str,
) -> Tuple[List[Dict[str, object]], int, int, List[str]]:
    total = len(records)
    parent = list(range(total))
    rank = [0] * total
    key_to_index: Dict[str, int] = {}
    keys_by_record: List[Dict[str, set]] = []
    keys_set_by_record: List[set] = []
    missing = 0

    def find(idx: int) -> int:
        while parent[idx] != idx:
            parent[idx] = parent[parent[idx]]
            idx = parent[idx]
        return idx

    def union(a: int, b: int) -> None:
        root_a = find(a)
        root_b = find(b)
        if root_a == root_b:
            return
        if rank[root_a] < rank[root_b]:
            parent[root_a] = root_b
        elif rank[root_a] > rank[root_b]:
            parent[root_b] = root_a
        else:
            parent[root_b] = root_a
            rank[root_a] += 1

    for idx, record in enumerate(records):
        keys_by_type, keys = _build_keys(record)
        keys_by_record.append(keys_by_type)
        keys_set_by_record.append(keys)
        if not keys:
            missing += 1
            continue
        for key in keys:
            if key in key_to_index:
                union(idx, key_to_index[key])
            else:
                key_to_index[key] = idx

    groups_map: Dict[int, List[int]] = {}
    for idx, keys in enumerate(keys_set_by_record):
        if not keys:
            continue
        root = find(idx)
        groups_map.setdefault(root, []).append(idx)

    groups: List[Dict[str, object]] = []
    warnings: List[str] = []
    duplicates_count = 0
    for indices in groups_map.values():
        if len(indices) > 1:
            duplicates_count += len(indices) - 1
            filas = ", ".join(str(i + 1) for i in indices)
            warnings.append(f"{label} filas {filas} unificadas.")
        group_records = [records[i] for i in indices]
        merged_record, merge_warnings = _merge_records(
            group_records, columns, label, [i + 1 for i in indices]
        )
        warnings.extend(merge_warnings)
        keys_by_type = {}
        keys_union: set = set()
        for idx in indices:
            for key_type, key_set in keys_by_record[idx].items():
                keys_by_type.setdefault(key_type, set()).update(key_set)
            keys_union.update(keys_set_by_record[idx])
        groups.append(
            {
                "record": merged_record,
                "keys_by_type": keys_by_type,
                "keys": keys_union,
                "rows": [i + 1 for i in indices],
            }
        )

    return groups, missing, duplicates_count, warnings


def _merge_records(
    records: List[Dict[str, object]],
    columns: List[str],
    label: str,
    rows: List[int],
) -> Tuple[Dict[str, object], List[str]]:
    merged = {col: "" for col in columns}
    level_flags = {col: False for col in columns if col in LEVEL_COLUMNS}
    warnings: List[str] = []
    filas = ", ".join(str(fila) for fila in rows)
    for record in records:
        for col in columns:
            value = str(record.get(col, "")).strip()
            if col in level_flags:
                if _value_is_true(value):
                    level_flags[col] = True
                continue
            if value and not merged[col]:
                merged[col] = value
            elif value and merged[col]:
                if _normalize_text(value) != _normalize_text(merged[col]):
                    warnings.append(f"{label} conflicto {col} en filas {filas}.")
    for col, enabled in level_flags.items():
        merged[col] = "SI" if enabled else ""
    return merged, warnings


def _build_keys(record: Dict[str, object]) -> Tuple[Dict[str, set], set]:
    keys_by_type: Dict[str, set] = {}
    persona_id = _normalize_numeric(record.get(PERSONA_ID_COLUMN, ""))
    if persona_id:
        keys_by_type["persona_id"] = {f"persona_id:{persona_id}"}
    dni = _normalize_dni(record.get("DNI", ""))
    if dni:
        keys_by_type["dni"] = {f"dni:{dni}"}
    login = _normalize_email(record.get("Login", ""))
    if login:
        keys_by_type.setdefault("login", set()).add(f"login:{login}")
    email = _normalize_email(record.get("E-mail", ""))
    if email:
        keys_by_type.setdefault("email", set()).add(f"email:{email}")
    nombre = _normalize_text(record.get("Nombre", ""))
    ap_p = _normalize_text(record.get("Apellido Paterno", ""))
    ap_m = _normalize_text(record.get("Apellido Materno", ""))
    if nombre and ap_p and ap_m:
        keys_by_type.setdefault("nombre", set()).add(
            f"nombre:{ap_p}|{ap_m}|{nombre}"
        )
    keys_union = set()
    for key_set in keys_by_type.values():
        keys_union.update(key_set)
    return keys_by_type, keys_union


def _normalize_header(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text)
    return text.strip().lower()


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


def _normalize_email(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    if not text or text in {"nan", "none", "null"}:
        return ""
    return text


def _normalize_numeric(value: object) -> str:
    if value is None:
        return ""
    text = _normalize_text(value)
    if not text:
        return ""
    return re.sub(r"\D", "", text)


def _normalize_dni(value: object) -> str:
    if value is None:
        return ""
    text = _normalize_text(value)
    if not text:
        return ""
    return re.sub(r"\D", "", text)


def _canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping: Dict[str, str] = {}
    used = set()
    for col in df.columns:
        key = _normalize_header(col)
        canonical = HEADER_ALIASES.get(key)
        if canonical and canonical not in used:
            mapping[col] = canonical
            used.add(canonical)
    return df.rename(columns=mapping)


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
