from __future__ import annotations

from typing import Callable, Dict, List, Optional, Tuple

import requests


GESTION_ESCOLAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases"
)

GESTION_ESCOLAR_ALUMNOS_CLASE_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases/{clase_id}/alumnos"
)

GESTION_ESCOLAR_CICLO_ID_DEFAULT = 207


def _safe_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_clases_gestion_escolar(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int = 30,
    ordered: bool = False,
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_URL.format(empresa_id=int(empresa_id), ciclo_id=int(ciclo_id))
    params: Dict[str, object] = {"colegioId": int(colegio_id)}
    if ordered:
        params["ordered"] = 1

    try:
        response = requests.get(url, headers=headers, params=params, timeout=int(timeout))
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

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

    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "rows", "data", "clases"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        for value in data.values():
            if isinstance(value, list):
                return value
    raise RuntimeError("Campo data no es lista")


def extract_clase_fields(item: Dict[str, object]) -> Optional[Dict[str, object]]:
    clase_id = _safe_int(item.get("geClaseId"))
    if clase_id is None:
        return None

    clase_codigo = str(item.get("geClaseClave") or "").strip()
    clase_nombre_largo = str(item.get("geClase") or "").strip()
    clase_nombre = clase_codigo or clase_nombre_largo

    cnc = item.get("colegioNivelCiclo") if isinstance(item.get("colegioNivelCiclo"), dict) else {}
    nivel = cnc.get("nivel") if isinstance(cnc.get("nivel"), dict) else {}
    nivel_id = _safe_int(nivel.get("nivelId"))
    nivel_nombre = str(nivel.get("nivel") or "").strip()

    cgg = item.get("colegioGradoGrupo") if isinstance(item.get("colegioGradoGrupo"), dict) else {}
    colegio_grado_grupo_id = _safe_int(cgg.get("colegioGradoGrupoId"))
    grado = cgg.get("grado") if isinstance(cgg.get("grado"), dict) else {}
    grupo = cgg.get("grupo") if isinstance(cgg.get("grupo"), dict) else {}

    grado_id = _safe_int(grado.get("gradoId"))
    grado_nombre = str(grado.get("grado") or "").strip()

    grupo_id = _safe_int(grupo.get("grupoId"))
    grupo_clave = str(grupo.get("grupoClave") or "").strip()
    if not grupo_clave:
        # Fallback: "Grupo A" -> "A"
        grupo_txt = str(grupo.get("grupo") or "").strip()
        grupo_clave = grupo_txt.split()[-1].strip() if grupo_txt else ""

    if nivel_id is None or grado_id is None:
        return None

    return {
        "clase_id": clase_id,
        "clase": clase_nombre,
        "clase_codigo": clase_codigo,
        "clase_nombre": clase_nombre_largo,
        "uuid": str(item.get("uuid") or "").strip(),
        "nivel_id": nivel_id,
        "nivel": nivel_nombre,
        "grado_id": grado_id,
        "grado": grado_nombre,
        "grupo_id": grupo_id,
        "seccion": grupo_clave,
        "colegio_grado_grupo_id": colegio_grado_grupo_id,
        "activo": bool(item.get("activo", False)),
        "baja": bool(item.get("baja", False)),
    }


def fetch_alumnos_clase_gestion_escolar(
    token: str,
    clase_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int = 30,
) -> Dict[str, object]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_ALUMNOS_CLASE_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
    )

    try:
        response = requests.get(url, headers=headers, timeout=int(timeout))
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

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

    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError("Campo data no es objeto")
    return data


def _extract_alumno_payload(item: Dict[str, object]) -> Dict[str, object]:
    nested = item.get("alumno")
    if isinstance(nested, dict):
        return nested
    return item


def _extract_alumno_nombre_completo(
    persona: Dict[str, object],
    source: Dict[str, object],
) -> str:
    full_name = str(persona.get("nombreCompleto") or "").strip()
    if full_name:
        return full_name
    parts = [
        str(persona.get("apellidoPaterno") or "").strip(),
        str(persona.get("apellidoMaterno") or "").strip(),
        str(persona.get("nombre") or "").strip(),
    ]
    full_name = " ".join(part for part in parts if part).strip()
    if full_name:
        return full_name
    return str(source.get("nombreCompleto") or "").strip()


def extract_alumno_clase_fields(
    item: Dict[str, object],
    clase_row: Dict[str, object],
) -> Dict[str, object]:
    source = _extract_alumno_payload(item)
    persona = source.get("persona") if isinstance(source.get("persona"), dict) else {}
    persona_login = (
        persona.get("personaLogin") if isinstance(persona.get("personaLogin"), dict) else {}
    )

    return {
        "clase_id": _safe_int(clase_row.get("clase_id")),
        "clase": str(clase_row.get("clase") or "").strip(),
        "clase_codigo": str(clase_row.get("clase_codigo") or "").strip(),
        "clase_nombre": str(clase_row.get("clase_nombre") or "").strip(),
        "nivel_id": _safe_int(clase_row.get("nivel_id")),
        "nivel": str(clase_row.get("nivel") or "").strip(),
        "grado_id": _safe_int(clase_row.get("grado_id")),
        "grado": str(clase_row.get("grado") or "").strip(),
        "grupo_id": _safe_int(clase_row.get("grupo_id")),
        "seccion": str(clase_row.get("seccion") or "").strip(),
        "alumno_id": _safe_int(source.get("alumnoId")) or _safe_int(item.get("alumnoId")),
        "persona_id": (
            _safe_int(persona.get("personaId"))
            or _safe_int(source.get("personaId"))
            or _safe_int(item.get("personaId"))
        ),
        "nombre": str(persona.get("nombre") or "").strip(),
        "apellido_paterno": str(persona.get("apellidoPaterno") or "").strip(),
        "apellido_materno": str(persona.get("apellidoMaterno") or "").strip(),
        "nombre_completo": _extract_alumno_nombre_completo(persona, source),
        "id_oficial": str(
            persona.get("idOficial") or source.get("idOficial") or item.get("idOficial") or ""
        ).strip(),
        "login": str(
            persona_login.get("login") or source.get("login") or item.get("login") or ""
        ).strip(),
    }


def listar_alumnos_por_clase_gestion_escolar(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int = 30,
    ordered: bool = True,
    include_inactive: bool = False,
    on_log: Optional[Callable[[str], None]] = None,
) -> List[Dict[str, object]]:
    clases_rows, _ = listar_y_mapear_clases(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
        ordered=ordered,
        on_log=on_log,
    )

    rows: List[Dict[str, object]] = []
    total_clases = len(clases_rows)
    for idx_clase, clase_row in enumerate(clases_rows, start=1):
        if not include_inactive and bool(clase_row.get("baja")):
            continue

        clase_id = _safe_int(clase_row.get("clase_id"))
        if clase_id is None:
            continue

        if on_log:
            on_log(
                "Pegasus clases/alumnos {idx}/{total}: {clase}".format(
                    idx=idx_clase,
                    total=max(total_clases, 1),
                    clase=str(clase_row.get("clase") or clase_id),
                )
            )

        clase_data = fetch_alumnos_clase_gestion_escolar(
            token=token,
            clase_id=int(clase_id),
            empresa_id=int(empresa_id),
            ciclo_id=int(ciclo_id),
            timeout=int(timeout),
        )
        clase_alumnos = (
            clase_data.get("claseAlumnos") if isinstance(clase_data.get("claseAlumnos"), list) else []
        )
        for item in clase_alumnos:
            if not isinstance(item, dict):
                continue
            row = extract_alumno_clase_fields(item, clase_row)
            if not row.get("alumno_id") and not row.get("login") and not row.get("id_oficial"):
                continue
            rows.append(row)

    rows.sort(
        key=lambda row: (
            str(row.get("clase_codigo") or row.get("clase") or "").upper(),
            str(row.get("nombre_completo") or "").upper(),
            str(row.get("login") or "").upper(),
            str(row.get("id_oficial") or "").upper(),
        )
    )
    return rows


def mapear_clases_por_grado_seccion(
    clases_rows: List[Dict[str, object]],
) -> Dict[Tuple[int, int, int], Dict[str, object]]:
    """
    Agrupa por (nivel_id, grado_id, grupo_id) y guarda:
    - metadatos de nivel/grado/seccion
    - lista de clases (id, nombre)
    """
    grouped: Dict[Tuple[int, int, int], Dict[str, object]] = {}
    for row in clases_rows:
        nivel_id = _safe_int(row.get("nivel_id"))
        grado_id = _safe_int(row.get("grado_id"))
        grupo_id = _safe_int(row.get("grupo_id"))
        if nivel_id is None or grado_id is None or grupo_id is None:
            continue
        key = (int(nivel_id), int(grado_id), int(grupo_id))
        entry = grouped.get(key)
        if entry is None:
            entry = {
                "nivel_id": int(nivel_id),
                "nivel": str(row.get("nivel") or ""),
                "grado_id": int(grado_id),
                "grado": str(row.get("grado") or ""),
                "grupo_id": int(grupo_id),
                "seccion": str(row.get("seccion") or ""),
                "colegio_grado_grupo_id": _safe_int(row.get("colegio_grado_grupo_id")),
                "clases": [],
            }
            grouped[key] = entry
        entry["clases"].append(
            {"clase_id": _safe_int(row.get("clase_id")), "clase": str(row.get("clase") or "")}
        )
    return grouped


def listar_y_mapear_clases(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int = 30,
    ordered: bool = True,
    on_log: Optional[Callable[[str], None]] = None,
) -> Tuple[List[Dict[str, object]], Dict[Tuple[int, int, int], Dict[str, object]]]:
    raw_items = fetch_clases_gestion_escolar(
        token=token,
        colegio_id=colegio_id,
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        timeout=timeout,
        ordered=ordered,
    )
    rows: List[Dict[str, object]] = []
    omitted = 0
    for item in raw_items:
        if not isinstance(item, dict):
            omitted += 1
            continue
        row = extract_clase_fields(item)
        if not row:
            omitted += 1
            continue
        rows.append(row)

    grouped = mapear_clases_por_grado_seccion(rows)

    if on_log:
        on_log(f"Clases API: {len(raw_items)} | Normalizadas: {len(rows)} | Omitidas: {omitted}")
        on_log(f"Grupos (nivel+grado+seccion): {len(grouped)}")

    return rows, grouped
