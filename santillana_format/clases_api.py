from __future__ import annotations

from typing import Callable, Dict, List, Optional, Tuple

import requests


GESTION_ESCOLAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases"
)


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

    clase_nombre = str(item.get("geClaseClave") or item.get("geClase") or "").strip()

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

