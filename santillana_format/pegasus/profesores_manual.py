from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

import requests

from .clases_api import extract_clase_fields, fetch_clases_gestion_escolar
from .profesores import (
    DEFAULT_CICLO_ID,
    DEFAULT_EMPRESA_ID,
    listar_profesores_filters_data,
)

STAFF_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/clases/{clase_id}/staff"
)
STAFF_PERSON_URL = f"{STAFF_URL}" + "/{persona_id}"
ROLE_CLAVE_PROF = "PROF"
ASIGNAR_NIVEL_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/profesores/{persona_id}/asignarNivel"
)


def listar_profesores_clases_panel_data(
    token: str,
    colegio_id: int,
    empresa_id: int = DEFAULT_EMPRESA_ID,
    ciclo_id: int = DEFAULT_CICLO_ID,
    timeout: int = 30,
    on_progress: Optional[Callable[[str, int, int, str], None]] = None,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], Dict[str, int], List[Dict[str, object]]]:
    profesores_data, _summary_profesores, errores = listar_profesores_filters_data(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
    )

    profesores_by_id: Dict[int, Dict[str, object]] = {}
    for entry in profesores_data:
        persona_id = _safe_int(entry.get("persona_id"))
        if persona_id is None:
            continue
        profesores_by_id[int(persona_id)] = {
            "persona_id": int(persona_id),
            "nombre": _compose_profesor_nombre(
                entry.get("nombre"),
                entry.get("apellido_paterno"),
                entry.get("apellido_materno"),
            ),
            "nombre_base": str(entry.get("nombre") or "").strip(),
            "apellido_paterno": str(entry.get("apellido_paterno") or "").strip(),
            "apellido_materno": str(entry.get("apellido_materno") or "").strip(),
            "login": str(entry.get("login") or "").strip(),
            "dni": str(entry.get("dni") or "").strip(),
            "email": str(entry.get("email") or "").strip(),
            "estado": str(entry.get("estado") or "").strip(),
            "clase_ids_actuales": [],
            "clases_actuales": [],
            "clases_actuales_count": 0,
        }

    raw_clases = fetch_clases_gestion_escolar(
        token=token,
        colegio_id=int(colegio_id),
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        timeout=int(timeout),
        ordered=True,
    )

    clases_rows: List[Dict[str, object]] = []
    for item in raw_clases:
        if not isinstance(item, dict):
            continue
        base_row = extract_clase_fields(item)
        if not base_row:
            continue
        clase_id = _safe_int(base_row.get("clase_id"))
        if clase_id is None:
            continue
        clase_name = str(item.get("geClase") or item.get("alias") or base_row.get("clase") or "").strip()
        clase_code = str(item.get("geClaseClave") or "").strip()
        clases_rows.append(
            {
                **base_row,
                "clase_id": int(clase_id),
                "clase_nombre": clase_name or str(base_row.get("clase") or "").strip(),
                "clase_codigo": clase_code,
                "clase_label": _build_clase_label(
                    clase_name=clase_name,
                    clase_code=clase_code,
                    nivel=str(base_row.get("nivel") or "").strip(),
                    grado=str(base_row.get("grado") or "").strip(),
                    seccion=str(base_row.get("seccion") or "").strip(),
                    activo=bool(base_row.get("activo")),
                    baja=bool(base_row.get("baja")),
                ),
                "staff_persona_ids": [],
                "staff_count": 0,
            }
        )

    total_staff_fetch = len(clases_rows)
    with requests.Session() as session:
        for index, clase_row in enumerate(clases_rows, start=1):
            clase_id = int(clase_row["clase_id"])
            if on_progress:
                on_progress(
                    "staff",
                    index,
                    total_staff_fetch,
                    f"Cargando staff clase {clase_id}",
                )
            staff_rows, error = _fetch_staff_profesores_detalle(
                session=session,
                token=token,
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                clase_id=clase_id,
                timeout=int(timeout),
            )
            if error:
                errores.append(
                    {
                        "tipo": "listar_staff",
                        "persona_id": "",
                        "clase_id": clase_id,
                        "error": error,
                    }
                )
                continue

            staff_persona_ids: List[int] = []
            for staff_row in staff_rows:
                persona_id = _safe_int(staff_row.get("persona_id"))
                if persona_id is None:
                    continue
                persona_id = int(persona_id)
                if persona_id not in profesores_by_id:
                    profesores_by_id[persona_id] = {
                        "persona_id": persona_id,
                        "nombre": str(staff_row.get("nombre") or "").strip() or f"Persona {persona_id}",
                        "nombre_base": str(staff_row.get("nombre_base") or "").strip(),
                        "apellido_paterno": str(staff_row.get("apellido_paterno") or "").strip(),
                        "apellido_materno": str(staff_row.get("apellido_materno") or "").strip(),
                        "login": str(staff_row.get("login") or "").strip(),
                        "dni": str(staff_row.get("dni") or "").strip(),
                        "email": "",
                        "estado": "Activo" if bool(staff_row.get("activo", True)) else "",
                        "clase_ids_actuales": [],
                        "clases_actuales": [],
                        "clases_actuales_count": 0,
                    }
                profesor_row = profesores_by_id[persona_id]
                if clase_id not in profesor_row["clase_ids_actuales"]:
                    profesor_row["clase_ids_actuales"].append(clase_id)
                clase_label = str(clase_row.get("clase_label") or "").strip()
                if clase_label and clase_label not in profesor_row["clases_actuales"]:
                    profesor_row["clases_actuales"].append(clase_label)
                staff_persona_ids.append(persona_id)

            clase_row["staff_persona_ids"] = sorted(set(staff_persona_ids))
            clase_row["staff_count"] = len(clase_row["staff_persona_ids"])

    profesores_rows: List[Dict[str, object]] = []
    for profesor_row in profesores_by_id.values():
        clase_ids_actuales = sorted(
            {int(item) for item in profesor_row.get("clase_ids_actuales", []) if _safe_int(item) is not None}
        )
        clases_actuales = sorted(
            {
                str(item).strip()
                for item in profesor_row.get("clases_actuales", [])
                if str(item).strip()
            }
        )
        profesor_row["clase_ids_actuales"] = clase_ids_actuales
        profesor_row["clases_actuales"] = clases_actuales
        profesor_row["clases_actuales_count"] = len(clase_ids_actuales)
        profesor_row["label"] = _build_profesor_label(profesor_row)
        profesores_rows.append(profesor_row)

    profesores_rows.sort(
        key=lambda row: (
            str(row.get("apellido_paterno") or "").upper(),
            str(row.get("apellido_materno") or "").upper(),
            str(row.get("nombre_base") or row.get("nombre") or "").upper(),
            int(row.get("persona_id") or 0),
        )
    )
    clases_rows.sort(
        key=lambda row: (
            str(row.get("nivel") or "").upper(),
            str(row.get("grado") or "").upper(),
            str(row.get("seccion") or "").upper(),
            str(row.get("clase_nombre") or row.get("clase") or "").upper(),
            int(row.get("clase_id") or 0),
        )
    )

    summary = {
        "profesores_total": len(profesores_rows),
        "clases_total": len(clases_rows),
        "staff_consultas_total": total_staff_fetch,
        "staff_consultas_error": sum(1 for item in errores if item.get("tipo") == "listar_staff"),
    }
    return profesores_rows, clases_rows, summary, errores


def asignar_clases_profesor_manual(
    token: str,
    persona_id: int,
    clase_ids: Sequence[int],
    current_clase_ids: Optional[Sequence[int]] = None,
    nivel_ids: Optional[Sequence[int]] = None,
    colegio_id: Optional[int] = None,
    empresa_id: int = DEFAULT_EMPRESA_ID,
    ciclo_id: int = DEFAULT_CICLO_ID,
    timeout: int = 30,
    dry_run: bool = True,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
) -> Tuple[Dict[str, int], List[str], List[Dict[str, object]]]:
    unique_clase_ids: List[int] = []
    seen = set()
    for item in clase_ids:
        clase_id = _safe_int(item)
        if clase_id is None or clase_id in seen:
            continue
        unique_clase_ids.append(int(clase_id))
        seen.add(int(clase_id))

    unique_current_clase_ids: List[int] = []
    seen_current = set()
    for item in current_clase_ids or []:
        clase_id = _safe_int(item)
        if clase_id is None or clase_id in seen_current:
            continue
        unique_current_clase_ids.append(int(clase_id))
        seen_current.add(int(clase_id))

    unique_nivel_ids: List[int] = []
    seen_levels = set()
    for item in nivel_ids or []:
        nivel_id = _safe_int(item)
        if nivel_id is None or nivel_id in seen_levels:
            continue
        unique_nivel_ids.append(int(nivel_id))
        seen_levels.add(int(nivel_id))

    target_ids = set(unique_clase_ids)
    current_ids = set(unique_current_clase_ids)
    unchanged_ids = sorted(target_ids & current_ids)
    add_ids = sorted(target_ids - current_ids)
    remove_ids = sorted(current_ids - target_ids)
    planned_ops = [("assign", clase_id) for clase_id in add_ids] + [
        ("remove", clase_id) for clase_id in remove_ids
    ]

    summary = {
        "clases_total": len(unique_clase_ids),
        "clases_actuales": len(unique_current_clase_ids),
        "ya_asignadas": 0,
        "pendientes": 0,
        "asignadas": 0,
        "desasignadas": 0,
        "niveles_total": len(unique_nivel_ids),
        "niveles_actualizados": 0,
        "niveles_omitidos": 0,
        "errores_api": 0,
    }
    warnings: List[str] = []
    results: List[Dict[str, object]] = []

    if unchanged_ids:
        summary["ya_asignadas"] = len(unchanged_ids)
        for clase_id in unchanged_ids:
            results.append(
                {
                    "clase_id": int(clase_id),
                    "estado": "ya_asignada",
                    "detalle": "El profesor ya estaba asignado.",
                }
            )

    with requests.Session() as session:
        if unique_nivel_ids and colegio_id is not None:
            if dry_run:
                warnings.append(
                    "Tambien se sincronizaran niveles: {niveles}.".format(
                        niveles=", ".join(str(item) for item in unique_nivel_ids)
                    )
                )
            else:
                ok_niveles, err_niveles = _assign_niveles_profesor(
                    session=session,
                    token=token,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    colegio_id=int(colegio_id),
                    persona_id=int(persona_id),
                    niveles=unique_nivel_ids,
                    timeout=int(timeout),
                )
                if not ok_niveles:
                    summary["errores_api"] += 1
                    summary["niveles_omitidos"] += 1
                    warnings.append(
                        "No se pudieron sincronizar niveles: {err}".format(
                            err=err_niveles or "sin detalle"
                        )
                    )
                else:
                    summary["niveles_actualizados"] = len(unique_nivel_ids)
        elif unique_nivel_ids:
            summary["niveles_omitidos"] += 1
            warnings.append(
                "No se pudo sincronizar niveles porque falta colegio_id."
            )
        elif unique_current_clase_ids and not unique_clase_ids:
            summary["niveles_omitidos"] += 1
            warnings.append(
                "Se quitaran las clases actuales, pero no se modifican niveles cuando no quedan clases seleccionadas."
            )

        if not planned_ops:
            warnings.append("No hay cambios de clases para aplicar.")
            return summary, warnings, results

        total_ops = len(planned_ops)
        for index, (action, clase_id) in enumerate(planned_ops, start=1):
            if on_progress:
                action_label = "alta" if action == "assign" else "baja"
                on_progress(index, total_ops, f"Validando {action_label} clase {clase_id}")

            staff_rows, error = _fetch_staff_profesores_detalle(
                session=session,
                token=token,
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                clase_id=int(clase_id),
                timeout=int(timeout),
            )
            if error:
                summary["errores_api"] += 1
                results.append(
                    {
                        "clase_id": int(clase_id),
                        "estado": "error_staff",
                        "detalle": error,
                    }
                )
                continue

            current_staff_ids = {
                int(item["persona_id"])
                for item in staff_rows
                if _safe_int(item.get("persona_id")) is not None
            }

            if action == "assign" and int(persona_id) in current_staff_ids:
                summary["ya_asignadas"] += 1
                results.append(
                    {
                        "clase_id": int(clase_id),
                        "estado": "ya_asignada",
                        "detalle": "El profesor ya estaba asignado.",
                    }
                )
                continue

            if action == "remove" and int(persona_id) not in current_staff_ids:
                results.append(
                    {
                        "clase_id": int(clase_id),
                        "estado": "ya_desasignada",
                        "detalle": "El profesor ya no estaba asignado.",
                    }
                )
                continue

            if dry_run:
                summary["pendientes"] += 1
                results.append(
                    {
                        "clase_id": int(clase_id),
                        "estado": (
                            "pendiente_asignar"
                            if action == "assign"
                            else "pendiente_desasignar"
                        ),
                        "detalle": (
                            "Asignacion lista para aplicar."
                            if action == "assign"
                            else "Desasignacion lista para aplicar."
                        ),
                    }
                )
                continue

            if on_progress:
                action_label = "Asignando" if action == "assign" else "Desasignando"
                on_progress(index, total_ops, f"{action_label} clase {clase_id}")
            if action == "assign":
                ok, error = _assign_staff_profesor(
                    session=session,
                    token=token,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    clase_id=int(clase_id),
                    persona_id=int(persona_id),
                    timeout=int(timeout),
                )
            else:
                ok, error = _unassign_staff_profesor(
                    session=session,
                    token=token,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    clase_id=int(clase_id),
                    persona_id=int(persona_id),
                    timeout=int(timeout),
                )
            if not ok:
                summary["errores_api"] += 1
                results.append(
                    {
                        "clase_id": int(clase_id),
                        "estado": (
                            "error_asignar"
                            if action == "assign"
                            else "error_desasignar"
                        ),
                        "detalle": error or "Error desconocido.",
                    }
                )
                continue

            if action == "assign":
                summary["asignadas"] += 1
            else:
                summary["desasignadas"] += 1
            results.append(
                {
                    "clase_id": int(clase_id),
                    "estado": "asignada" if action == "assign" else "desasignada",
                    "detalle": (
                        "Asignacion realizada."
                        if action == "assign"
                        else "Desasignacion realizada."
                    ),
                }
            )

    if not unique_clase_ids and unique_current_clase_ids:
        warnings.append("Se desasignaran todas las clases actuales.")

    return summary, warnings, results


def _assign_niveles_profesor(
    session: requests.Session,
    token: str,
    empresa_id: int,
    ciclo_id: int,
    colegio_id: int,
    persona_id: int,
    niveles: Sequence[int],
    timeout: int,
) -> Tuple[bool, Optional[str]]:
    url = ASIGNAR_NIVEL_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        persona_id=int(persona_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"niveles": [{"nivelId": int(nivel_id)} for nivel_id in sorted(set(niveles))]}
    try:
        response = session.post(url, headers=headers, json=payload, timeout=int(timeout))
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
        return False, str(message)
    return True, None


def _safe_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _compose_profesor_nombre(
    nombre: object,
    apellido_paterno: object,
    apellido_materno: object,
) -> str:
    return " ".join(
        item
        for item in (
            str(nombre or "").strip(),
            str(apellido_paterno or "").strip(),
            str(apellido_materno or "").strip(),
        )
        if item
    ).strip()


def _build_profesor_label(row: Dict[str, object]) -> str:
    nombre = str(row.get("nombre") or "").strip() or f"Persona {row.get('persona_id')}"
    login = str(row.get("login") or "").strip()
    dni = str(row.get("dni") or "").strip()
    suffix = []
    if login:
        suffix.append(f"login {login}")
    if dni:
        suffix.append(f"DNI {dni}")
    suffix_text = " | ".join(suffix)
    if suffix_text:
        return f"{nombre} | {suffix_text}"
    return nombre


def _build_clase_label(
    clase_name: str,
    clase_code: str,
    nivel: str,
    grado: str,
    seccion: str,
    activo: bool,
    baja: bool,
) -> str:
    return (clase_name or clase_code or "Clase sin nombre").strip()


def _extract_staff_rows(payload: Dict[str, object]) -> Optional[List[Dict[str, object]]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
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
                return [item for item in candidate if isinstance(item, dict)]
        if "personaId" in data:
            return [data]
    return None


def _fetch_staff_profesores_detalle(
    session: requests.Session,
    token: str,
    empresa_id: int,
    ciclo_id: int,
    clase_id: int,
    timeout: int,
) -> Tuple[List[Dict[str, object]], Optional[str]]:
    url = STAFF_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = session.get(
            url,
            headers=headers,
            params={"rolClave": ROLE_CLAVE_PROF},
            timeout=int(timeout),
        )
    except requests.RequestException as exc:
        return [], f"Error de red: {exc}"

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError:
        return [], f"Respuesta no JSON (status {status_code})"

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        return [], message or f"HTTP {status_code}"

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        return [], message or "Respuesta invalida"

    items = _extract_staff_rows(payload)
    if items is None:
        return [], "Campo data no es lista"

    rows: List[Dict[str, object]] = []
    for item in items:
        rol = item.get("rol") if isinstance(item.get("rol"), dict) else {}
        rol_clave = str(rol.get("rolClave") or "").strip()
        if rol_clave and rol_clave != ROLE_CLAVE_PROF:
            continue
        persona = item.get("persona") if isinstance(item.get("persona"), dict) else {}
        persona_id = item.get("personaId")
        if persona_id is None:
            persona_id = persona.get("personaId")
        persona_id_int = _safe_int(persona_id)
        if persona_id_int is None:
            continue
        persona_login = persona.get("personaLogin") if isinstance(persona.get("personaLogin"), dict) else {}
        rows.append(
            {
                "persona_id": int(persona_id_int),
                "nombre": str(persona.get("nombreCompleto") or "").strip()
                or _compose_profesor_nombre(
                    persona.get("nombre"),
                    persona.get("apellidoPaterno"),
                    persona.get("apellidoMaterno"),
                ),
                "nombre_base": str(persona.get("nombre") or "").strip(),
                "apellido_paterno": str(persona.get("apellidoPaterno") or "").strip(),
                "apellido_materno": str(persona.get("apellidoMaterno") or "").strip(),
                "login": str(persona_login.get("login") or "").strip(),
                "dni": str(persona.get("idOficial") or "").strip(),
                "activo": bool(item.get("activo", True)),
            }
        )
    return rows, None


def _assign_staff_profesor(
    session: requests.Session,
    token: str,
    empresa_id: int,
    ciclo_id: int,
    clase_id: int,
    persona_id: int,
    timeout: int,
) -> Tuple[bool, Optional[str]]:
    url = STAFF_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"rolClave": ROLE_CLAVE_PROF, "personaId": int(persona_id)}
    try:
        response = session.post(url, headers=headers, json=payload, timeout=int(timeout))
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


def _unassign_staff_profesor(
    session: requests.Session,
    token: str,
    empresa_id: int,
    ciclo_id: int,
    clase_id: int,
    persona_id: int,
    timeout: int,
) -> Tuple[bool, Optional[str]]:
    url = STAFF_PERSON_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
        persona_id=int(persona_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = session.delete(url, headers=headers, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        data = response.json() if response.content else {}
    except ValueError:
        data = {}

    if not response.ok:
        message = data.get("message") if isinstance(data, dict) else ""
        return False, message or f"HTTP {status_code}"

    if isinstance(data, dict) and data.get("success") is False:
        message = data.get("message") or "Respuesta invalida"
        return False, message
    return True, None
