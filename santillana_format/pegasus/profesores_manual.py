import re
import unicodedata
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
PRIMARIA_NIVEL_ID = 39
SANTILLANA_INCLUSIVA_PREFIX = "SANTILLANA INCLUSIVA"
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
            "login_activo": entry.get("login_activo"),
            "niveles_presentes": sorted(
                _unique_ints(entry.get("niveles_presentes") or [])
            ),
            "niveles_activos": {
                int(nivel_id): bool(activo)
                for nivel_id, activo in (
                    entry.get("niveles_activos")
                    if isinstance(entry.get("niveles_activos"), dict)
                    else {}
                ).items()
                if _safe_int(nivel_id) is not None
            },
            "niveles_detalle": sorted(
                _unique_ints(entry.get("niveles_detalle") or [])
            ),
            "niveles_detalle_activos": sorted(
                _unique_ints(entry.get("niveles_detalle_activos") or [])
            ),
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
                "es_santillana_inclusiva": _is_santillana_inclusiva_item(item),
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
                        "login_activo": None,
                        "niveles_presentes": [],
                        "niveles_activos": {},
                        "niveles_detalle": [],
                        "niveles_detalle_activos": [],
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


def build_radartec_profesores_groups(
    profesores: Sequence[Dict[str, object]],
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], Dict[str, int]]:
    vinculados: List[Dict[str, object]] = []
    no_vinculados: List[Dict[str, object]] = []
    seen_persona_ids: Set[int] = set()

    for raw_profesor in profesores:
        if not isinstance(raw_profesor, dict):
            continue
        persona_id = _safe_int(raw_profesor.get("persona_id"))
        if persona_id is None or int(persona_id) in seen_persona_ids:
            continue
        seen_persona_ids.add(int(persona_id))

        clase_ids = sorted(
            _unique_ints(raw_profesor.get("clase_ids_actuales") or [])
        )
        clases_labels = sorted(
            {
                str(value).strip()
                for value in (raw_profesor.get("clases_actuales") or [])
                if str(value).strip()
            }
        )
        login = str(raw_profesor.get("login") or "").strip()
        login_activo = bool(login) and _profesor_login_activo(raw_profesor)
        nombre = str(raw_profesor.get("nombre") or "").strip()
        if not nombre:
            nombre = _compose_profesor_nombre(
                raw_profesor.get("nombre_base"),
                raw_profesor.get("apellido_paterno"),
                raw_profesor.get("apellido_materno"),
            )

        row = {
            "persona_id": int(persona_id),
            "docente": nombre or f"Persona {int(persona_id)}",
            "login": login,
            "login_display": login or "SIN LOGIN",
            "login_activo": bool(login_activo),
            "estado_login": "Activo" if login_activo else "Inactivo",
            "nivel_ids": sorted(_profesor_level_ids(raw_profesor)),
            "clases_total": len(clase_ids),
            "clases": clases_labels,
        }
        if clase_ids:
            vinculados.append(row)
        else:
            no_vinculados.append(row)

    sort_key = lambda row: (
        _normalize_text(row.get("login_display")),
        _normalize_text(row.get("docente")),
        int(row.get("persona_id") or 0),
    )
    vinculados.sort(key=sort_key)
    no_vinculados.sort(key=sort_key)
    summary = {
        "profesores_total": len(vinculados) + len(no_vinculados),
        "vinculados_total": len(vinculados),
        "vinculados_activos": sum(
            1 for row in vinculados if bool(row.get("login_activo"))
        ),
        "vinculados_inactivos": sum(
            1 for row in vinculados if not bool(row.get("login_activo"))
        ),
        "no_vinculados_total": len(no_vinculados),
        "no_vinculados_activos": sum(
            1 for row in no_vinculados if bool(row.get("login_activo"))
        ),
        "no_vinculados_inactivos": sum(
            1 for row in no_vinculados if not bool(row.get("login_activo"))
        ),
    }
    return vinculados, no_vinculados, summary


def build_santillana_inclusiva_profesores_plan(
    profesores: Sequence[Dict[str, object]],
    clases: Sequence[Dict[str, object]],
    primaria_nivel_id: int = PRIMARIA_NIVEL_ID,
) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    clases_by_id: Dict[int, Dict[str, object]] = {}
    inclusivas_primaria: List[Dict[str, object]] = []
    for raw_clase in clases:
        if not isinstance(raw_clase, dict):
            continue
        clase_id = _safe_int(raw_clase.get("clase_id"))
        if clase_id is None:
            continue
        clase = dict(raw_clase)
        clase["clase_id"] = int(clase_id)
        clases_by_id[int(clase_id)] = clase
        if (
            _safe_int(clase.get("nivel_id")) == int(primaria_nivel_id)
            and _is_santillana_inclusiva_class_row(clase)
        ):
            inclusivas_primaria.append(clase)

    plan_rows: List[Dict[str, object]] = []
    for raw_profesor in profesores:
        if not isinstance(raw_profesor, dict):
            continue
        persona_id = _safe_int(raw_profesor.get("persona_id"))
        if persona_id is None:
            continue

        current_ids = sorted(
            _unique_ints(raw_profesor.get("clase_ids_actuales") or [])
        )
        current_primary_classes = [
            clases_by_id[clase_id]
            for clase_id in current_ids
            if clase_id in clases_by_id
            and _safe_int(clases_by_id[clase_id].get("nivel_id"))
            == int(primaria_nivel_id)
        ]
        niveles = _profesor_level_ids(raw_profesor)
        if (
            int(primaria_nivel_id) not in niveles
            and not current_primary_classes
        ):
            continue

        source_classes = [
            clase
            for clase in current_primary_classes
            if not _is_santillana_inclusiva_class_row(clase)
        ]
        source_contexts: List[Dict[str, object]] = []
        seen_contexts: Set[Tuple[int, int, int, str]] = set()
        for clase in source_classes:
            context = _class_context(clase)
            if context is None:
                continue
            context_key = _class_context_identity(context)
            if context_key in seen_contexts:
                continue
            seen_contexts.add(context_key)
            source_contexts.append(context)

        target_classes: List[Dict[str, object]] = []
        seen_target_ids: Set[int] = set()
        for inclusiva in inclusivas_primaria:
            if not any(
                _same_class_context(source_context, inclusiva)
                for source_context in source_contexts
            ):
                continue
            target_id = int(inclusiva["clase_id"])
            if target_id in seen_target_ids:
                continue
            seen_target_ids.add(target_id)
            target_classes.append(
                {
                    "clase_id": target_id,
                    "clase_label": str(
                        inclusiva.get("clase_label")
                        or inclusiva.get("clase_nombre")
                        or inclusiva.get("clase")
                        or f"Clase {target_id}"
                    ).strip(),
                    "grado": str(inclusiva.get("grado") or "").strip(),
                    "seccion": str(inclusiva.get("seccion") or "").strip(),
                    "grado_id": _safe_int(inclusiva.get("grado_id")),
                    "grupo_id": _safe_int(inclusiva.get("grupo_id")),
                }
            )

        current_set = set(current_ids)
        pending_classes = [
            clase
            for clase in target_classes
            if int(clase["clase_id"]) not in current_set
        ]
        already_classes = [
            clase
            for clase in target_classes
            if int(clase["clase_id"]) in current_set
        ]
        nombre = str(raw_profesor.get("nombre") or "").strip()
        if not nombre:
            nombre = _compose_profesor_nombre(
                raw_profesor.get("nombre_base"),
                raw_profesor.get("apellido_paterno"),
                raw_profesor.get("apellido_materno"),
            )
        plan_rows.append(
            {
                "persona_id": int(persona_id),
                "nombre": nombre or f"Persona {int(persona_id)}",
                "login": str(raw_profesor.get("login") or "").strip(),
                "dni": str(raw_profesor.get("dni") or "").strip(),
                "estado": str(raw_profesor.get("estado") or "").strip(),
                "contextos": source_contexts,
                "contextos_labels": [
                    _format_class_context(context) for context in source_contexts
                ],
                "clases_destino": target_classes,
                "clases_pendientes": pending_classes,
                "clases_ya_asignadas": already_classes,
                "clase_ids_actuales": current_ids,
            }
        )

    plan_rows.sort(
        key=lambda row: (
            _normalize_text(row.get("nombre")),
            int(row.get("persona_id") or 0),
        )
    )
    summary = {
        "docentes_primaria": len(plan_rows),
        "docentes_con_contexto": sum(
            1 for row in plan_rows if row.get("contextos")
        ),
        "docentes_con_cambios": sum(
            1 for row in plan_rows if row.get("clases_pendientes")
        ),
        "asignaciones_pendientes": sum(
            len(row.get("clases_pendientes") or []) for row in plan_rows
        ),
        "clases_inclusivas_primaria": len(inclusivas_primaria),
    }
    return plan_rows, summary


def asignar_santillana_inclusiva_profesores(
    token: str,
    plan_rows: Sequence[Dict[str, object]],
    empresa_id: int = DEFAULT_EMPRESA_ID,
    ciclo_id: int = DEFAULT_CICLO_ID,
    timeout: int = 30,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
) -> Tuple[Dict[str, int], List[Dict[str, object]]]:
    operations: List[Tuple[int, str, Dict[str, object]]] = []
    for plan in plan_rows:
        if not isinstance(plan, dict):
            continue
        persona_id = _safe_int(plan.get("persona_id"))
        if persona_id is None:
            continue
        nombre = str(plan.get("nombre") or f"Persona {persona_id}").strip()
        for clase in plan.get("clases_pendientes") or []:
            if not isinstance(clase, dict) or _safe_int(clase.get("clase_id")) is None:
                continue
            operations.append((int(persona_id), nombre, dict(clase)))

    summary = {
        "docentes_procesados": len({persona_id for persona_id, _, _ in operations}),
        "asignaciones_total": len(operations),
        "asignadas": 0,
        "ya_asignadas": 0,
        "errores_api": 0,
    }
    results: List[Dict[str, object]] = []
    staff_cache: Dict[int, Set[int]] = {}
    staff_errors: Dict[int, str] = {}

    with requests.Session() as session:
        for index, (persona_id, nombre, clase) in enumerate(operations, start=1):
            clase_id = int(clase["clase_id"])
            clase_label = str(
                clase.get("clase_label") or f"Clase {clase_id}"
            ).strip()
            if on_progress:
                on_progress(
                    index,
                    len(operations),
                    f"{nombre}: {clase_label}",
                )

            if clase_id not in staff_cache and clase_id not in staff_errors:
                staff_rows, error = _fetch_staff_profesores_detalle(
                    session=session,
                    token=token,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    clase_id=clase_id,
                    timeout=int(timeout),
                )
                if error:
                    staff_errors[clase_id] = error
                else:
                    staff_cache[clase_id] = {
                        int(row["persona_id"])
                        for row in staff_rows
                        if _safe_int(row.get("persona_id")) is not None
                    }

            if clase_id in staff_errors:
                summary["errores_api"] += 1
                results.append(
                    {
                        "persona_id": persona_id,
                        "nombre": nombre,
                        "clase_id": clase_id,
                        "clase": clase_label,
                        "estado": "error_staff",
                        "detalle": staff_errors[clase_id],
                    }
                )
                continue

            current_staff = staff_cache.setdefault(clase_id, set())
            if persona_id in current_staff:
                summary["ya_asignadas"] += 1
                results.append(
                    {
                        "persona_id": persona_id,
                        "nombre": nombre,
                        "clase_id": clase_id,
                        "clase": clase_label,
                        "estado": "ya_asignada",
                        "detalle": "El profesor ya estaba asignado.",
                    }
                )
                continue

            ok, error = _assign_staff_profesor(
                session=session,
                token=token,
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                clase_id=clase_id,
                persona_id=persona_id,
                timeout=int(timeout),
            )
            if not ok:
                summary["errores_api"] += 1
                results.append(
                    {
                        "persona_id": persona_id,
                        "nombre": nombre,
                        "clase_id": clase_id,
                        "clase": clase_label,
                        "estado": "error_asignar",
                        "detalle": error or "Error desconocido.",
                    }
                )
                continue

            current_staff.add(persona_id)
            summary["asignadas"] += 1
            results.append(
                {
                    "persona_id": persona_id,
                    "nombre": nombre,
                    "clase_id": clase_id,
                    "clase": clase_label,
                    "estado": "asignada",
                    "detalle": "Asignacion realizada.",
                }
            )

    return summary, results


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


def _unique_ints(values: Sequence[object]) -> Set[int]:
    return {
        int(value)
        for value in values
        if _safe_int(value) is not None
    }


def _normalize_text(value: object) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(
        char for char in text if unicodedata.category(char) != "Mn"
    )
    return re.sub(r"\s+", " ", text).strip()


def _normalize_section(value: object) -> str:
    text = _normalize_text(value)
    match = re.search(r"([A-Z0-9]+)\s*$", text)
    return match.group(1) if match else text


def _is_santillana_inclusiva_item(item: Dict[str, object]) -> bool:
    if _is_santillana_inclusiva_class_row(item):
        return True
    clase_materias = (
        item.get("claseMaterias")
        if isinstance(item.get("claseMaterias"), list)
        else []
    )
    for entry in clase_materias:
        if not isinstance(entry, dict):
            continue
        materia = (
            entry.get("materia")
            if isinstance(entry.get("materia"), dict)
            else {}
        )
        if _normalize_text(materia.get("materia")) == "NO APLICA":
            return True
    return False


def _is_santillana_inclusiva_class_row(row: Dict[str, object]) -> bool:
    explicit = row.get("es_santillana_inclusiva")
    if explicit is not None:
        return bool(explicit)
    search_text = " ".join(
        _normalize_text(row.get(key))
        for key in (
            "clase",
            "clase_nombre",
            "clase_codigo",
            "clase_label",
            "geClase",
            "geClaseClave",
            "alias",
        )
        if row.get(key)
    )
    return SANTILLANA_INCLUSIVA_PREFIX in search_text


def _profesor_level_ids(row: Dict[str, object]) -> Set[int]:
    level_ids: Set[int] = set()
    for key in (
        "niveles_presentes",
        "niveles_detalle",
        "niveles_detalle_activos",
    ):
        values = row.get(key)
        if isinstance(values, (list, tuple, set)):
            level_ids.update(_unique_ints(values))
    activos = row.get("niveles_activos")
    if isinstance(activos, dict):
        for nivel_id, activo in activos.items():
            nivel_id_int = _safe_int(nivel_id)
            if nivel_id_int is not None and bool(activo):
                level_ids.add(int(nivel_id_int))
    return level_ids


def _profesor_login_activo(row: Dict[str, object]) -> bool:
    explicit = row.get("login_activo")
    if isinstance(explicit, bool):
        return explicit
    if isinstance(explicit, (int, float)):
        return explicit != 0
    if isinstance(explicit, str) and explicit.strip():
        return _normalize_text(explicit) in {
            "ACTIVO",
            "ACTIVE",
            "ENABLED",
            "SI",
            "TRUE",
            "1",
        }
    return _normalize_text(row.get("estado")) in {
        "ACTIVO",
        "ACTIVA",
        "ACTIVE",
        "ENABLED",
        "SI",
        "TRUE",
        "1",
    }


def _class_context(row: Dict[str, object]) -> Optional[Dict[str, object]]:
    nivel_id = _safe_int(row.get("nivel_id"))
    grado_id = _safe_int(row.get("grado_id"))
    if nivel_id is None or grado_id is None:
        return None
    return {
        "nivel_id": int(nivel_id),
        "grado_id": int(grado_id),
        "grupo_id": _safe_int(row.get("grupo_id")),
        "nivel": str(row.get("nivel") or "").strip(),
        "grado": str(row.get("grado") or "").strip(),
        "seccion": str(row.get("seccion") or "").strip(),
    }


def _class_context_identity(
    context: Dict[str, object],
) -> Tuple[int, int, int, str]:
    return (
        int(_safe_int(context.get("nivel_id")) or 0),
        int(_safe_int(context.get("grado_id")) or 0),
        int(_safe_int(context.get("grupo_id")) or 0),
        _normalize_section(context.get("seccion")),
    )


def _same_class_context(
    source_context: Dict[str, object],
    target_class: Dict[str, object],
) -> bool:
    target_context = _class_context(target_class)
    if target_context is None:
        return False
    if (
        _safe_int(source_context.get("nivel_id"))
        != _safe_int(target_context.get("nivel_id"))
        or _safe_int(source_context.get("grado_id"))
        != _safe_int(target_context.get("grado_id"))
    ):
        return False

    source_group_id = _safe_int(source_context.get("grupo_id"))
    target_group_id = _safe_int(target_context.get("grupo_id"))
    if source_group_id is not None and target_group_id is not None:
        return int(source_group_id) == int(target_group_id)

    source_section = _normalize_section(source_context.get("seccion"))
    target_section = _normalize_section(target_context.get("seccion"))
    return bool(source_section and source_section == target_section)


def _format_class_context(context: Dict[str, object]) -> str:
    grado = str(context.get("grado") or "").strip()
    seccion = str(context.get("seccion") or "").strip()
    if grado and seccion:
        return f"{grado} {seccion}"
    if grado:
        return grado
    grado_id = _safe_int(context.get("grado_id"))
    if seccion:
        return f"Grado {grado_id or '-'} {seccion}"
    return f"Grado {grado_id or '-'}"


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
