from __future__ import annotations

import os
import unicodedata
from datetime import date
from typing import Dict, List

import streamlit as st

from .ssr import (
    IPA_DEFAULT_NIVEL_ID,
    IPA_DEFAULT_TIMEOUT,
    IPA_SCHOOLS_BASE_URL,
    add_ipa_course_to_planificacion,
    build_ipa_add_course_url,
    build_ipa_course_progress_payload,
    build_ipa_result_payload,
    build_ipa_result_rows,
    build_ipa_school_courses_payload,
    build_ipa_schools_payload,
    extract_ipa_session_value,
    fetch_ipa_add_course_context,
    fetch_ipa_course_progress,
    fetch_ipa_grade_areas,
    fetch_ipa_grade_listing,
    fetch_ipa_massive_planificaciones,
    fetch_ipa_school_courses,
    fetch_ipa_schools_listing,
    fetch_ipa_structure,
    search_ipa_course_for_planificacion,
)


def _ensure_ipa_session_state_defaults() -> None:
    current_session = str(st.session_state.get("ipa_session_value", "") or "").strip()
    if "ipa_session_value_input" not in st.session_state:
        st.session_state["ipa_session_value_input"] = current_session
    elif not str(st.session_state.get("ipa_session_value_input", "") or "").strip() and current_session:
        st.session_state["ipa_session_value_input"] = current_session

    current_base_url = str(st.session_state.get("ipa_base_url", "") or "").strip()
    if not current_base_url:
        current_base_url = str(os.environ.get("IPA_BASE_URL", "") or "").strip()
        if not current_base_url:
            current_base_url = IPA_SCHOOLS_BASE_URL
        st.session_state["ipa_base_url"] = current_base_url
    if "ipa_base_url_input" not in st.session_state:
        st.session_state["ipa_base_url_input"] = current_base_url
    elif not str(st.session_state.get("ipa_base_url_input", "") or "").strip() and current_base_url:
        st.session_state["ipa_base_url_input"] = current_base_url


def _sync_ipa_session_from_input() -> None:
    session_value = extract_ipa_session_value(
        st.session_state.get("ipa_session_value_input", "")
    )
    st.session_state["ipa_session_value"] = session_value
    st.session_state["ipa_session_bridge_mode"] = "write"
    st.session_state["ipa_session_bridge_value"] = session_value
    base_url = str(st.session_state.get("ipa_base_url_input", "") or "").strip()
    st.session_state["ipa_base_url"] = base_url or IPA_SCHOOLS_BASE_URL


def _get_ipa_add_course_contexts() -> Dict[str, Dict[str, Dict[str, object]]]:
    raw = st.session_state.get("ipa_add_course_contexts")
    if not isinstance(raw, dict):
        contexts: Dict[str, Dict[str, Dict[str, object]]] = {}
        st.session_state["ipa_add_course_contexts"] = contexts
        return contexts

    is_old_shape = any(
        str(key or "").strip().lower() in {"comunicacion", "matematica"} for key in raw.keys()
    )
    if is_old_shape:
        migrated: Dict[str, Dict[str, Dict[str, object]]] = {}
        for area_key, context in raw.items():
            if not isinstance(context, dict):
                continue
            params = context.get("params") if isinstance(context.get("params"), dict) else {}
            grade_id = str(context.get("gradeId") or params.get("id_nivel") or "").strip()
            area_key_clean = str(area_key or context.get("areaKey") or "").strip().lower()
            if not grade_id or not area_key_clean:
                continue
            migrated.setdefault(grade_id, {})[area_key_clean] = context
        st.session_state["ipa_add_course_contexts"] = migrated
        return migrated

    contexts: Dict[str, Dict[str, Dict[str, object]]] = {}
    for grade_id, grade_contexts in raw.items():
        grade_id_clean = str(grade_id or "").strip()
        if not grade_id_clean:
            continue
        if not isinstance(grade_contexts, dict):
            continue
        contexts[grade_id_clean] = {
            str(area_key or "").strip().lower(): context
            for area_key, context in grade_contexts.items()
            if isinstance(context, dict) and str(area_key or "").strip()
        }

    st.session_state["ipa_add_course_contexts"] = contexts
    return contexts


def _get_ipa_grade_name_to_id_overrides() -> Dict[str, Dict[str, str]]:
    raw = st.session_state.get("ipa_grade_name_to_id_override")
    if not isinstance(raw, dict):
        overrides: Dict[str, Dict[str, str]] = {}
        st.session_state["ipa_grade_name_to_id_override"] = overrides
        return overrides

    cleaned: Dict[str, Dict[str, str]] = {}
    for nivel_id, mapping in raw.items():
        nivel_id_clean = str(nivel_id or "").strip()
        if not nivel_id_clean or not isinstance(mapping, dict):
            continue
        cleaned[nivel_id_clean] = {
            str(key or "").strip(): str(value or "").strip()
            for key, value in mapping.items()
            if str(key or "").strip() and str(value or "").strip()
        }

    st.session_state["ipa_grade_name_to_id_override"] = cleaned
    return cleaned


def _resolve_ipa_grade_name_from_id(grade_id: str) -> str:
    grade_id_clean = str(grade_id or "").strip()
    if not grade_id_clean:
        return ""

    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    base_url = str(st.session_state.get("ipa_base_url", "") or "").strip()
    nivel_id = int(st.session_state.get("ipa_nivel_id", IPA_DEFAULT_NIVEL_ID) or IPA_DEFAULT_NIVEL_ID)
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)

    try:
        result = fetch_ipa_grade_listing(
            session_value=session_value,
            base_url=base_url,
            nivel_id=nivel_id,
            timeout=timeout,
        )
    except Exception:
        return ""

    for grade in result.grades:
        if str(grade.grade_id) == grade_id_clean:
            return str(grade.name or "").strip()
    return ""


def _resolve_ipa_target_area_key(grade_id: str, area_id: str) -> str:
    grade_id_clean = str(grade_id or "").strip()
    area_id_clean = str(area_id or "").strip()
    if not grade_id_clean or not area_id_clean:
        return ""

    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    base_url = str(st.session_state.get("ipa_base_url", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)

    try:
        result = fetch_ipa_grade_areas(
            session_value=session_value,
            grade_id=grade_id_clean,
            base_url=base_url,
            timeout=timeout,
            target_areas_only=True,
        )
    except Exception:
        return {"32": "comunicacion", "35": "matematica"}.get(area_id_clean, "")

    for area in result.areas:
        if str(area.area_id) == area_id_clean:
            area_key = str(area.area_key or "").strip().lower()
            if area_key in {"comunicacion", "matematica"}:
                return area_key
    return {"32": "comunicacion", "35": "matematica"}.get(area_id_clean, "")


def _store_ipa_add_course_context(context: Dict[str, object]) -> str:
    params = context.get("params") if isinstance(context.get("params"), dict) else {}
    grade_id = str(params.get("id_nivel") or "").strip()
    area_id = str(params.get("id_area") or "").strip()
    if not grade_id or not area_id:
        return ""

    area_key = _resolve_ipa_target_area_key(grade_id, area_id)
    grade_name = _resolve_ipa_grade_name_from_id(grade_id)
    if area_key:
        context["areaKey"] = area_key
        context["gradeId"] = grade_id
        context["gradeName"] = grade_name
        contexts = _get_ipa_add_course_contexts()
        contexts.setdefault(grade_id, {})[area_key] = context
        st.session_state["ipa_add_course_contexts"] = contexts
    return area_key


def _render_ipa_grade_listing_preview() -> None:
    with st.container(border=True):
        st.markdown("**Currículum (grados y cursos)**")
        notice = str(st.session_state.pop("ipa_last_grade_listing_notice", "") or "").strip()
        if notice:
            if notice.lower().startswith("no se pudo"):
                st.warning(notice)
            else:
                st.success(notice)

        payload = st.session_state.get("ipa_last_grade_listing_payload")
        rows = st.session_state.get("ipa_last_grade_listing_rows") or []
        if not isinstance(payload, dict) or not isinstance(rows, list) or not rows:
            st.info("Presiona 'Listar colegios' para cargar los grados del currículum.")
            return

        contexts = _get_ipa_add_course_contexts()
        summary_rows: List[Dict[str, object]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            grade_name = str(row.get("Curso") or "").strip()
            grade_id = str(row.get("Curso ID") or "").strip()
            grade_contexts = contexts.get(grade_id) if isinstance(contexts, dict) else None
            if not isinstance(grade_contexts, dict):
                grade_contexts = {}
            summary_rows.append(
                {
                    "Grado": grade_name,
                    "Grado ID": grade_id,
                    "Comunicación (área)": "32",
                    "Matemática (área)": "35",
                    "Contexto Comunicación": "sí" if grade_contexts.get("comunicacion") else "no",
                    "Contexto Matemática": "sí" if grade_contexts.get("matematica") else "no",
                }
            )

        st.caption(
            f"Grados: {len(summary_rows)} | Requests: {len(payload.get('visitedUrls') or [])}"
        )
        st.dataframe(summary_rows, use_container_width=True, hide_index=True)

        with st.expander("Ver rutas Comunicación/Matemática"):
            routes_notice = str(st.session_state.pop("ipa_grade_routes_notice", "") or "").strip()
            if routes_notice:
                st.success(routes_notice)

            if st.button(
                "Mapear rutas de todos los grados",
                key="ipa_map_grade_routes_btn",
                use_container_width=True,
            ):
                try:
                    _load_ipa_grade_routes()
                except Exception as exc:
                    st.error(str(exc))
                else:
                    st.rerun()

            route_rows = st.session_state.get("ipa_grade_routes_rows") or []
            if isinstance(route_rows, list) and route_rows:
                st.dataframe(route_rows, use_container_width=True, hide_index=True)
            else:
                st.info("Aún no se han mapeado las rutas (usa el botón).")

        with st.expander("Ver JSON"):
            st.json(payload)


def _normalize_ipa_view_lookup(value: object) -> str:
    text = unicodedata.normalize("NFD", str(value or ""))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return " ".join(text.lower().split())


def _is_ipa_course_already_included(search_response: Dict[str, object]) -> bool:
    response_type = _normalize_ipa_view_lookup(search_response.get("tipo", ""))
    message = _normalize_ipa_view_lookup(search_response.get("mensaje", ""))
    return response_type == "exito_ya_incluido" or "ya esta incluido" in message


def _ipa_search_response_course_name(search_response: Dict[str, object]) -> str:
    data = search_response.get("data") if isinstance(search_response, dict) else {}
    if not isinstance(data, dict):
        return ""
    course = data.get("curso") if isinstance(data.get("curso"), dict) else {}
    return str(course.get("nombre") or "").strip()


def _format_ipa_school_option(school_item: Dict[str, object]) -> str:
    school_id = str(school_item.get("id") or "").strip()
    name = str(school_item.get("nombre") or "Colegio").strip()
    lms_id = str(school_item.get("idLms") or "").strip()
    suffix = f" | LMS: {lms_id}" if lms_id else ""
    return f"{school_id or '-'} | {name}{suffix}"


def _select_ipa_course(grade_item: Dict[str, object], course_item: Dict[str, object]) -> None:
    grade = grade_item.get("grado")
    if not isinstance(grade, dict):
        grade = {}
    st.session_state["ipa_selected_course"] = {
        "grado": {
            "nombre": str(grade.get("nombre") or "").strip(),
            "id": str(grade.get("id") or "").strip(),
        },
        "curso": {
            "nombre": str(course_item.get("nombre") or "").strip(),
            "areaId": str(course_item.get("areaId") or "").strip(),
            "gradoId": str(course_item.get("gradoId") or "").strip(),
            "url": str(course_item.get("url") or "").strip(),
        },
    }


def _load_ipa_course_progress(grade_item: Dict[str, object], course_item: Dict[str, object]) -> None:
    _select_ipa_course(grade_item, course_item)
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    course_url = str(course_item.get("url") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    with st.spinner("Leyendo Progreso academico del curso..."):
        result = fetch_ipa_course_progress(
            session_value=session_value,
            course_url=course_url,
            timeout=timeout,
        )
    st.session_state["ipa_last_progress_payload"] = build_ipa_course_progress_payload(result)
    st.session_state["ipa_last_planificaciones_payload"] = {}
    st.session_state["ipa_last_progress_notice"] = (
        f"Progreso academico leido. Evaluaciones: {len(result.evaluations)}."
    )


def _load_ipa_planificaciones(massive_planning_url: str) -> None:
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    with st.spinner("Leyendo planificaciones masivas..."):
        payload = fetch_ipa_massive_planificaciones(
            session_value=session_value,
            massive_planning_url=massive_planning_url,
            timeout=timeout,
        )
    st.session_state["ipa_last_planificaciones_payload"] = payload
    data = payload.get("data") if isinstance(payload, dict) else {}
    planificaciones = data.get("planificaciones") if isinstance(data, dict) else []
    count = len(planificaciones) if isinstance(planificaciones, list) else 0
    st.session_state["ipa_last_planificaciones_notice"] = (
        f"Planificaciones leidas. Registros: {count}."
    )


def _open_ipa_add_course_context(add_course_url: str) -> None:
    _sync_ipa_session_from_input()
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    with st.spinner("Abriendo formulario para agregar cursos..."):
        context = fetch_ipa_add_course_context(
            session_value=session_value,
            add_course_url=add_course_url,
            timeout=timeout,
        )
    st.session_state["ipa_add_course_context"] = context
    st.session_state["ipa_add_course_search_result"] = {}
    st.session_state["ipa_add_course_result"] = {}
    st.session_state["ipa_school_course_add_results"] = {}
    st.session_state["ipa_school_courses_batch_notice"] = ""
    area_key = _store_ipa_add_course_context(context)
    area_label = {"comunicacion": "Comunicación", "matematica": "Matemática"}.get(area_key, "")
    grade_label = str(context.get("gradeName") or "") if isinstance(context, dict) else ""
    grade_label = grade_label.strip()
    suffix = f" | Grado: {grade_label}" if grade_label else ""
    if area_label:
        st.session_state["ipa_add_course_notice"] = f"Contexto guardado: {area_label}{suffix}."
    else:
        st.session_state["ipa_add_course_notice"] = "Formulario listo para buscar curso."


def _search_ipa_add_course(curso_id_override: object = None) -> Dict[str, object]:
    context = st.session_state.get("ipa_add_course_context")
    if not isinstance(context, dict) or not context:
        raise ValueError("Primero abre una planificacion con Agregar cursos.")
    curso_id = str(
        curso_id_override
        if curso_id_override is not None
        else st.session_state.get("ipa_add_course_id_input", "")
    ).strip()
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    with st.spinner("Buscando curso..."):
        response = search_ipa_course_for_planificacion(
            session_value=session_value,
            context=context,
            curso_id=curso_id,
            timeout=timeout,
        )
    st.session_state["ipa_add_course_search_result"] = response
    st.session_state["ipa_add_course_result"] = {}
    return response


def _submit_ipa_add_course(curso_id_override: object = None) -> Dict[str, object]:
    context = st.session_state.get("ipa_add_course_context")
    if not isinstance(context, dict) or not context:
        raise ValueError("Primero abre una planificacion con Agregar cursos.")
    curso_id = str(
        curso_id_override
        if curso_id_override is not None
        else st.session_state.get("ipa_add_course_id_input", "")
    ).strip()
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    with st.spinner("Agregando curso a la planificacion..."):
        response = add_ipa_course_to_planificacion(
            session_value=session_value,
            context=context,
            curso_id=curso_id,
            timeout=timeout,
        )
    st.session_state["ipa_add_course_result"] = response
    if response.get("success"):
        st.session_state["ipa_add_course_search_result"] = {}
    return response


def _add_ipa_school_course_to_planificacion(course_item: Dict[str, object]) -> Dict[str, object]:
    context = st.session_state.get("ipa_add_course_context")
    if not isinstance(context, dict) or not context:
        raise ValueError("Primero abre una planificacion con Agregar cursos.")

    curso_id = str(course_item.get("id") or "").strip()
    if not curso_id:
        raise ValueError("El curso seleccionado no tiene ID.")

    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    result: Dict[str, object] = {
        "cursoId": curso_id,
        "cursoNombre": str(course_item.get("nombre") or "").strip(),
        "grado": str(course_item.get("grado") or "").strip(),
    }
    with st.spinner(f"Validando y agregando curso {curso_id}..."):
        search_response = search_ipa_course_for_planificacion(
            session_value=session_value,
            context=context,
            curso_id=curso_id,
            timeout=timeout,
        )
        result["buscar"] = search_response
        result["cursoApiNombre"] = _ipa_search_response_course_name(search_response)
        if search_response.get("success") and search_response.get("puede_agregar"):
            add_response = add_ipa_course_to_planificacion(
                session_value=session_value,
                context=context,
                curso_id=curso_id,
                timeout=timeout,
            )
            result["agregar"] = add_response
            result["estado"] = "agregado" if add_response.get("success") else "error_agregar"
            result["mensaje"] = str(add_response.get("mensaje") or "").strip()
        elif search_response.get("success") and _is_ipa_course_already_included(search_response):
            result["estado"] = "ya_incluido"
            result["mensaje"] = str(search_response.get("mensaje") or "").strip()
        else:
            result["estado"] = "no_agregado"
            result["mensaje"] = str(search_response.get("mensaje") or "").strip()

    add_results = st.session_state.get("ipa_school_course_add_results")
    if not isinstance(add_results, dict):
        add_results = {}
    add_results[curso_id] = result
    st.session_state["ipa_school_course_add_results"] = add_results
    return result


def _verify_ipa_school_course_assignment(course_item: Dict[str, object]) -> Dict[str, object]:
    context = st.session_state.get("ipa_add_course_context")
    if not isinstance(context, dict) or not context:
        raise ValueError("Primero abre una planificacion con Agregar cursos.")

    curso_id = str(course_item.get("id") or "").strip()
    if not curso_id:
        raise ValueError("El curso seleccionado no tiene ID.")

    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    result: Dict[str, object] = {
        "cursoId": curso_id,
        "cursoNombre": str(course_item.get("nombre") or "").strip(),
        "grado": str(course_item.get("grado") or "").strip(),
    }
    with st.spinner(f"Verificando asignacion del curso {curso_id}..."):
        search_response = search_ipa_course_for_planificacion(
            session_value=session_value,
            context=context,
            curso_id=curso_id,
            timeout=timeout,
        )
    result["buscar"] = search_response
    result["cursoApiNombre"] = _ipa_search_response_course_name(search_response)
    result["mensaje"] = str(search_response.get("mensaje") or "").strip()
    if search_response.get("success") and _is_ipa_course_already_included(search_response):
        result["estado"] = "ya_incluido"
    elif search_response.get("success") and search_response.get("puede_agregar"):
        result["estado"] = "disponible"
    elif search_response.get("success"):
        result["estado"] = "no_agregado"
    else:
        result["estado"] = "error"

    add_results = st.session_state.get("ipa_school_course_add_results")
    if not isinstance(add_results, dict):
        add_results = {}
    add_results[curso_id] = result
    st.session_state["ipa_school_course_add_results"] = add_results
    return result


def _verify_ipa_school_courses_assignment(course_items: List[Dict[str, object]]) -> None:
    if not course_items:
        raise ValueError("No hay cursos para verificar.")

    already_included = 0
    available = 0
    not_added = 0
    failed = 0
    for course_item in course_items:
        try:
            result = _verify_ipa_school_course_assignment(course_item)
        except Exception as exc:
            failed += 1
            course_id = str(course_item.get("id") or "").strip()
            add_results = st.session_state.get("ipa_school_course_add_results")
            if not isinstance(add_results, dict):
                add_results = {}
            add_results[course_id or f"error_{failed}"] = {
                "cursoId": course_id,
                "estado": "error",
                "mensaje": str(exc),
            }
            st.session_state["ipa_school_course_add_results"] = add_results
            continue

        estado = str(result.get("estado") or "").strip()
        if estado == "ya_incluido":
            already_included += 1
        elif estado == "disponible":
            available += 1
        elif estado == "no_agregado":
            not_added += 1
        else:
            failed += 1

    st.session_state["ipa_school_courses_batch_notice"] = (
        "Verificacion terminada. "
        f"Ya asignados: {already_included} | Disponibles para agregar: {available} | "
        f"No agregados: {not_added} | Errores: {failed}."
    )


def _add_ipa_school_courses_to_planificacion(course_items: List[Dict[str, object]]) -> None:
    if not course_items:
        raise ValueError("No hay cursos para agregar.")

    added = 0
    already_included = 0
    available = 0
    not_added = 0
    failed = 0
    for course_item in course_items:
        try:
            result = _add_ipa_school_course_to_planificacion(course_item)
        except Exception as exc:
            failed += 1
            course_id = str(course_item.get("id") or "").strip()
            add_results = st.session_state.get("ipa_school_course_add_results")
            if not isinstance(add_results, dict):
                add_results = {}
            add_results[course_id or f"error_{failed}"] = {
                "cursoId": course_id,
                "estado": "error",
                "mensaje": str(exc),
            }
            st.session_state["ipa_school_course_add_results"] = add_results
            continue

        estado = str(result.get("estado") or "").strip()
        if estado == "agregado":
            added += 1
        elif estado == "ya_incluido":
            already_included += 1
        elif estado == "disponible":
            available += 1
        elif estado == "no_agregado":
            not_added += 1
        else:
            failed += 1

    st.session_state["ipa_school_courses_batch_notice"] = (
        "Proceso terminado. "
        f"Agregados: {added} | Ya incluidos: {already_included} | "
        f"Disponibles: {available} | No agregados: {not_added} | Errores: {failed}."
    )


def _load_ipa_grade_listing() -> None:
    _sync_ipa_session_from_input()
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    base_url = str(st.session_state.get("ipa_base_url", "") or "").strip()
    nivel_id = int(st.session_state.get("ipa_nivel_id", IPA_DEFAULT_NIVEL_ID) or IPA_DEFAULT_NIVEL_ID)
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    with st.spinner("Leyendo cursos del nivel..."):
        result = fetch_ipa_grade_listing(
            session_value=session_value,
            base_url=base_url,
            nivel_id=nivel_id,
            timeout=timeout,
        )

    payload: Dict[str, object] = {
        "baseUrl": result.base_url,
        "nivelUrl": result.level_url,
        "nivelResponseUrl": result.level_response_url,
        "visitedUrls": result.visited_urls,
        "cursos": [
            {
                "id": grade.grade_id,
                "nombre": grade.name,
                "url": grade.url,
            }
            for grade in result.grades
        ],
    }
    rows: List[Dict[str, object]] = [
        {
            "Curso": grade.name,
            "Curso ID": grade.grade_id,
            "URL": grade.url,
        }
        for grade in result.grades
    ]

    st.session_state["ipa_last_grade_listing_payload"] = payload
    st.session_state["ipa_last_grade_listing_rows"] = rows
    st.session_state["ipa_last_grade_listing_notice"] = f"Currículum cargado. Cursos: {len(rows)}."


def _load_ipa_grade_routes() -> None:
    _sync_ipa_session_from_input()
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    base_url = str(st.session_state.get("ipa_base_url", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)

    grade_rows = st.session_state.get("ipa_last_grade_listing_rows")
    if not isinstance(grade_rows, list) or not grade_rows:
        _load_ipa_grade_listing()
        grade_rows = st.session_state.get("ipa_last_grade_listing_rows")

    if not isinstance(grade_rows, list) or not grade_rows:
        raise RuntimeError("Primero carga el currículum (grados).")

    mapping_rows: List[Dict[str, object]] = []
    visited_urls: List[str] = []
    progress = st.progress(0)
    for index, row in enumerate(grade_rows):
        if not isinstance(row, dict):
            continue
        grade_name = str(row.get("Curso") or "").strip()
        grade_id = str(row.get("Curso ID") or "").strip()
        if not grade_id:
            continue
        try:
            result = fetch_ipa_grade_areas(
                session_value=session_value,
                grade_id=grade_id,
                base_url=base_url,
                timeout=timeout,
                target_areas_only=True,
            )
        except Exception as exc:
            mapping_rows.append(
                {
                    "Grado": grade_name,
                    "Grado ID": grade_id,
                    "Áreas URL": "",
                    "Comunicación areaId": "",
                    "Comunicación URL": "",
                    "Matemática areaId": "",
                    "Matemática URL": "",
                    "Error": str(exc),
                }
            )
        else:
            visited_urls.extend(result.visited_urls)
            comm = next((area for area in result.areas if area.area_key == "comunicacion"), None)
            math = next((area for area in result.areas if area.area_key == "matematica"), None)
            mapping_rows.append(
                {
                    "Grado": grade_name,
                    "Grado ID": grade_id,
                    "Áreas URL": result.areas_response_url or result.areas_url,
                    "Comunicación areaId": getattr(comm, "area_id", "") if comm is not None else "",
                    "Comunicación URL": getattr(comm, "url", "") if comm is not None else "",
                    "Matemática areaId": getattr(math, "area_id", "") if math is not None else "",
                    "Matemática URL": getattr(math, "url", "") if math is not None else "",
                    "Error": "",
                }
            )
        progress.progress(min(1.0, (index + 1) / max(1, len(grade_rows))))

    st.session_state["ipa_grade_routes_rows"] = mapping_rows
    st.session_state["ipa_grade_routes_payload"] = {
        "baseUrl": base_url,
        "visitedUrls": visited_urls,
        "rows": mapping_rows,
    }
    st.session_state["ipa_grade_routes_notice"] = f"Rutas mapeadas: {len(mapping_rows)} grados."


def _load_ipa_schools() -> None:
    _sync_ipa_session_from_input()
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    with st.spinner("Leyendo listado de colegios desde santadmin.pleno.digital..."):
        result = fetch_ipa_schools_listing(
            session_value=session_value,
            timeout=timeout,
        )
    st.session_state["ipa_schools_payload"] = build_ipa_schools_payload(result)
    st.session_state["ipa_school_courses_payload"] = {}
    st.session_state["ipa_selected_school_id"] = (
        result.schools[0].school_id if result.schools else ""
    )
    st.session_state["ipa_schools_notice"] = (
        f"Colegios leidos. Registros: {len(result.schools)}."
    )
    try:
        _load_ipa_grade_listing()
    except Exception as exc:
        st.session_state["ipa_last_grade_listing_notice"] = f"No se pudo cargar el currículum: {exc}"


def _load_ipa_school_courses(school_item: Dict[str, object]) -> None:
    _sync_ipa_session_from_input()
    session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
    base_url = str(st.session_state.get("ipa_base_url", "") or "").strip()
    timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
    school_id = str(school_item.get("id") or "").strip()
    school_name = str(school_item.get("nombre") or "").strip()
    school_url = str(school_item.get("url") or "").strip()
    with st.spinner(f"Leyendo cursos de {school_name or school_id or 'colegio'}..."):
        result = fetch_ipa_school_courses(
            session_value=session_value,
            school_url_or_id=school_url or school_id,
            base_url=base_url,
            school_id=school_id,
            school_name=school_name,
            timeout=timeout,
        )
    payload = build_ipa_school_courses_payload(result)
    st.session_state["ipa_school_courses_payload"] = payload
    st.session_state["ipa_school_course_add_results"] = {}
    st.session_state["ipa_school_courses_batch_notice"] = ""
    st.session_state.pop("ipa_pending_school_course_assignment", None)
    st.session_state.pop("ipa_school_courses_assignment_notice", None)
    st.session_state.pop("ipa_school_courses_assignment_notice_ok", None)
    st.session_state["ipa_school_courses_notice"] = (
        f"Cursos leidos. Registros: {len(result.courses)}."
    )


def _render_selected_ipa_course() -> None:
    selected = st.session_state.get("ipa_selected_course")
    if not isinstance(selected, dict):
        return
    grade = selected.get("grado")
    course = selected.get("curso")
    if not isinstance(grade, dict) or not isinstance(course, dict):
        return

    course_name = str(course.get("nombre") or "").strip()
    grade_name = str(grade.get("nombre") or "").strip()
    course_url = str(course.get("url") or "").strip()
    if not course_name and not course_url:
        return

    with st.container(border=True):
        st.markdown("**Curso seleccionado**")
        st.caption(
            f"{grade_name or 'Grado'} | {course_name or 'Curso'} | "
            f"areaId: {course.get('areaId') or '-'} | gradoId: {course.get('gradoId') or '-'}"
        )
        if course_url:
            st.link_button(
                "Abrir unidades del curso",
                url=course_url,
                use_container_width=True,
            )


def _render_ipa_course_buttons(payload: Dict[str, object]) -> None:
    grades = payload.get("grados")
    if not isinstance(grades, list) or not grades:
        return

    _render_selected_ipa_course()
    for grade_index, grade_item in enumerate(grades):
        if not isinstance(grade_item, dict):
            continue
        grade = grade_item.get("grado")
        courses = grade_item.get("areas")
        if not isinstance(grade, dict) or not isinstance(courses, list):
            continue

        grade_name = str(grade.get("nombre") or "Grado").strip()
        grade_id = str(grade.get("id") or "").strip()
        with st.expander(
            f"{grade_name} | gradoId: {grade_id or '-'} | cursos: {len(courses)}",
            expanded=grade_index == 0,
        ):
            if not courses:
                st.warning("No se encontraron cursos para este grado.")
                continue

            for course_index, course_item in enumerate(courses):
                if not isinstance(course_item, dict):
                    continue
                course_name = str(course_item.get("nombre") or "Curso").strip()
                course_url = str(course_item.get("url") or "").strip()
                course_cols = st.columns([3, 0.9, 0.9, 0.9], gap="small")
                course_cols[0].markdown(
                    f"**{course_name}**  \n"
                    f"areaId: `{course_item.get('areaId') or '-'}` | "
                    f"gradoId: `{course_item.get('gradoId') or '-'}`"
                )
                if course_cols[1].button(
                    "Seleccionar",
                    key=f"ipa_select_course_{grade_id}_{course_item.get('areaId')}_{course_index}",
                    use_container_width=True,
                ):
                    _select_ipa_course(grade_item, course_item)
                    st.rerun()
                if course_cols[2].button(
                    "Progreso",
                    key=f"ipa_progress_course_{grade_id}_{course_item.get('areaId')}_{course_index}",
                    use_container_width=True,
                ):
                    try:
                        _load_ipa_course_progress(grade_item, course_item)
                    except Exception as exc:
                        st.error(str(exc))
                if course_url:
                    course_cols[3].link_button(
                        f"Abrir {course_name} {course_item.get('areaId') or course_index}",
                        url=course_url,
                        use_container_width=True,
                    )


def _render_ipa_planificaciones_result() -> None:
    payload = st.session_state.get("ipa_last_planificaciones_payload")
    if not isinstance(payload, dict) or not payload:
        return

    notice = str(st.session_state.pop("ipa_last_planificaciones_notice", "") or "").strip()
    if notice:
        st.success(notice)

    data = payload.get("data")
    if not isinstance(data, dict):
        st.warning("La respuesta de planificaciones no contiene datos.")
        return
    planificaciones = data.get("planificaciones")
    if not isinstance(planificaciones, list):
        st.warning("La respuesta de planificaciones no contiene una lista.")
        return

    st.markdown("**Planificaciones masivas**")
    st.caption(f"Registros: {len(planificaciones)}")
    if not planificaciones:
        st.info("No se encontraron planificaciones masivas.")
        return

    selected_massive_url = ""
    progress_payload = st.session_state.get("ipa_last_progress_payload")
    if isinstance(progress_payload, dict):
        evaluations = progress_payload.get("evaluaciones")
        if isinstance(evaluations, list):
            for evaluation in evaluations:
                if isinstance(evaluation, dict) and evaluation.get("planificacionMasivaUrl"):
                    selected_massive_url = str(evaluation.get("planificacionMasivaUrl") or "").strip()
                    break
    data_eval = data.get("evaluacion") if isinstance(data.get("evaluacion"), dict) else {}
    data_unit = data.get("unidad") if isinstance(data.get("unidad"), dict) else {}
    data_area = data.get("area") if isinstance(data.get("area"), dict) else {}
    data_grade = data.get("nivel") if isinstance(data.get("nivel"), dict) else {}
    data_type = str(data.get("tipo") or "").strip()

    for index, planificacion in enumerate(planificaciones):
        if not isinstance(planificacion, dict):
            continue
        id_lote = str(planificacion.get("id_lote") or "").strip()
        is_closed = str(planificacion.get("estado") or "").strip().lower() == "cerrada"
        add_course_url = build_ipa_add_course_url(
            selected_massive_url or str(payload.get("responseUrl") or payload.get("ajaxUrl") or ""),
            id_lote,
            eval_id=data_eval.get("id_prueba"),
            unit_id=data_unit.get("id_unidad"),
            area_id=data_area.get("id_area"),
            grade_id=data_grade.get("id_nivel"),
            eval_type=data_type,
        )
        row_cols = st.columns([3.5, 1, 1], gap="small")
        row_cols[0].markdown(
            f"**{id_lote or index + 1}** | {planificacion.get('estado') or '-'}  \n"
            f"Inicio: `{planificacion.get('inicio_utc') or '-'}` | "
            f"Fin: `{planificacion.get('fin_utc') or '-'}`"
        )
        if add_course_url:
            row_cols[1].link_button(
                "Abrir agregar",
                url=add_course_url,
                use_container_width=True,
                disabled=is_closed,
            )
            if row_cols[2].button(
                "Agregar cursos",
                key=f"ipa_open_add_course_{id_lote or index}",
                use_container_width=True,
                disabled=is_closed,
            ):
                try:
                    _open_ipa_add_course_context(add_course_url)
                except Exception as exc:
                    st.error(str(exc))
        else:
            row_cols[1].caption("Sin enlace")

    _render_ipa_add_course_panel()


def _render_ipa_add_course_panel() -> None:
    context = st.session_state.get("ipa_add_course_context")
    if not isinstance(context, dict) or not context:
        return

    notice = str(st.session_state.pop("ipa_add_course_notice", "") or "").strip()
    if notice:
        st.success(notice)

    params = context.get("params") if isinstance(context.get("params"), dict) else {}
    with st.container(border=True):
        st.markdown("**Agregar cursos nuevos**")
        st.caption(
            f"id_lote: {params.get('id_lote') or '-'} | "
            f"id_prueba: {params.get('id_prueba') or '-'} | "
            f"id_area: {params.get('id_area') or '-'} | "
            f"id_nivel: {params.get('id_nivel') or '-'} | "
            f"tipo: {params.get('tipo') or '-'} | "
            f"id_pais: {params.get('id_pais') or '-'}"
        )

        input_col, search_col = st.columns([3, 1], gap="small")
        with input_col:
            st.text_input(
                "ID Curso",
                key="ipa_add_course_id_input",
                placeholder="Ejemplo: 123456",
            )
        with search_col:
            st.markdown(
                "<div style='height: 1.85rem;' aria-hidden='true'></div>",
                unsafe_allow_html=True,
            )
            if st.button(
                "Buscar curso",
                key="ipa_add_course_search_btn",
                use_container_width=True,
            ):
                try:
                    _search_ipa_add_course()
                except Exception as exc:
                    st.error(str(exc))

        search_result = st.session_state.get("ipa_add_course_search_result")
        can_add = False
        if isinstance(search_result, dict) and search_result:
            message = str(search_result.get("mensaje") or "").strip()
            if search_result.get("success"):
                data = search_result.get("data") if isinstance(search_result.get("data"), dict) else {}
                course = data.get("curso") if isinstance(data.get("curso"), dict) else {}
                st.info(
                    f"{message or 'Curso encontrado.'} "
                    f"ID {course.get('colegio_id') or '-'}, {course.get('colegio_nombre') or '-'} | "
                    f"ID {course.get('id') or '-'}, {course.get('nombre') or '-'}"
                )
                can_add = bool(search_result.get("puede_agregar"))
                if not can_add:
                    st.warning("Este curso no se puede agregar o ya esta incluido.")
            else:
                st.warning(message or "No se pudo encontrar el curso.")

        add_result = st.session_state.get("ipa_add_course_result")
        if isinstance(add_result, dict) and add_result:
            message = str(add_result.get("mensaje") or "").strip()
            if add_result.get("success"):
                st.success(message or "Curso agregado.")
            else:
                st.warning(message or "No se pudo agregar el curso.")

        if st.button(
            "Agregar curso",
            key="ipa_add_course_submit_btn",
            type="primary",
            use_container_width=True,
            disabled=not can_add,
        ):
            try:
                _submit_ipa_add_course()
            except Exception as exc:
                st.error(str(exc))
            else:
                st.rerun()


def _render_ipa_progress_result() -> None:
    payload = st.session_state.get("ipa_last_progress_payload")
    if not isinstance(payload, dict) or not payload:
        return

    notice = str(st.session_state.pop("ipa_last_progress_notice", "") or "").strip()
    if notice:
        st.success(notice)

    evaluations = payload.get("evaluaciones")
    if not isinstance(evaluations, list):
        return

    st.markdown("**Progreso academico**")
    st.caption(
        f"Evaluaciones: {len(evaluations)} | URL: {payload.get('progressResponseUrl') or payload.get('progressUrl') or '-'}"
    )
    if not evaluations:
        st.warning("No se encontraron evaluaciones en Progreso academico.")
        return

    for index, evaluation in enumerate(evaluations):
        if not isinstance(evaluation, dict):
            continue
        eval_id = str(evaluation.get("evalId") or index).strip()
        massive_url = str(evaluation.get("planificacionMasivaUrl") or "").strip()
        detail_url = str(evaluation.get("detalleUrl") or "").strip()
        row_cols = st.columns([3.5, 0.8, 1, 1.1], gap="small")
        row_cols[0].markdown(
            f"**{evaluation.get('nombre') or 'Evaluacion'}**  \n"
            f"evalId: `{evaluation.get('evalId') or '-'}` | "
            f"items: `{evaluation.get('items') or '-'}` | "
            f"unidadId: `{evaluation.get('unidadId') or '-'}`"
        )
        if detail_url:
            row_cols[1].link_button(
                f"Ver {eval_id}",
                url=detail_url,
                use_container_width=True,
            )
        if massive_url:
            row_cols[2].link_button(
                f"Masiva {eval_id}",
                url=massive_url,
                use_container_width=True,
            )
            if row_cols[3].button(
                "Leer masiva",
                key=f"ipa_read_massive_{eval_id}_{index}",
                use_container_width=True,
            ):
                try:
                    _load_ipa_planificaciones(massive_url)
                except Exception as exc:
                    st.error(str(exc))
        else:
            row_cols[2].caption("Sin masiva")

    _render_ipa_planificaciones_result()


def _render_ipa_school_courses_result() -> None:
    payload = st.session_state.get("ipa_school_courses_payload")
    if not isinstance(payload, dict) or not payload:
        return

    notice = str(st.session_state.pop("ipa_school_courses_notice", "") or "").strip()
    if notice:
        st.success(notice)

    school = payload.get("colegio")
    if not isinstance(school, dict):
        school = {}

    year_filter = str(
        int(st.session_state.get("ipa_school_year_filter", date.today().year) or date.today().year)
    )

    courses_raw = payload.get("cursos")
    courses: List[Dict[str, object]] = [
        course for course in courses_raw if isinstance(course, dict)
    ] if isinstance(courses_raw, list) else []

    filtered_courses = [
        course
        for course in courses
        if str(course.get("anoLectivo") or "").strip() == year_filter
    ]

    st.markdown("**Cursos del colegio**")
    st.caption(
        f"{school.get('nombre') or 'Colegio'} | ID: {school.get('id') or '-'} | "
        f"Año lectivo: {year_filter} | Secciones: {len(filtered_courses)}"
    )
    if not filtered_courses:
        st.warning(f"No se encontraron cursos del año lectivo {year_filter} para este colegio.")
        return

    assignment_notice = str(st.session_state.pop("ipa_school_courses_assignment_notice", "") or "").strip()
    assignment_ok = bool(st.session_state.pop("ipa_school_courses_assignment_notice_ok", False))
    if assignment_notice:
        if assignment_ok:
            st.success(assignment_notice)
        else:
            st.warning(assignment_notice)

    grade_listing_rows = st.session_state.get("ipa_last_grade_listing_rows") or []
    grade_name_to_id: Dict[str, str] = {}
    grade_id_to_name: Dict[str, str] = {}
    if isinstance(grade_listing_rows, list):
        for row in grade_listing_rows:
            if not isinstance(row, dict):
                continue
            grade_name = str(row.get("Curso") or "").strip()
            grade_id = str(row.get("Curso ID") or "").strip()
            if not grade_name or not grade_id:
                continue
            key = _normalize_ipa_view_lookup(grade_name)
            if key and key not in grade_name_to_id:
                grade_name_to_id[key] = grade_id
            if grade_id not in grade_id_to_name:
                grade_id_to_name[grade_id] = grade_name

    nivel_id = int(st.session_state.get("ipa_nivel_id", IPA_DEFAULT_NIVEL_ID) or IPA_DEFAULT_NIVEL_ID)
    overrides_by_nivel = _get_ipa_grade_name_to_id_overrides()
    nivel_overrides = overrides_by_nivel.get(str(nivel_id), {})
    if not isinstance(nivel_overrides, dict):
        nivel_overrides = {}

    contexts = _get_ipa_add_course_contexts()

    add_results_raw = st.session_state.get("ipa_school_course_add_results")
    add_results = add_results_raw if isinstance(add_results_raw, dict) else {}

    def _area_results(area_key: str) -> Dict[str, Dict[str, object]]:
        area = add_results.get(area_key)
        return area if isinstance(area, dict) else {}

    pending = st.session_state.get("ipa_pending_school_course_assignment")
    if isinstance(pending, dict) and pending.get("courseId") and pending.get("areaKey"):
        pending_school_id = str(pending.get("schoolId") or "").strip()
        current_school_id = str(school.get("id") or "").strip()
        if pending_school_id and current_school_id and pending_school_id != current_school_id:
            st.session_state.pop("ipa_pending_school_course_assignment", None)
        else:
            pending_area_key = str(pending.get("areaKey") or "").strip().lower()
            pending_course_id = str(pending.get("courseId") or "").strip()
            pending_course_name = str(pending.get("courseName") or "").strip()
            pending_grade_id = str(pending.get("gradeId") or "").strip()
            pending_grade_name = str(pending.get("gradeName") or "").strip()
            pending_label = {"comunicacion": "Comunicación", "matematica": "Matemática"}.get(
                pending_area_key,
                pending_area_key or "Área",
            )
            with st.container(border=True):
                st.markdown(
                    f"**{pending_label}**  \n"
                    f"Sección: `{pending_course_id}` | {pending_course_name or '-'}  \n"
                    f"Grado: {pending_grade_name or '-'}"
                )
                grade_contexts = contexts.get(pending_grade_id)
                if not isinstance(grade_contexts, dict):
                    grade_contexts = {}
                context = grade_contexts.get(pending_area_key)

                pending_checked = bool(pending.get("checked"))
                pending_can_add = bool(pending.get("canAdd"))
                search_response = pending.get("searchResponse") if pending_checked else {}

                if not isinstance(context, dict) or not context:
                    st.error(
                        f"No hay contexto guardado para {pending_grade_name or pending_grade_id} / {pending_label}."
                    )
                    action_cols = st.columns([1, 1], gap="small")
                    if action_cols[0].button(
                        "Cerrar",
                        key=f"ipa_close_assign_{pending_area_key}_{pending_course_id}",
                        use_container_width=True,
                    ):
                        st.session_state.pop("ipa_pending_school_course_assignment", None)
                        st.rerun()
                    if action_cols[1].button(
                        "Cancelar",
                        key=f"ipa_cancel_assign_{pending_area_key}_{pending_course_id}",
                        use_container_width=True,
                    ):
                        st.session_state.pop("ipa_pending_school_course_assignment", None)
                        st.rerun()
                else:
                    if not isinstance(search_response, dict) or not search_response:
                        session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
                        timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
                        try:
                            with st.spinner(f"Verificando {pending_label} para {pending_course_id}..."):
                                search_response = search_ipa_course_for_planificacion(
                                    session_value=session_value,
                                    context=context,
                                    curso_id=pending_course_id,
                                    timeout=timeout,
                                )
                        except Exception as exc:
                            search_response = {"success": False, "mensaje": str(exc)}

                        pending["checked"] = True
                        pending["searchResponse"] = search_response
                        st.session_state["ipa_pending_school_course_assignment"] = pending

                    message = str(search_response.get("mensaje") or "").strip()
                    already_included = bool(
                        search_response.get("success") and _is_ipa_course_already_included(search_response)
                    )
                    pending_can_add = bool(search_response.get("success") and search_response.get("puede_agregar"))
                    pending["canAdd"] = pending_can_add
                    st.session_state["ipa_pending_school_course_assignment"] = pending

                    estado = "error"
                    if already_included:
                        estado = "ya_incluido"
                        st.success(message or "Ya está registrado.")
                    elif pending_can_add:
                        estado = "disponible"
                        st.info(message or "No está registrado. Se puede agregar.")
                    elif search_response.get("success"):
                        estado = "no_agregado"
                        st.warning(message or "No se puede agregar este curso.")
                    else:
                        st.error(message or "No se pudo verificar el curso.")

                    result: Dict[str, object] = {
                        "areaKey": pending_area_key,
                        "gradoId": pending_grade_id,
                        "gradoNombre": pending_grade_name,
                        "cursoId": pending_course_id,
                        "cursoNombre": pending_course_name,
                        "buscar": search_response if isinstance(search_response, dict) else {},
                        "cursoApiNombre": _ipa_search_response_course_name(search_response)
                        if isinstance(search_response, dict)
                        else "",
                        "estado": estado,
                        "mensaje": message,
                    }

                    stored = st.session_state.get("ipa_school_course_add_results")
                    if not isinstance(stored, dict):
                        stored = {}
                    area_bucket = stored.get(pending_area_key)
                    if not isinstance(area_bucket, dict):
                        area_bucket = {}
                    area_bucket[pending_course_id] = result
                    stored[pending_area_key] = area_bucket
                    st.session_state["ipa_school_course_add_results"] = stored

                    action_cols = st.columns([1, 1], gap="small")
                    if pending_can_add and action_cols[0].button(
                        "Confirmar asignación",
                        key=f"ipa_apply_assign_{pending_area_key}_{pending_course_id}",
                        type="primary",
                        use_container_width=True,
                    ):
                        session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
                        timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
                        add_result: Dict[str, object] = dict(result)
                        try:
                            with st.spinner(f"Asignando {pending_label} a {pending_course_id}..."):
                                add_response = add_ipa_course_to_planificacion(
                                    session_value=session_value,
                                    context=context,
                                    curso_id=pending_course_id,
                                    timeout=timeout,
                                )
                        except Exception as exc:
                            add_result["agregar"] = {"success": False, "mensaje": str(exc)}
                            add_result["estado"] = "error"
                            add_result["mensaje"] = str(exc)
                        else:
                            add_result["agregar"] = add_response
                            add_result["estado"] = "agregado" if add_response.get("success") else "error_agregar"
                            add_result["mensaje"] = str(add_response.get("mensaje") or "").strip()

                        stored = st.session_state.get("ipa_school_course_add_results")
                        if not isinstance(stored, dict):
                            stored = {}
                        area_bucket = stored.get(pending_area_key)
                        if not isinstance(area_bucket, dict):
                            area_bucket = {}
                        area_bucket[pending_course_id] = add_result
                        stored[pending_area_key] = area_bucket
                        st.session_state["ipa_school_course_add_results"] = stored

                        ok = str(add_result.get("estado") or "") in {"agregado", "ya_incluido"}
                        st.session_state["ipa_school_courses_assignment_notice"] = (
                            add_result.get("mensaje")
                            or f"{pending_label}: {add_result.get('estado') or 'ok'}"
                        )
                        st.session_state["ipa_school_courses_assignment_notice_ok"] = ok
                        st.session_state.pop("ipa_pending_school_course_assignment", None)
                        st.rerun()

                    if action_cols[1].button(
                        "Cerrar",
                        key=f"ipa_close_assign_{pending_area_key}_{pending_course_id}",
                        use_container_width=True,
                    ):
                        st.session_state.pop("ipa_pending_school_course_assignment", None)
                        st.rerun()

    grade_to_courses: Dict[str, List[Dict[str, object]]] = {}
    for course in filtered_courses:
        grade_name = str(course.get("grado") or "").strip() or "Sin grado"
        grade_to_courses.setdefault(grade_name, []).append(course)

    groups_raw = payload.get("grados")
    ordered_grades: List[str] = []
    if isinstance(groups_raw, list):
        for group in groups_raw:
            if not isinstance(group, dict):
                continue
            name = str(group.get("nombre") or "").strip()
            if name and name in grade_to_courses and name not in ordered_grades:
                ordered_grades.append(name)
    for name in sorted(grade_to_courses.keys()):
        if name not in ordered_grades:
            ordered_grades.append(name)

    for idx, grade_name in enumerate(ordered_grades):
        grade_courses = grade_to_courses.get(grade_name, [])
        norm_key = _normalize_ipa_view_lookup(grade_name)
        auto_grade_id = grade_name_to_id.get(norm_key, "")
        override_grade_id = str(nivel_overrides.get(norm_key) or "").strip()
        grade_id = override_grade_id or auto_grade_id
        grade_contexts = contexts.get(grade_id) if grade_id else None
        if not isinstance(grade_contexts, dict):
            grade_contexts = {}
        comm_ready = bool(grade_contexts.get("comunicacion"))
        math_ready = bool(grade_contexts.get("matematica"))

        exp_label = (
            f"{grade_name} | secciones: {len(grade_courses)} | "
            f"gradoId: {grade_id or '-'} | "
            f"ctx C: {'sí' if comm_ready else 'no'} | ctx M: {'sí' if math_ready else 'no'}"
        )
        with st.expander(exp_label, expanded=idx == 0):
            if grade_id_to_name:
                sorted_grade_ids = sorted(
                    grade_id_to_name.keys(),
                    key=lambda value: _normalize_ipa_view_lookup(grade_id_to_name.get(str(value), value)),
                )
                override_options = [""] + sorted_grade_ids
                desired_override = override_grade_id if override_grade_id in grade_id_to_name else ""
                try:
                    override_index = override_options.index(desired_override)
                except ValueError:
                    override_index = 0
                selected_override = st.selectbox(
                    f"Grado currículum para '{grade_name}'",
                    options=override_options,
                    index=override_index,
                    key=f"ipa_grade_override_{nivel_id}_{norm_key.replace(' ', '_')}",
                    format_func=lambda value: (
                        f"(Auto) {auto_grade_id} | {grade_id_to_name.get(auto_grade_id, '')}".strip()
                        if str(value) == ""
                        else f"{value} | {grade_id_to_name.get(str(value), '')}".strip()
                    ),
                )
                selected_override = str(selected_override or "").strip()
                if selected_override:
                    nivel_overrides[norm_key] = selected_override
                else:
                    nivel_overrides.pop(norm_key, None)
                overrides_by_nivel[str(nivel_id)] = nivel_overrides
                st.session_state["ipa_grade_name_to_id_override"] = overrides_by_nivel

                grade_id = selected_override or auto_grade_id
                grade_contexts = contexts.get(grade_id) if grade_id else None
                if not isinstance(grade_contexts, dict):
                    grade_contexts = {}
                comm_ready = bool(grade_contexts.get("comunicacion"))
                math_ready = bool(grade_contexts.get("matematica"))

            if not grade_id:
                st.warning(
                    "No se pudo hacer match del grado con el currículum. "
                    "Verifica que el currículum esté cargado y que el Nivel ID sea correcto."
                )
            if not comm_ready or not math_ready:
                st.info(
                    "Para habilitar los botones, guarda el contexto de planificación para Comunicación y Matemática "
                    "del grado correspondiente (en 'Avanzado')."
                )

            for course in sorted(grade_courses, key=lambda item: str(item.get("nombre") or "")):
                course_id = str(course.get("id") or "").strip()
                course_name = str(course.get("nombre") or "").strip()
                students = str(course.get("alumnos") or "").strip()
                if not course_id:
                    continue

                comm_status = ""
                math_status = ""
                comm_result = _area_results("comunicacion").get(course_id)
                math_result = _area_results("matematica").get(course_id)
                if isinstance(comm_result, dict):
                    comm_status = str(comm_result.get("estado") or "").strip()
                if isinstance(math_result, dict):
                    math_status = str(math_result.get("estado") or "").strip()

                row_cols = st.columns([3.6, 1, 1], gap="small")
                status_line = ""
                if comm_status or math_status:
                    status_line = f"  \nEstado | C: `{comm_status or '-'}` | M: `{math_status or '-'}`"
                row_cols[0].markdown(
                    f"**{course_name or course_id}**  \n"
                    f"Curso ID: `{course_id}` | Alumnos: `{students or '-'}`"
                    f"{status_line}"
                )

                if row_cols[1].button(
                    "Comunicación",
                    key=f"ipa_assign_comm_{school.get('id')}_{course_id}",
                    use_container_width=True,
                    disabled=not (grade_id and comm_ready),
                ):
                    st.session_state["ipa_pending_school_course_assignment"] = {
                        "schoolId": str(school.get("id") or "").strip(),
                        "areaKey": "comunicacion",
                        "gradeName": grade_name,
                        "gradeId": grade_id,
                        "courseId": course_id,
                        "courseName": course_name,
                    }
                    st.rerun()

                if row_cols[2].button(
                    "Matemática",
                    key=f"ipa_assign_math_{school.get('id')}_{course_id}",
                    use_container_width=True,
                    disabled=not (grade_id and math_ready),
                ):
                    st.session_state["ipa_pending_school_course_assignment"] = {
                        "schoolId": str(school.get("id") or "").strip(),
                        "areaKey": "matematica",
                        "gradeName": grade_name,
                        "gradeId": grade_id,
                        "courseId": course_id,
                        "courseName": course_name,
                    }
                    st.rerun()


def _render_ipa_schools_section() -> None:
    with st.container(border=True):
        st.markdown("**Colegios**")
        st.caption("Año lectivo: 2026")
        st.session_state["ipa_school_year_filter"] = 2026
        if st.button(
            "Listar colegios",
            key="ipa_read_schools_btn",
            type="primary",
            use_container_width=True,
        ):
            try:
                _load_ipa_schools()
            except Exception as exc:
                st.error(str(exc))

        notice = str(st.session_state.pop("ipa_schools_notice", "") or "").strip()
        if notice:
            st.success(notice)

        payload = st.session_state.get("ipa_schools_payload")
        if not isinstance(payload, dict) or not payload:
            st.info("Lee el listado para buscar un colegio y ver sus cursos por grado.")
            return

        schools = payload.get("colegios")
        if not isinstance(schools, list):
            st.warning("La respuesta de colegios no contiene una lista.")
            return

        school_by_id: Dict[str, Dict[str, object]] = {}
        school_ids: List[str] = []
        for school in schools:
            if not isinstance(school, dict):
                continue
            school_id = str(school.get("id") or "").strip()
            if not school_id or school_id in school_by_id:
                continue
            school_by_id[school_id] = school
            school_ids.append(school_id)

        if not school_ids:
            st.warning("No se encontraron colegios validos en la respuesta.")
            return

        current_school_id = str(st.session_state.get("ipa_selected_school_id", "") or "").strip()
        if current_school_id and current_school_id not in school_by_id:
            st.session_state["ipa_selected_school_id"] = school_ids[0]

        st.caption(f"Colegios cargados: {len(school_ids)}")
        selected_school_id = st.selectbox(
            "Colegio",
            options=school_ids,
            format_func=lambda school_id: _format_ipa_school_option(
                school_by_id.get(str(school_id), {})
            ),
            key="ipa_selected_school_id",
        )
        selected_school = school_by_id.get(str(selected_school_id), {})
        if not selected_school:
            st.warning("Selecciona un colegio valido.")
            return

        st.markdown(
            f"**{selected_school.get('nombre') or 'Colegio'}**  \n"
            f"ID: `{selected_school_id or '-'}` | ID LMS: `{selected_school.get('idLms') or '-'}` | "
            f"Huso: `{selected_school.get('husoHorario') or '-'}` | "
            f"Offline: `{selected_school.get('plenoOffline') or '-'}` | "
            f"Demo: `{selected_school.get('demo') or '-'}`"
        )

        current_courses_payload = st.session_state.get("ipa_school_courses_payload")
        current_loaded_id = ""
        if isinstance(current_courses_payload, dict):
            current_school = (
                current_courses_payload.get("colegio")
                if isinstance(current_courses_payload.get("colegio"), dict)
                else {}
            )
            current_loaded_id = str(current_school.get("id") or "").strip()
        if current_loaded_id != str(selected_school_id):
            try:
                _load_ipa_school_courses(selected_school)
            except Exception as exc:
                st.error(str(exc))

    _render_ipa_school_courses_result()


def _render_ipa_result_preview() -> None:
    payload = st.session_state.get("ipa_last_payload")
    rows = st.session_state.get("ipa_last_rows") or []
    if not isinstance(payload, dict) or not isinstance(rows, list) or not rows:
        return

    st.markdown("**Resultado**")
    st.caption(
        f"Grados: {len(payload.get('grados') or [])} | Cursos: {len(rows)} | "
        f"Requests: {len(payload.get('visitedUrls') or [])}"
    )

    _render_ipa_course_buttons(payload)
    _render_ipa_progress_result()

    with st.expander("Ver estructura JSON"):
        st.json(payload)


def render_ipa_view() -> None:
    _ensure_ipa_session_state_defaults()

    st.subheader("IPA")
    st.caption(
        "Lee todos los cursos de todos los grados usando la cookie local-santadmin."
    )

    current_session = str(st.session_state.get("ipa_session_value", "") or "").strip()
    if current_session:
        st.caption("Hay una cookie local-santadmin cargada en la sesion actual.")
    else:
        st.warning("No hay cookie local-santadmin cargada. Usa Lectura Tokens o pegala manualmente aqui.")

    with st.container(border=True):
        base_col, nivel_col, timeout_col = st.columns([3.2, 0.8, 1], gap="small")
        with base_col:
            st.text_input(
                "URL base Santadmin",
                key="ipa_base_url_input",
                placeholder="https://dominio-del-santadmin",
                help="Dominio base donde existen las rutas /niveles, /areas, /unidad y /evaluaciones.",
            )
        with nivel_col:
            st.number_input(
                "Nivel ID",
                min_value=1,
                max_value=999999,
                value=int(st.session_state.get("ipa_nivel_id", IPA_DEFAULT_NIVEL_ID) or IPA_DEFAULT_NIVEL_ID),
                step=1,
                key="ipa_nivel_id",
            )
        with timeout_col:
            st.number_input(
                "Timeout",
                min_value=5,
                max_value=120,
                value=int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT),
                step=1,
                key="ipa_timeout",
            )

        session_col, save_col = st.columns([5.2, 1], gap="small")
        with session_col:
            st.text_input(
                "local-santadmin",
                key="ipa_session_value_input",
                type="password",
                help="Acepta solo el SESSION_VALUE o un Cookie header que contenga local-santadmin=...",
            )
        with save_col:
            st.markdown(
                "<div style='height: 1.85rem;' aria-hidden='true'></div>",
                unsafe_allow_html=True,
            )
            if st.button("Usar", key="ipa_use_session_btn", use_container_width=True):
                _sync_ipa_session_from_input()
                st.rerun()

    curriculum_col, schools_col = st.columns([1, 1], gap="large")
    with curriculum_col:
        _render_ipa_grade_listing_preview()
    with schools_col:
        _render_ipa_schools_section()

    with st.expander("Avanzado: lectura y planificaciones", expanded=False):
        if st.button(
            "Cargar currículum (grados)",
            key="ipa_fetch_grade_listing_btn",
            use_container_width=True,
        ):
            try:
                _load_ipa_grade_listing()
            except Exception as exc:
                st.error(str(exc))
            else:
                st.rerun()

        if st.button(
            "Leer cursos de todos los grados",
            type="primary",
            key="ipa_fetch_structure_btn",
            use_container_width=True,
        ):
            _sync_ipa_session_from_input()
            session_value = str(st.session_state.get("ipa_session_value", "") or "").strip()
            base_url = str(st.session_state.get("ipa_base_url", "") or "").strip()
            nivel_id = int(st.session_state.get("ipa_nivel_id", IPA_DEFAULT_NIVEL_ID) or IPA_DEFAULT_NIVEL_ID)
            timeout = int(st.session_state.get("ipa_timeout", IPA_DEFAULT_TIMEOUT) or IPA_DEFAULT_TIMEOUT)
            try:
                with st.spinner("Leyendo grados y cursos de IPA..."):
                    result = fetch_ipa_structure(
                        session_value=session_value,
                        base_url=base_url,
                        nivel_id=nivel_id,
                        timeout=timeout,
                        include_progress=False,
                        target_areas_only=False,
                    )
                    payload: Dict[str, object] = build_ipa_result_payload(result)
                    rows: List[Dict[str, object]] = build_ipa_result_rows(result)
            except Exception as exc:
                st.error(str(exc))
            else:
                st.session_state["ipa_last_payload"] = payload
                st.session_state["ipa_last_rows"] = rows
                st.session_state["ipa_last_progress_payload"] = {}
                st.session_state["ipa_last_planificaciones_payload"] = {}
                st.success(
                    f"IPA leido correctamente. Grados: {len(result.grades)} | Cursos: {len(rows)}."
                )

        _render_ipa_result_preview()
