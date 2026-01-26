import os
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import streamlit as st

from santillana_format.alumnos import DEFAULT_EMPRESA_ID
from santillana_format.duplicados import (
    BASE_SHEET_NAME,
    NUEVO_SHEET_NAME,
    build_comparacion_clave,
    build_comparacion_grado_seccion_diferente,
    build_comparacion_nombre,
    compare_alumnos,
    export_alumnos_excel,
    find_coincidencias_nombre_apellidos,
    read_alumnos_file,
    select_comparacion_basica,
    select_comparacion_con_grado,
)
from santillana_format.processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    process_excel,
)


GESTION_ESCOLAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases"
)
GESTION_ESCOLAR_CICLO_ID_DEFAULT = 207
COMPARTIR_BASE_URL = "https://compartirconocimientos-pe.santillana.com"
COMPARTIR_GROUPS_URL = (
    f"{COMPARTIR_BASE_URL}/api/front/school-admin/{{school_guid}}/groups"
)
COMPARTIR_GROUP_DETAIL_URL = (
    f"{COMPARTIR_BASE_URL}/api/front/school-admin/{{school_guid}}/groups/{{group_guid}}"
)
COMPARTIR_GROUP_COURSES_URL = (
    f"{COMPARTIR_BASE_URL}/api/front/school-admin/{{school_guid}}/groups/{{group_guid}}/courses"
)
COMPARTIR_EVALUATION_SCALES_URL = (
    f"{COMPARTIR_BASE_URL}/api/v3/school-admin/evaluation_scales"
)
COMPARTIR_EVALUATION_PERIOD_URL = (
    f"{COMPARTIR_BASE_URL}/api/v3/school-admin/evaluation_period"
)
COMPARTIR_EVALUATION_MODEL_URL = (
    f"{COMPARTIR_BASE_URL}/api/v3/school-admin/evaluation_model"
)
COMPARTIR_COURSE_CONFIG_URL = (
    f"{COMPARTIR_BASE_URL}/api/v3/course/{{course_guid}}/course_config"
)
COMPARTIR_SCHOOL_GUID_DEFAULT = "00000000-0000-1000-0000-000000004230"
COMPARTIR_SCHOOL_YEAR_GUID_DEFAULT = "46ce7910-dd64-11f0-9cdb-eb7c2dc331dd"
COMPARTIR_ANIO_DEFAULT = "2026"
COMPARTIR_PERIOD_KEY_DEFAULT = "annual"


st.set_page_config(page_title="Generador de Plantilla", layout="centered")
st.title("Generar Plantilla de Clases")
st.write("Elige si quieres crear clases, depurar alumnos o gestionar clases.")

tab_clases, tab_depurar, tab_clases_api, tab_compartir = st.tabs(
    ["Crear clases", "Depurar alumnos", "Clases API", "Escala Compartir"]
)


def _print_repetidos_console(df) -> None:
    if df.empty:
        return
    print("\nAlumnos repetidos (base vs nuevo):")
    clean = select_comparacion_basica(df)
    print(clean.to_string(index=False))


def _print_coincidencias_nombre_console(df) -> None:
    if df.empty:
        return
    print("\nCoincidencias por nombre y apellidos (base vs nuevo):")
    clean = select_comparacion_basica(df)
    print(clean.to_string(index=False))


def _print_diferencias_grado_console(df) -> None:
    if df.empty:
        return
    print("\nRepetidos con diferente grado/seccion (base vs nuevo):")
    clean = select_comparacion_con_grado(df)
    print(clean.to_string(index=False))


def _clean_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


def _compartir_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _parse_compartir_payload(response: requests.Response) -> Dict[str, object]:
    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = ""
        if isinstance(payload, dict):
            message = payload.get("message") or payload.get("error") or ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict):
        raise RuntimeError("Respuesta invalida (no es objeto)")

    status = payload.get("status")
    if status and str(status).lower() not in {"success", "ok"}:
        message = payload.get("message") or payload.get("error") or str(status)
        raise RuntimeError(message or "Respuesta invalida")

    return payload


def _fetch_compartir_groups(
    token: str,
    school_guid: str,
    school_year_guid: str,
    timeout: int,
    page_size: int = 100,
    check_config: bool = True,
) -> List[Dict[str, object]]:
    headers = _compartir_headers(token)
    url = COMPARTIR_GROUPS_URL.format(school_guid=school_guid)
    groups: List[Dict[str, object]] = []
    page = 0
    offset = 0
    page_size = max(1, int(page_size))

    while True:
        params = {
            "offset": offset,
            "page": page,
            "pageSize": page_size,
            "checkConfig": "true" if check_config else "false",
            "schoolYearGuid[]": [school_year_guid],
        }
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
        except requests.RequestException as exc:
            raise RuntimeError(f"Error de red: {exc}") from exc

        payload = _parse_compartir_payload(response)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Campo data no es objeto")
        school_groups = data.get("schoolGroups") or []
        if not isinstance(school_groups, list):
            raise RuntimeError("Campo schoolGroups no es lista")
        groups.extend(item for item in school_groups if isinstance(item, dict))

        left = data.get("left")
        if isinstance(left, str):
            left = int(left) if left.isdigit() else None
        if isinstance(left, (int, float)) and int(left) <= 0:
            break
        if not school_groups:
            break
        page += 1
        offset += page_size

    return groups


def _fetch_compartir_group_courses(
    token: str,
    school_guid: str,
    group_guid: str,
    timeout: int,
    check_config: bool = True,
) -> List[Dict[str, object]]:
    headers = _compartir_headers(token)
    url = COMPARTIR_GROUP_COURSES_URL.format(
        school_guid=school_guid, group_guid=group_guid
    )
    params = {"checkConfig": "true" if check_config else "false"}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    payload = _parse_compartir_payload(response)
    data = payload.get("data")
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")
    return [item for item in data if isinstance(item, dict)]


def _fetch_compartir_evaluation_scales(
    token: str, school_guid: str, timeout: int
) -> List[Dict[str, object]]:
    headers = _compartir_headers(token)
    params = {"school_guid": school_guid}
    try:
        response = requests.get(
            COMPARTIR_EVALUATION_SCALES_URL,
            headers=headers,
            params=params,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc
    payload = _parse_compartir_payload(response)
    data = payload.get("data")
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")
    return [item for item in data if isinstance(item, dict)]


def _fetch_compartir_evaluation_periods(
    token: str, school_guid: str, timeout: int
) -> List[Dict[str, object]]:
    headers = _compartir_headers(token)
    params = {"school_guid": school_guid}
    try:
        response = requests.get(
            COMPARTIR_EVALUATION_PERIOD_URL,
            headers=headers,
            params=params,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc
    payload = _parse_compartir_payload(response)
    data = payload.get("data")
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")
    return [item for item in data if isinstance(item, dict)]


def _fetch_compartir_evaluation_models(
    token: str, school_guid: str, timeout: int
) -> List[Dict[str, object]]:
    headers = _compartir_headers(token)
    params = {"school_guid": school_guid}
    try:
        response = requests.get(
            COMPARTIR_EVALUATION_MODEL_URL,
            headers=headers,
            params=params,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc
    payload = _parse_compartir_payload(response)
    data = payload.get("data")
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")
    return [item for item in data if isinstance(item, dict)]


def _build_compartir_config_payload(
    school_guid: str,
    scale_id: str,
    model: Optional[Dict[str, object]],
    period: Optional[Dict[str, object]],
    period_key: str,
    include_categories: bool,
) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "id": str(uuid.uuid4()),
        "school_guid": school_guid,
        "scale_id": scale_id,
    }

    if model and model.get("id"):
        payload["evaluation_model_id"] = model.get("id")

    if period:
        config_period: Dict[str, object] = {"id": str(uuid.uuid4())}
        if period_key:
            config_period["period_id"] = period_key
        elif period.get("id"):
            config_period["period_id"] = period.get("id")
        if period.get("id"):
            config_period["evaluation_period_id"] = period.get("id")
        if period.get("name"):
            config_period["name"] = period.get("name")
        if period.get("start_date"):
            config_period["start_date"] = period.get("start_date")
        if period.get("end_date"):
            config_period["end_date"] = period.get("end_date")
        if period.get("academic_session_id"):
            config_period["academic_session_id"] = period.get("academic_session_id")
        if period.get("academic_session_name"):
            config_period["academic_session_name"] = period.get("academic_session_name")
        payload["config_periods"] = [config_period]

    if include_categories and model:
        categories: List[Dict[str, object]] = []
        for item in model.get("categories") or []:
            if not isinstance(item, dict):
                continue
            category_id = item.get("category_id")
            scale_ref = item.get("evaluation_scales_id")
            weight = item.get("weight")
            if category_id is None and scale_ref is None and weight is None:
                continue
            entry: Dict[str, object] = {"id": str(uuid.uuid4())}
            if category_id is not None:
                entry["category_id"] = category_id
            if scale_ref is not None:
                entry["evaluation_scales_id"] = scale_ref
            if weight is not None:
                entry["weight"] = weight
            if item.get("evaluation_model_id"):
                entry["evaluation_model_id"] = item.get("evaluation_model_id")
            categories.append(entry)
        if categories:
            payload["config_categories"] = categories

    return payload


def _post_compartir_course_config(
    token: str, course_guid: str, payload: Dict[str, object], timeout: int
) -> Dict[str, object]:
    headers = _compartir_headers(token)
    url = COMPARTIR_COURSE_CONFIG_URL.format(course_guid=course_guid)
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc
    return _parse_compartir_payload(response)


def _compartir_option_label(item: Dict[str, object], name_key: str) -> str:
    name = str(item.get(name_key) or "").strip()
    item_id = str(item.get("id") or "").strip()
    if name and item_id:
        return f"{name} ({item_id})"
    return name or item_id or "(sin nombre)"


def _find_first_index(
    items: List[Dict[str, object]], predicate
) -> int:
    for idx, item in enumerate(items):
        if predicate(item):
            return idx
    return 0


def _period_matches_year(period: Dict[str, object], year_label: str) -> bool:
    label = str(year_label or "").strip()
    if not label:
        return False
    if label in str(period.get("academic_session_name") or ""):
        return True
    if label in str(period.get("name") or ""):
        return True
    return False


def _collect_compartir_courses(
    token: str,
    school_guid: str,
    groups: List[Dict[str, object]],
    timeout: int,
    check_config: bool = True,
) -> Tuple[List[Dict[str, object]], List[str]]:
    courses: List[Dict[str, object]] = []
    errors: List[str] = []
    for group in groups:
        group_guid = group.get("guid")
        if not group_guid:
            errors.append("Grupo sin guid.")
            continue
        group_name = str(group.get("name") or "")
        try:
            group_courses = _fetch_compartir_group_courses(
                token=token,
                school_guid=school_guid,
                group_guid=str(group_guid),
                timeout=timeout,
                check_config=check_config,
            )
        except Exception as exc:
            errors.append(f"{group_name or group_guid}: {exc}")
            continue
        for course in group_courses:
            courses.append(
                {
                    "course_guid": course.get("guid"),
                    "course_name": course.get("name") or "",
                    "group_guid": group_guid,
                    "group_name": group_name,
                    "has_config": course.get("has_config"),
                }
            )
    return courses, errors


def _fetch_clases_gestion_escolar(
    token: str, colegio_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id)
    try:
        response = requests.get(
            url, headers=headers, params={"colegioId": colegio_id}, timeout=timeout
        )
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

    data = payload.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")
    return data


def _delete_clase_gestion_escolar(
    token: str, clase_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> None:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id)
    url = f"{url}/{clase_id}"
    try:
        response = requests.delete(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    if not response.ok:
        status_code = response.status_code
        try:
            payload = response.json()
            message = payload.get("message") if isinstance(payload, dict) else ""
        except ValueError:
            message = ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if response.content:
        try:
            payload = response.json()
        except ValueError:
            return
        if isinstance(payload, dict) and payload.get("success") is False:
            message = payload.get("message") or "Respuesta invalida"
            raise RuntimeError(message)


def _collect_colegios(clases: List[Dict[str, object]]) -> List[Dict[str, object]]:
    colegios: Dict[int, str] = {}
    for item in clases:
        cnc = item.get("colegioNivelCiclo") if isinstance(item, dict) else None
        colegio = cnc.get("colegio") if isinstance(cnc, dict) else None
        if isinstance(colegio, dict):
            colegio_id = colegio.get("colegioId")
            colegio_nombre = colegio.get("colegio", "")
            if colegio_id is not None:
                colegios[int(colegio_id)] = str(colegio_nombre or "")
    return [
        {"colegioId": colegio_id, "colegio": nombre}
        for colegio_id, nombre in sorted(colegios.items())
    ]


with tab_clases:
    with st.expander("Opciones de entrada", expanded=True):
        uploaded_excel = st.file_uploader(
            "Excel de entrada",
            type=["xlsx"],
            help="Ejemplo: PreOnboarding_Detalle_20251212.xlsx",
        )
        col1, col2 = st.columns(2)
        codigo = col1.text_input("Codigo (CRM)", placeholder="00001053")
        columna_codigo = col2.text_input(
            "Columna de codigo",
            value=CODE_COLUMN_NAME,
            help="Nombre de la columna donde buscar el codigo",
        )
        hoja = col1.text_input("Hoja a leer", value=SHEET_NAME, help="Nombre de la hoja")

    if st.button("Generar", type="primary"):
        if not uploaded_excel:
            st.error("Sube un Excel de entrada.")
            st.stop()
        if not codigo.strip():
            st.error("Ingresa un codigo.")
            st.stop()

        excel_bytes = uploaded_excel.read()
        plantilla_path = Path(OUTPUT_FILENAME) if Path(OUTPUT_FILENAME).exists() else None

        try:
            with st.spinner("Procesando..."):
                output_bytes, summary = process_excel(
                    excel_bytes,
                    codigo=codigo,
                    columna_codigo=columna_codigo,
                    hoja=hoja,
                    plantilla_path=plantilla_path,
                )
            st.success(
                f"Listo. Filtradas: {summary['filas_filtradas']}, Salida: {summary['filas_salida']} filas."
            )
            download_name = f"{Path(OUTPUT_FILENAME).stem}_{codigo}.xlsx"
            st.download_button(
                label="Descargar Excel",
                data=output_bytes,
                file_name=download_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")


with tab_depurar:
    st.subheader("Depurar alumnos")
    st.write(
        "Sube el archivo base (alumnos actuales) y el archivo nuevo (alumnos a cargar)."
    )
    st.caption(
        "Se leen las hojas '{base}' y '{nuevo}'. Comparacion por Apellido Paterno + Apellido Materno + "
        "Nombre (hasta 4 nombres) + Fecha de Nacimiento + Grado + Seccion.".format(
            base=BASE_SHEET_NAME,
            nuevo=NUEVO_SHEET_NAME,
        )
    )
    col1, col2 = st.columns(2)
    uploaded_base = col1.file_uploader(
        f"Archivo base ({BASE_SHEET_NAME})",
        type=["xlsx", "csv"],
        key="base_alumnos",
    )
    uploaded_nuevo = col2.file_uploader(
        f"Archivo nuevo ({NUEVO_SHEET_NAME})",
        type=["xlsx", "csv"],
        key="nuevo_alumnos",
    )

    if st.button("Comparar y filtrar", type="primary"):
        if not uploaded_base or not uploaded_nuevo:
            st.error("Sube ambos archivos.")
            st.stop()

        try:
            base_bytes = uploaded_base.read()
            nuevo_bytes = uploaded_nuevo.read()
            df_base = read_alumnos_file(
                base_bytes,
                uploaded_base.name,
                sheet_name=BASE_SHEET_NAME,
            )
            df_nuevo = read_alumnos_file(
                nuevo_bytes,
                uploaded_nuevo.name,
                sheet_name=NUEVO_SHEET_NAME,
            )
            repetidos, filtrados, summary = compare_alumnos(df_base, df_nuevo)
            coincidencias_nombre = find_coincidencias_nombre_apellidos(
                df_base, df_nuevo
            )
            comparacion_clave = build_comparacion_clave(df_base, df_nuevo)
            comparacion_nombre = build_comparacion_nombre(df_base, df_nuevo)
            comparacion_diferente = build_comparacion_grado_seccion_diferente(
                df_base, df_nuevo
            )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
            st.stop()

        _print_repetidos_console(comparacion_clave)
        _print_coincidencias_nombre_console(comparacion_nombre)
        _print_diferencias_grado_console(comparacion_diferente)
        st.success(
            "Listo. Base: {base_total}, Nuevo: {nuevo_total}, Repetidos: {repetidos}, Sin repetir: {sin_repetir}.".format(
                **summary
            )
        )
        if summary["base_sin_clave"] or summary["nuevo_sin_clave"]:
            st.warning(
                "Filas sin clave de comparacion. Base: {base}, Nuevo: {nuevo}.".format(
                    base=summary["base_sin_clave"],
                    nuevo=summary["nuevo_sin_clave"],
                )
            )

        st.write("Alumnos repetidos (en archivo nuevo):")
        if repetidos.empty:
            st.info("No se encontraron alumnos repetidos.")
        else:
            st.dataframe(repetidos, use_container_width=True)

        st.write("Coincidencias por nombre y apellidos (base vs nuevo):")
        if coincidencias_nombre.empty:
            st.info("No se encontraron coincidencias por nombre y apellidos.")
        else:
            st.dataframe(coincidencias_nombre, use_container_width=True)

        output_bytes = export_alumnos_excel(filtrados)
        output_name = f"{Path(uploaded_nuevo.name).stem}_sin_repetidos.xlsx"
        st.download_button(
            label="Descargar archivo sin repetidos",
            data=output_bytes,
            file_name=output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


with tab_clases_api:
    st.subheader("Listar y eliminar clases")
    st.write("Lista y elimina clases del API de gestion escolar.")
    token_input = st.text_input("Token (Bearer)", type="password")
    colegio_id = st.number_input("Colegio Clave", min_value=1, step=1, format="%d")
    with st.expander("Opciones avanzadas", expanded=False):
        ciclo_id = st.number_input(
            "Ciclo ID",
            min_value=1,
            step=1,
            value=GESTION_ESCOLAR_CICLO_ID_DEFAULT,
            format="%d",
        )

    token = _clean_token(token_input)
    if not token:
        token = _clean_token(os.environ.get("PEGASUS_TOKEN", ""))
    empresa_id = DEFAULT_EMPRESA_ID
    timeout = 30

    col_list, col_delete = st.columns(2)
    if col_list.button("Listar clases"):
        if not token:
            st.error("Falta el token. Usa el input o la variable de entorno.")
        else:
            try:
                clases = _fetch_clases_gestion_escolar(
                    token=token,
                    colegio_id=int(colegio_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:  # pragma: no cover - UI
                st.error(f"Error: {exc}")
            else:
                if not clases:
                    st.info("No se encontraron clases.")
                else:
                    tabla = [
                        {
                            "ID": item.get("geClaseId"),
                            "Clase": item.get("geClase")
                            or item.get("geClaseClave")
                            or "",
                        }
                        for item in clases
                        if isinstance(item, dict)
                    ]
                    st.write(f"Clases encontradas: {len(tabla)}")
                    st.dataframe(tabla, use_container_width=True)

    confirm_delete = st.checkbox("Confirmo eliminar todas las clases listadas.")
    if col_delete.button("Eliminar clases"):
        if not token:
            st.error("Falta el token. Usa el input o la variable de entorno.")
            st.stop()
        if not confirm_delete:
            st.error("Debes confirmar antes de eliminar.")
            st.stop()
        try:
            clases = _fetch_clases_gestion_escolar(
                token=token,
                colegio_id=int(colegio_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
            st.stop()

        if not clases:
            st.info("No se encontraron clases.")
            st.stop()

        errores: List[str] = []
        for item in clases:
            clase_id = item.get("geClaseId") if isinstance(item, dict) else None
            if clase_id is None:
                errores.append("Clase sin geClaseId.")
                continue
            try:
                _delete_clase_gestion_escolar(
                    token=token,
                    clase_id=int(clase_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:  # pragma: no cover - UI
                errores.append(f"{clase_id}: {exc}")

        colegios = _collect_colegios(clases)
        if colegios:
            st.write("Colegios eliminados (id, nombre):")
            st.dataframe(colegios, use_container_width=True)
        eliminadas = len(clases) - len(errores)
        st.success(f"Clases eliminadas: {eliminadas}")
        if errores:
            st.error("Errores al eliminar:")
            st.write("\n".join(f"- {item}" for item in errores))
