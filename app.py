import os
import re
import tempfile
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st

from santillana_format.alumnos import (
    DEFAULT_CICLO_ID as ALUMNOS_CICLO_ID_DEFAULT,
    DEFAULT_EMPRESA_ID,
    descargar_plantilla_edicion_masiva,
)
from santillana_format.alumnos_compare import comparar_plantillas
from santillana_format.processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    process_excel,
)
from santillana_format.profesores import (
    DEFAULT_CICLO_ID as PROFESORES_CICLO_ID_DEFAULT,
    export_profesores_excel,
    listar_profesores_data,
)
from santillana_format.profesores_clases import asignar_profesores_clases
from santillana_format.profesores_password import actualizar_passwords_docentes


GESTION_ESCOLAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases"
)
GESTION_ESCOLAR_ALUMNOS_CLASE_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases/{clase_id}/alumnos"
)
CENSO_ALUMNOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnos"
)
CENSO_NIVELES_GRADOS_GRUPOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnos/nivelesGradosGrupos"
)
CENSO_PLANTILLA_EDICION_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/descargarPlantillaEdicionMasiva"
)
GESTION_ESCOLAR_CICLO_ID_DEFAULT = 207


st.set_page_config(page_title="Generador de Plantilla", layout="centered")
st.title("Si estas acá es porque eres flojo")
st.write("El maravilloso mundo de TED :0 automatiza tu chamba por un buenos dias al dia ;)")

tab_clases, tab_profesores_clases, tab_alumnos, tab_clases_api, tab_clases_alumnos = st.tabs(
    [
        "Crear clases",
        "Profesores con clases",
        "Alumnos registrados",
        "Clases API",
        "Clases + alumnos",
    ]
)


def _clean_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


def _parse_persona_ids(raw: str) -> List[int]:
    if not raw:
        return []
    tokens = re.split(r"[,\s;]+", raw.strip())
    ids: List[int] = []
    invalid: List[str] = []
    for token in tokens:
        if not token:
            continue
        try:
            ids.append(int(token))
        except ValueError:
            invalid.append(token)
    if invalid:
        raise ValueError(f"IDs invalidos: {', '.join(invalid)}")
    unique: List[int] = []
    seen = set()
    for value in ids:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def _parse_colegio_id(raw: object, field_name: str = "Colegio Clave") -> int:
    text = str(raw or "").strip()
    if not text:
        raise ValueError(f"{field_name} es obligatorio.")
    compact = re.sub(r"\s+", "", text)
    if not compact.isdigit():
        raise ValueError(
            f"{field_name} invalido: '{text}'. Usa un ID numerico (ej: 2326)."
        )
    value = int(compact)
    if value <= 0:
        raise ValueError(f"{field_name} invalido: '{text}'. Debe ser mayor a 0.")
    return value


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


def _fetch_alumnos_clase_gestion_escolar(
    token: str, clase_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> Dict[str, object]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_ALUMNOS_CLASE_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        clase_id=clase_id,
    )
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
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


def _fetch_niveles_grados_grupos_censo(
    token: str, colegio_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = CENSO_NIVELES_GRADOS_GRUPOS_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
    )
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
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
    niveles = data.get("niveles") or []
    if not isinstance(niveles, list):
        raise RuntimeError("Campo data.niveles no es lista")
    return niveles


def _fetch_alumnos_censo(
    token: str,
    colegio_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = CENSO_ALUMNOS_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
    )
    params = {"nivelId": nivel_id, "gradoId": grado_id, "grupoId": grupo_id}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
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


def _fetch_login_password_lookup_censo(
    token: str, colegio_id: int, empresa_id: int, ciclo_id: int, timeout: int
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = CENSO_PLANTILLA_EDICION_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        colegio_id=colegio_id,
    )
    try:
        response = requests.get(url, headers=headers, params={"descargar": 0}, timeout=timeout)
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

    by_alumno_id: Dict[str, Dict[str, str]] = {}
    by_persona_id: Dict[str, Dict[str, str]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        login = str(item.get("login") or "").strip()
        password = str(item.get("password") or "").strip()
        alumno_id = str(item.get("alumnoId") or "").strip()
        persona_id = str(item.get("personaId") or "").strip()

        if login or password:
            value = {"login": login, "password": password}
            if alumno_id:
                by_alumno_id[alumno_id] = value
            if persona_id:
                by_persona_id[persona_id] = value

    return by_alumno_id, by_persona_id


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "si", "sÃ­", "yes"}
    return False


def _resolve_alumno_login_password(
    item: Dict[str, object],
    by_alumno_id: Dict[str, Dict[str, str]],
    by_persona_id: Dict[str, Dict[str, str]],
) -> Tuple[str, str]:
    persona = item.get("persona") if isinstance(item.get("persona"), dict) else {}
    login = ""
    password = str(item.get("password") or "").strip()

    persona_login = persona.get("personaLogin") if isinstance(persona, dict) else None
    if isinstance(persona_login, dict):
        login = str(persona_login.get("login") or "").strip()
    if not login:
        login = str(item.get("login") or "").strip()

    alumno_id = str(item.get("alumnoId") or "").strip()
    persona_id = str(persona.get("personaId") or item.get("personaId") or "").strip()

    if (not login or not password) and alumno_id and alumno_id in by_alumno_id:
        lookup = by_alumno_id[alumno_id]
        if not login:
            login = str(lookup.get("login") or "").strip()
        if not password:
            password = str(lookup.get("password") or "").strip()

    if (not login or not password) and persona_id and persona_id in by_persona_id:
        lookup = by_persona_id[persona_id]
        if not login:
            login = str(lookup.get("login") or "").strip()
        if not password:
            password = str(lookup.get("password") or "").strip()

    return login, password


def _to_int_or_default(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _grupo_sort_key(grupo_clave: str, grupo_nombre: str) -> Tuple[int, str]:
    clave = (grupo_clave or "").strip().upper()
    if len(clave) == 1 and clave.isalpha():
        return 0, clave
    return 1, (grupo_nombre or "").strip().upper()


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
    st.info(
        "Flujo sugerido: 1) Sube el Excel de detalle, 2) Ingresa el codigo CRM "
        "exacto (respeta ceros), 3) Define secciones (A,B,C...)."
    )
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
        grupos = col2.text_input(
            "Secciones (A,B,C,D)",
            value="A",
            help="Letras separadas por coma para crear secciones.",
        )

    if st.button("Generar", type="primary"):
        if not uploaded_excel:
            st.error("Sube un Excel de entrada.")
            st.stop()
        if not codigo.strip():
            st.error("Ingresa un codigo.")
            st.stop()
        if not grupos.strip():
            st.error("Ingresa las secciones (A,B,C,D).")
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
                    grupos=grupos,
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


with tab_profesores_clases:
    st.subheader("Profesores con clases")
    st.write(
        "Genera el Excel base de profesores (activos e inactivos) y luego asignalos a clases."
    )
    st.warning(
        "Recomendacion: primero usa 'Simular' para revisar el resumen antes de aplicar "
        "cambios reales."
    )

    col1, col2 = st.columns(2)
    token_input = col1.text_input(
        "Token (sin Bearer)",
        type="password",
        key="profesores_token",
        help="Pega el token JWT sin el prefijo 'Bearer '.",
    )
    colegio_id_raw = col2.text_input(
        "Colegio Clave",
        key="profesores_colegio_text",
        placeholder="2326",
        help="Acepta texto o numero. Debe ser un ID numerico de colegio.",
    )

    with st.expander("Opciones avanzadas", expanded=False):
        ciclo_id = st.number_input(
            "Ciclo ID",
            min_value=1,
            step=1,
            value=PROFESORES_CICLO_ID_DEFAULT,
            format="%d",
            key="profesores_ciclo",
        )
        timeout = st.number_input(
            "Timeout (seg)",
            min_value=5,
            step=5,
            value=30,
            format="%d",
            key="profesores_timeout",
        )

    st.subheader("Generar Excel base de profesores (activos e inactivos)")
    st.write(
        "Crea un Excel listo con columnas Id, Nombre, Apellido, Estado, Sexo, DNI, "
        "E-mail, Login y Password (en blanco)."
    )
    persona_ids_raw = st.text_input(
        "Filtrar por personaId (opcional, separado por coma)",
        key="profesores_ids",
    )
    if st.button("Generar Excel base", type="primary", key="profesores_generar"):
        token = _clean_token(token_input)
        if not token:
            token = _clean_token(os.environ.get("PEGASUS_TOKEN", ""))
        if not token:
            st.error("Falta el token. Usa el input o la variable de entorno.")
            st.stop()
        try:
            colegio_id_int = _parse_colegio_id(colegio_id_raw)
        except ValueError as exc:
            st.error(f"Error: {exc}")
            st.stop()
        try:
            persona_ids = _parse_persona_ids(persona_ids_raw)
        except ValueError as exc:
            st.error(f"Error: {exc}")
            st.stop()
        try:
            data, summary, errores = listar_profesores_data(
                token=token,
                colegio_id=colegio_id_int,
                empresa_id=DEFAULT_EMPRESA_ID,
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
            st.stop()

        if persona_ids:
            data = [
                item for item in data if int(item.get("persona_id", 0)) in persona_ids
            ]

        filas: List[Dict[str, object]] = []
        for entry in data:
            dni = entry.get("dni", "") or ""
            email = entry.get("email", "") or ""
            login = entry.get("login", "") or email
            filas.append(
                {
                    "Id": entry.get("persona_id", ""),
                    "Nombre": entry.get("nombre", ""),
                    "Apellido Paterno": entry.get("apellido_paterno", ""),
                    "Apellido Materno": entry.get("apellido_materno", ""),
                    "Estado": entry.get("estado", ""),
                    "Sexo": entry.get("sexo", ""),
                    "DNI": dni,
                    "E-mail": email,
                    "Login": login,
                    "Password": "",
                    "Inicial": "",
                    "Primaria": "",
                    "Secundaria": "",
                    "I3": "",
                    "I4": "",
                    "I5": "",
                    "P1": "",
                    "P2": "",
                    "P3": "",
                    "P4": "",
                    "P5": "",
                    "P6": "",
                    "S1": "",
                    "S2": "",
                    "S3": "",
                    "S4": "",
                    "S5": "",
                    "Clases": "",
                    "Secciones": "",
                }
            )

        if not filas:
            st.warning("No se encontraron profesores para generar el Excel.")
        else:
            output_bytes = export_profesores_excel(filas)
            file_name = f"profesores_base_{colegio_id_int}.xlsx"
            st.session_state["profesores_excel_base"] = output_bytes
            st.session_state["profesores_excel_base_name"] = file_name
            st.success(
                "Excel base listo. Profesores: {profesores_total}, Errores detalle: {detalle_error}.".format(
                    **summary
                )
            )
            if errores:
                st.error("Errores al obtener profesores:")
                st.dataframe(errores, use_container_width=True)

    if st.session_state.get("profesores_excel_base"):
        st.download_button(
            label="Descargar Excel base",
            data=st.session_state["profesores_excel_base"],
            file_name=st.session_state["profesores_excel_base_name"],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.subheader("Asignar profesores a clases")
    st.write(
        "Sube un Excel con columnas persona_id y CURSO, mas niveles/grados "
        "(Inicial/Primaria/Secundaria o I3, P1, S2...). "
        "Opcional: columna Secciones con valores como 1PA,2PB,3SB para asignar solo esas secciones. "
        "Si incluyes la columna Estado (Activo/Inactivo), se sincroniza por nivel al aplicar."
    )
    st.markdown("**Procesos a ejecutar**")
    st.caption(
        "Niveles = acceso por nivel (Inicial/Primaria/Secundaria). "
        "Clases/Secciones = asignacion directa a clases especificas (incluye grupos)."
    )
    col_proc1, col_proc2 = st.columns(2)
    do_password = col_proc1.checkbox("Actualizar login/password", value=True)
    do_niveles = col_proc1.checkbox("Asignar niveles (asignarNivel)", value=True)
    do_estado = col_proc1.checkbox("Activar/Inactivar (Estado)", value=True)
    do_clases = col_proc2.checkbox("Asignar clases y secciones", value=True)
    remove_missing = col_proc2.checkbox(
        "Eliminar profesores que no estan en el Excel (solo clases evaluadas)",
        value=False,
        key="profesores_remove",
        disabled=not do_clases,
    )
    if remove_missing and do_clases:
        st.warning(
            "Eliminar profesores quita asignaciones en las clases evaluadas. "
            "Revisa el Excel antes de aplicar."
        )
    uploaded_profesores = st.file_uploader(
        "Excel de profesores",
        type=["xlsx", "csv", "txt"],
        key="profesores_excel",
    )
    sheet_name = st.text_input(
        "Hoja (opcional)",
        value="Profesores_clases",
        help="Nombre de la hoja. Si lo dejas en blanco se intentara usar Profesores_clases.",
    )
    confirm_apply = st.checkbox(
        "Confirmo aplicar cambios",
        value=False,
        key="profesores_confirm_apply",
    )

    col_run, col_apply = st.columns(2)
    run_sim = col_run.button("Simular", type="primary", key="profesores_simular")
    run_apply = col_apply.button(
        "Aplicar cambios", type="secondary", key="profesores_apply"
    )
    st.info(
        "Para aplicar cambios debes marcar 'Confirmo aplicar cambios'. "
        "Si no confirmas, se ejecuta en modo simulacion."
    )

    if run_sim or run_apply:
        if not uploaded_profesores:
            st.error("Sube un Excel de profesores.")
            st.stop()

        token = _clean_token(token_input)
        if not token:
            token = _clean_token(os.environ.get("PEGASUS_TOKEN", ""))
        if not token:
            st.error("Falta el token. Usa el input o la variable de entorno.")
            st.stop()
        try:
            colegio_id_int = _parse_colegio_id(colegio_id_raw)
        except ValueError as exc:
            st.error(f"Error: {exc}")
            st.stop()
        if run_apply and not confirm_apply:
            st.error("Debes confirmar antes de aplicar cambios.")
            st.stop()
        if not any([do_password, do_niveles, do_estado, do_clases]):
            st.error("Selecciona al menos un proceso.")
            st.stop()

        suffix = Path(uploaded_profesores.name).suffix or ".xlsx"
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(uploaded_profesores.read())
                tmp_path = Path(tmp.name)

            logs: List[str] = []

            def _on_log(line: str) -> None:
                logs.append(line)

            progress = st.progress(0)
            status = st.empty()

            def _on_progress(phase: str, current: int, total: int, message: str) -> None:
                percent = int((current / total) * 100) if total else 0
                progress.progress(percent)
                status.write(f"{phase}: {message} ({current}/{total})")

            if do_password:
                pwd_summary, pwd_warnings, pwd_errors = actualizar_passwords_docentes(
                    token=token,
                    colegio_id=colegio_id_int,
                    excel_path=tmp_path,
                    sheet_name=sheet_name.strip() or None,
                    empresa_id=DEFAULT_EMPRESA_ID,
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                    dry_run=not run_apply,
                    on_progress=lambda current, total, msg: _on_progress(
                        "passwords", current, total, msg
                    ),
                )
                st.info(
                    "Passwords -> Docentes: {docentes_total}, Niveles: {niveles_total}, "
                    "Actualizaciones: {actualizaciones}, Errores API: {errores_api}.".format(
                        **pwd_summary
                    )
                )
                if pwd_warnings:
                    st.warning("Warnings passwords:")
                    st.markdown("\n".join(f"- {item}" for item in pwd_warnings))
                if pwd_errors:
                    st.error("Errores passwords:")
                    st.dataframe(pwd_errors, use_container_width=True)

            run_asignacion = any([do_niveles, do_estado, do_clases])
            if run_asignacion:
                summary, warnings, errors = asignar_profesores_clases(
                    token=token,
                    empresa_id=DEFAULT_EMPRESA_ID,
                    ciclo_id=int(ciclo_id),
                    colegio_id=colegio_id_int,
                    excel_path=tmp_path,
                    sheet_name=sheet_name.strip() or None,
                    timeout=int(timeout),
                    dry_run=not run_apply,
                    remove_missing=remove_missing if do_clases else False,
                    on_log=_on_log,
                    on_progress=_on_progress,
                    do_niveles=do_niveles,
                    do_estado=do_estado,
                    do_clases=do_clases,
                    do_grupos=do_clases,
                )
            else:
                summary, warnings, errors = {}, [], []
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
            st.stop()
        finally:
            if tmp_path:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

        if summary:
            resumen = [
                f"Docentes: {summary.get('docentes_procesados', 0)}",
                f"Omitidos (no colegio): {summary.get('docentes_omitidos_no_colegio', 0)}",
                f"Sin match: {summary.get('docentes_sin_match', 0)}",
                f"Clases: {summary.get('clases_encontradas', 0)}",
                f"Asignaciones nuevas: {summary.get('asignaciones_nuevas', 0)}",
                f"Asig. omitidas: {summary.get('asignaciones_omitidas', 0)}",
                f"Grupos asignados: {summary.get('grupos_asignados', 0)}",
                f"Grupos omitidos: {summary.get('grupos_omitidos', 0)}",
                f"Eliminaciones: {summary.get('eliminaciones', 0)}",
                f"Estado activaciones: {summary.get('estado_activaciones', 0)}",
                f"Estado inactivaciones: {summary.get('estado_inactivaciones', 0)}",
                f"Estado omitidas: {summary.get('estado_omitidas', 0)}",
                f"Errores API: {summary.get('errores_api', 0)}",
            ]
            st.success("Resumen de ejecucion")
            st.markdown("\n".join(f"- {item}" for item in resumen))
            if warnings:
                st.warning("Advertencias:")
                st.markdown("\n".join(f"- {item}" for item in warnings))
            if errors:
                st.error("Errores al asignar profesores:")
                st.dataframe(errors, use_container_width=True)
            if logs:
                display_logs = [line for line in logs if line is not None]
                while display_logs and not str(display_logs[0]).strip():
                    display_logs.pop(0)
                while display_logs and not str(display_logs[-1]).strip():
                    display_logs.pop()
                st.text_area(
                    "Log de ejecucion",
                    value="\n".join(display_logs),
                    height=300,
                )
        else:
            st.success("Listo. Solo se procesaron passwords.")

with tab_alumnos:
    st.subheader("Plantilla de alumnos registrados")
    st.write(
        "Descarga la plantilla de edicion masiva con alumnos ya registrados, "
        "ordenada por nivel (Inicial, Primaria, Secundaria), grado y grupo (A,B,C...)."
    )
    st.info("Usa esta plantilla como base oficial de alumnos registrados.")
    col1, col2 = st.columns(2)
    token_input = col1.text_input(
        "Token (sin Bearer)",
        type="password",
        key="alumnos_token",
        help="Pega el token JWT sin el prefijo 'Bearer '.",
    )
<<<<<<< HEAD
    colegio_id_raw = col2.text_input(
        "Colegio Clave",
        key="alumnos_colegio_text",
        placeholder="2326",
        help="Acepta texto o numero. Debe ser un ID numerico de colegio.",
    )
=======
    colegio_id = col2.text_input(
    "Colegio Clave",
    key="alumnos_colegio",
    placeholder="Ejemplo: PAMHU.PE"
)
>>>>>>> a7f0970f2fd63244192fed87dd5c0d129d2a2018
    with st.expander("Opciones avanzadas", expanded=False):
        ciclo_id = st.number_input(
            "Ciclo ID",
            min_value=1,
            step=1,
            value=ALUMNOS_CICLO_ID_DEFAULT,
            format="%d",
            key="alumnos_ciclo",
        )
        empresa_id = st.number_input(
            "Empresa ID",
            min_value=1,
            step=1,
            value=DEFAULT_EMPRESA_ID,
            format="%d",
            key="alumnos_empresa",
        )
        timeout = st.number_input(
            "Timeout (seg)",
            min_value=5,
            step=5,
            value=30,
            format="%d",
            key="alumnos_timeout",
        )

    if st.button("Descargar plantilla", type="primary", key="alumnos_descargar"):
        token = _clean_token(token_input)
        if not token:
            token = _clean_token(os.environ.get("PEGASUS_TOKEN", ""))
        if not token:
            st.error("Falta el token. Usa el input o la variable de entorno.")
            st.stop()
        try:
            colegio_id_int = _parse_colegio_id(colegio_id_raw)
        except ValueError as exc:
            st.error(f"Error: {exc}")
            st.stop()
        try:
            with st.spinner("Descargando plantilla..."):
                output_bytes, summary = descargar_plantilla_edicion_masiva(
                    token=token,
<<<<<<< HEAD
                    colegio_id=colegio_id_int,
=======
                    colegio_id=colegio_id,
>>>>>>> a7f0970f2fd63244192fed87dd5c0d129d2a2018
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
            st.stop()

<<<<<<< HEAD
        file_name = f"plantilla_edicion_alumnos_{colegio_id_int}.xlsx"
=======
        file_name = f"plantilla_edicion_alumnos_{colegio_id}.xlsx"
>>>>>>> a7f0970f2fd63244192fed87dd5c0d129d2a2018
        st.success(f"Listo. Alumnos: {summary['alumnos_total']}.")
        st.download_button(
            label="Descargar plantilla",
            data=output_bytes,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.divider()
    st.subheader("Comparar Plantilla_BD vs Plantilla_Actualizada")
    st.write(
        "Sube el Excel descargado (con hojas Plantilla_BD y Plantilla_Actualizada) "
        "y se generan dos hojas: 'Plantilla alta de alumnos' y 'Plantilla edición masiva'."
    )
    st.warning(
        "Asegurate de que el archivo tenga ambas hojas con nombres exactos: "
        "Plantilla_BD y Plantilla_Actualizada."
    )
    uploaded_compare = st.file_uploader(
        "Excel con Plantilla_BD y Plantilla_Actualizada",
        type=["xlsx"],
        key="alumnos_compare_excel",
    )
    if st.button("Generar alumnos_resultados", type="primary", key="alumnos_compare"):
        if not uploaded_compare:
            st.error("Sube un Excel .xlsx con Plantilla_BD y Plantilla_Actualizada.")
            st.stop()
        suffix = Path(uploaded_compare.name).suffix or ".xlsx"
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(uploaded_compare.read())
                tmp_path = Path(tmp.name)
            output_bytes, summary = comparar_plantillas(excel_path=tmp_path)
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
            st.stop()
        finally:
            if tmp_path:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

        st.success(
            "Listo. Base: {base_total}, Actualizada: {actualizados_total}, "
            "Match NUIP: {nuip_match}, Nuevos: {nuevos_total}.".format(**summary)
        )
        download_name = f"alumnos_resultados_{Path(uploaded_compare.name).stem}.xlsx"
        st.download_button(
            label="Descargar alumnos_resultados",
            data=output_bytes,
            file_name=download_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

with tab_clases_api:
    st.subheader("Listar y eliminar clases")
    st.write("Lista y elimina clases del API de gestion escolar.")
    st.warning(
        "Eliminar clases es irreversible. Revisa el listado antes de confirmar."
    )
    token_input = st.text_input(
        "Token (sin Bearer)",
        type="password",
        help="Pega el token JWT sin el prefijo 'Bearer '.",
    )
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

with tab_clases_alumnos:
    st.subheader("Listar clases con todos sus alumnos")
    st.write(
        "Consulta todas las clases del colegio y muestra todos los alumnos asociados "
        "a cada clase."
    )

    col1, col2 = st.columns(2)
    token_input = col1.text_input(
        "Token (sin Bearer)",
        type="password",
        key="clases_alumnos_token",
        help="Pega el token JWT sin el prefijo 'Bearer '.",
    )
    colegio_id = col2.number_input(
        "Colegio Clave",
        min_value=1,
        step=1,
        format="%d",
        key="clases_alumnos_colegio",
    )

    with st.expander("Opciones avanzadas", expanded=False):
        ciclo_id = st.number_input(
            "Ciclo ID",
            min_value=1,
            step=1,
            value=GESTION_ESCOLAR_CICLO_ID_DEFAULT,
            format="%d",
            key="clases_alumnos_ciclo",
        )
        empresa_id = st.number_input(
            "Empresa ID",
            min_value=1,
            step=1,
            value=DEFAULT_EMPRESA_ID,
            format="%d",
            key="clases_alumnos_empresa",
        )
        timeout = st.number_input(
            "Timeout (seg)",
            min_value=5,
            step=5,
            value=30,
            format="%d",
            key="clases_alumnos_timeout",
        )
        solo_activos = st.checkbox(
            "Solo alumnos activos",
            value=False,
            key="clases_alumnos_solo_activos",
        )

    if st.button(
        "Listar clases con alumnos",
        type="primary",
        key="clases_alumnos_listar",
    ):
        token = _clean_token(token_input)
        if not token:
            token = _clean_token(os.environ.get("PEGASUS_TOKEN", ""))
        if not token:
            st.error("Falta el token. Usa el input o la variable de entorno.")
            st.stop()

        try:
            with st.spinner("Listando clases..."):
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
            st.info("No se encontraron clases para ese colegio/ciclo.")
            st.stop()

        detalle_rows: List[Dict[str, object]] = []
        resumen_rows: List[Dict[str, object]] = []
        errores: List[str] = []
        total = len(clases)
        progress = st.progress(0)
        status = st.empty()

        for index, item in enumerate(clases, start=1):
            progress.progress(int((index / total) * 100))

            if not isinstance(item, dict):
                errores.append("Clase con formato invalido.")
                continue

            clase_id_raw = item.get("geClaseId")
            if clase_id_raw is None:
                errores.append("Clase sin geClaseId.")
                continue
            try:
                clase_id = int(clase_id_raw)
            except (TypeError, ValueError):
                errores.append(f"Clase con geClaseId invalido: {clase_id_raw}")
                continue

            clase_name = str(item.get("geClase") or item.get("geClaseClave") or "")
            status.write(f"Revisando {index}/{total}: {clase_id} {clase_name}".strip())

            try:
                clase_data = _fetch_alumnos_clase_gestion_escolar(
                    token=token,
                    clase_id=clase_id,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:  # pragma: no cover - UI
                errores.append(f"{clase_id}: {exc}")
                continue

            alumnos_data = clase_data.get("claseAlumnos") or []
            if not isinstance(alumnos_data, list):
                errores.append(f"{clase_id}: campo claseAlumnos no es lista")
                continue

            cgg = clase_data.get("colegioGradoGrupo") if isinstance(clase_data, dict) else None
            grado_info = cgg.get("grado") if isinstance(cgg, dict) else None
            grupo_info = cgg.get("grupo") if isinstance(cgg, dict) else None
            grado = str(grado_info.get("grado") or "") if isinstance(grado_info, dict) else ""
            grupo = str(grupo_info.get("grupo") or "") if isinstance(grupo_info, dict) else ""

            total_api = len(alumnos_data)
            listados = 0

            for entry in alumnos_data:
                if not isinstance(entry, dict):
                    continue
                alumno = entry.get("alumno")
                if not isinstance(alumno, dict):
                    continue
                persona = alumno.get("persona")
                if not isinstance(persona, dict):
                    continue
                persona_login = persona.get("personaLogin")
                if not isinstance(persona_login, dict):
                    persona_login = {}

                activo_en_clase = bool(entry.get("activo", False))
                activo_en_censo = bool(alumno.get("activo", False))
                if solo_activos and (not activo_en_clase or not activo_en_censo):
                    continue

                listados += 1
                detalle_rows.append(
                    {
                        "Clase ID": clase_id,
                        "Clase": clase_name,
                        "Grado": grado,
                        "Grupo": grupo,
                        "Alumno ID": alumno.get("alumnoId", ""),
                        "Persona ID": persona.get("personaId", ""),
                        "Login": persona_login.get("login", ""),
                        "Nombre completo": persona.get("nombreCompleto", ""),
                        "Activo censo": activo_en_censo,
                        "Activo clase": activo_en_clase,
                    }
                )

            resumen_rows.append(
                {
                    "Clase ID": clase_id,
                    "Clase": clase_name,
                    "Alumnos API": total_api,
                    "Alumnos listados": listados,
                }
            )

            if listados == 0:
                detalle_rows.append(
                    {
                        "Clase ID": clase_id,
                        "Clase": clase_name,
                        "Grado": grado,
                        "Grupo": grupo,
                        "Alumno ID": "",
                        "Persona ID": "",
                        "Login": "",
                        "Nombre completo": "(sin alumnos)",
                        "Activo censo": "",
                        "Activo clase": "",
                    }
                )

        progress.progress(100)
        status.empty()

        st.success("Listado generado.")
        st.markdown(f"- Clases evaluadas: `{total}`")
        st.markdown(f"- Clases con error: `{len(errores)}`")
        st.markdown(f"- Registros listados: `{len(detalle_rows)}`")

        if resumen_rows:
            resumen_rows = sorted(resumen_rows, key=lambda row: int(row.get("Clase ID", 0)))
            st.subheader("Resumen por clase")
            st.dataframe(resumen_rows, use_container_width=True)

        if detalle_rows:
            detalle_rows = sorted(
                detalle_rows,
                key=lambda row: (
                    int(row.get("Clase ID", 0)),
                    str(row.get("Nombre completo", "")),
                ),
            )
            st.subheader("Detalle de alumnos por clase")
            st.dataframe(detalle_rows, use_container_width=True)
        else:
            st.info("No hay alumnos para mostrar.")

        if errores:
            st.warning("Hubo errores en algunas clases.")
            st.write("\n".join(f"- {item}" for item in errores[:20]))
            restantes = len(errores) - 20
            if restantes > 0:
                st.caption(f"... y {restantes} errores mas.")

    st.divider()
    st.subheader("Generar Excel por niveles, grados y secciones (Censo)")
    st.write(
        "Usa el endpoint nivelesGradosGrupos, luego consulta alumnos por cada "
        "nivel/grado/grupo y genera un Excel."
    )
    solo_activos_censo = st.checkbox(
        "Solo alumnos activos en censo",
        value=False,
        key="clases_alumnos_excel_solo_activos",
    )
    if st.button(
        "Generar Excel alumnos (Censo)",
        type="primary",
        key="clases_alumnos_excel_generar",
    ):
        token = _clean_token(token_input)
        if not token:
            token = _clean_token(os.environ.get("PEGASUS_TOKEN", ""))
        if not token:
            st.error("Falta el token. Usa el input o la variable de entorno.")
            st.stop()

        try:
            with st.spinner("Consultando niveles/grados/grupos..."):
                niveles_data = _fetch_niveles_grados_grupos_censo(
                    token=token,
                    colegio_id=int(colegio_id),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
            st.stop()

        contexts: List[Dict[str, object]] = []
        seen_contexts = set()
        for nivel_entry in niveles_data:
            if not isinstance(nivel_entry, dict):
                continue
            nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
            nivel_id = nivel.get("nivelId")
            if nivel_id is None:
                continue
            try:
                nivel_id_int = int(nivel_id)
            except (TypeError, ValueError):
                continue
            nivel_name = str(nivel.get("nivel") or "")
            nivel_order = _to_int_or_default(nivel.get("nivel_orden"), 9999)
            grados = nivel_entry.get("grados") or []
            if not isinstance(grados, list):
                continue

            for grado_entry in grados:
                if not isinstance(grado_entry, dict):
                    continue
                grado = (
                    grado_entry.get("grado")
                    if isinstance(grado_entry.get("grado"), dict)
                    else {}
                )
                grado_id = grado.get("gradoId")
                if grado_id is None:
                    continue
                try:
                    grado_id_int = int(grado_id)
                except (TypeError, ValueError):
                    continue
                grado_name = str(grado.get("grado") or "")
                grado_order = _to_int_or_default(grado_id, 9999)
                grupos = grado_entry.get("grupos") or []
                if not isinstance(grupos, list):
                    continue

                for grupo_entry in grupos:
                    if not isinstance(grupo_entry, dict):
                        continue
                    grupo = (
                        grupo_entry.get("grupo")
                        if isinstance(grupo_entry.get("grupo"), dict)
                        else {}
                    )
                    grupo_id = grupo.get("grupoId")
                    if grupo_id is None:
                        continue
                    try:
                        grupo_id_int = int(grupo_id)
                    except (TypeError, ValueError):
                        continue

                    grupo_name = str(grupo.get("grupo") or "")
                    grupo_clave = str(grupo.get("grupoClave") or "")
                    key = (nivel_id_int, grado_id_int, grupo_id_int)
                    if key in seen_contexts:
                        continue
                    seen_contexts.add(key)
                    contexts.append(
                        {
                            "nivel_id": nivel_id_int,
                            "nivel": nivel_name,
                            "nivel_order": nivel_order,
                            "grado_id": grado_id_int,
                            "grado": grado_name,
                            "grado_order": grado_order,
                            "grupo_id": grupo_id_int,
                            "grupo": grupo_name,
                            "grupo_clave": grupo_clave,
                        }
                    )

        if not contexts:
            st.warning("No se encontraron niveles/grados/grupos para el colegio.")
            st.stop()

        contexts = sorted(
            contexts,
            key=lambda ctx: (
                int(ctx.get("nivel_order", 9999)),
                int(ctx.get("grado_order", 9999)),
                _grupo_sort_key(
                    str(ctx.get("grupo_clave", "")),
                    str(ctx.get("grupo", "")),
                ),
            ),
        )

        by_alumno_id: Dict[str, Dict[str, str]] = {}
        by_persona_id: Dict[str, Dict[str, str]] = {}
        try:
            by_alumno_id, by_persona_id = _fetch_login_password_lookup_censo(
                token=token,
                colegio_id=int(colegio_id),
                empresa_id=int(empresa_id),
                ciclo_id=int(ciclo_id),
                timeout=int(timeout),
            )
        except Exception as exc:  # pragma: no cover - UI
            st.warning(
                "No se pudo cargar lookup de login/password desde plantilla de "
                f"edicion masiva: {exc}"
            )

        rows_excel: List[Dict[str, object]] = []
        errores_excel: List[str] = []
        total = len(contexts)
        progress = st.progress(0)
        status = st.empty()

        for index, ctx in enumerate(contexts, start=1):
            progress.progress(int((index / total) * 100))
            status.write(
                "Consultando {idx}/{total}: N{nivel} G{grado} S{seccion}".format(
                    idx=index,
                    total=total,
                    nivel=ctx["nivel_id"],
                    grado=ctx["grado_id"],
                    seccion=ctx["grupo_id"],
                )
            )
            try:
                alumnos_data = _fetch_alumnos_censo(
                    token=token,
                    colegio_id=int(colegio_id),
                    nivel_id=int(ctx["nivel_id"]),
                    grado_id=int(ctx["grado_id"]),
                    grupo_id=int(ctx["grupo_id"]),
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    timeout=int(timeout),
                )
            except Exception as exc:  # pragma: no cover - UI
                errores_excel.append(
                    "nivelId={nivel} gradoId={grado} grupoId={grupo}: {error}".format(
                        nivel=ctx["nivel_id"],
                        grado=ctx["grado_id"],
                        grupo=ctx["grupo_id"],
                        error=exc,
                    )
                )
                continue

            for item in alumnos_data:
                if not isinstance(item, dict):
                    continue
                if solo_activos_censo and not _to_bool(item.get("activo")):
                    continue

                persona = item.get("persona") if isinstance(item.get("persona"), dict) else {}
                login, password = _resolve_alumno_login_password(
                    item=item,
                    by_alumno_id=by_alumno_id,
                    by_persona_id=by_persona_id,
                )
                rows_excel.append(
                    {
                        "_nivel_order": int(ctx["nivel_order"]),
                        "_grado_order": int(ctx["grado_order"]),
                        "_grupo_sort": _grupo_sort_key(
                            str(ctx.get("grupo_clave", "")),
                            str(ctx.get("grupo", "")),
                        ),
                        "Nivel": str(ctx.get("nivel", "")),
                        "Grado": str(ctx.get("grado", "")),
                        "Seccion": str(ctx.get("grupo_clave") or ctx.get("grupo") or ""),
                        "Nombre": str(persona.get("nombre") or ""),
                        "Apellido Paterno": str(persona.get("apellidoPaterno") or ""),
                        "Apellido Materno": str(persona.get("apellidoMaterno") or ""),
                        "Login": login,
                        "Password": password,
                    }
                )

        progress.progress(100)
        status.empty()

        if rows_excel:
            rows_excel = sorted(
                rows_excel,
                key=lambda row: (
                    row["_nivel_order"],
                    row["_grado_order"],
                    row["_grupo_sort"],
                    str(row.get("Apellido Paterno", "")).lower(),
                    str(row.get("Apellido Materno", "")).lower(),
                    str(row.get("Nombre", "")).lower(),
                ),
            )

        output = BytesIO()
        excel_columns = [
            "Nivel",
            "Grado",
            "Seccion",
            "Nombre",
            "Apellido Paterno",
            "Apellido Materno",
            "Login",
            "Password",
        ]
        df_excel = pd.DataFrame(rows_excel)
        if df_excel.empty:
            df_excel = pd.DataFrame(columns=excel_columns)
        else:
            df_excel = df_excel.drop(
                columns=["_nivel_order", "_grado_order", "_grupo_sort"], errors="ignore"
            )
            df_excel = df_excel.reindex(columns=excel_columns)

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_excel.to_excel(writer, index=False, sheet_name="Alumnos")
            if errores_excel:
                pd.DataFrame({"error": errores_excel}).to_excel(
                    writer, index=False, sheet_name="Errores"
                )
            ws = writer.book["Alumnos"]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions

        output.seek(0)
        file_name = f"alumnos_censo_{int(colegio_id)}_{int(ciclo_id)}.xlsx"
        st.success("Excel generado.")
        st.markdown(f"- Combinaciones evaluadas: `{total}`")
        st.markdown(f"- Filas en Excel: `{len(df_excel)}`")
        st.markdown(f"- Errores: `{len(errores_excel)}`")
        st.download_button(
            label="Descargar Excel alumnos",
            data=output.getvalue(),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="clases_alumnos_excel_download",
        )

        if not df_excel.empty:
            st.dataframe(df_excel, use_container_width=True)
        if errores_excel:
            st.warning("Hubo errores en algunas combinaciones.")
            st.write("\n".join(f"- {item}" for item in errores_excel[:20]))
            restantes = len(errores_excel) - 20
            if restantes > 0:
                st.caption(f"... y {restantes} errores mas.")
