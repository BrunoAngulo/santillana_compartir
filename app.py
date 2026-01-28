import os
import re
import tempfile
from datetime import date
from pathlib import Path
from typing import Dict, List

import requests
import streamlit as st

from santillana_format.alumnos import DEFAULT_EMPRESA_ID
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
from santillana_format.richmond_groups import process_rs_groups


GESTION_ESCOLAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases"
)
GESTION_ESCOLAR_CICLO_ID_DEFAULT = 207


st.set_page_config(page_title="Generador de Plantilla", layout="centered")
st.title("Generar Plantilla de Clases")
st.write(
    "Elige si quieres crear clases, asignar profesores a clases o gestionar clases."
)

tab_clases, tab_profesores_clases, tab_clases_api, tab_rs = st.tabs(
    ["Crear clases", "Profesores con clases", "Clases API", "Clases RS (Standalone)"]
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
    st.write("Genera el Excel base de profesores activos y luego asignalos a clases.")

    col1, col2 = st.columns(2)
    token_input = col1.text_input(
        "Token (Bearer)", type="password", key="profesores_token"
    )
    colegio_id = col2.number_input(
        "Colegio Clave", min_value=1, step=1, format="%d", key="profesores_colegio"
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

    st.subheader("Generar Excel base de profesores activos")
    st.write(
        "Crea un Excel listo con columnas Id, Nombre, Apellido, Sexo, DNI, E-mail, Login y Password."
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
            persona_ids = _parse_persona_ids(persona_ids_raw)
        except ValueError as exc:
            st.error(f"Error: {exc}")
            st.stop()
        try:
            data, summary, errores = listar_profesores_data(
                token=token,
                colegio_id=int(colegio_id),
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
                    "Sexo": entry.get("sexo", ""),
                    "DNI": dni,
                    "E-mail": email,
                    "Login": login,
                    "Password": dni,
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
            file_name = f"profesores_base_{int(colegio_id)}.xlsx"
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
        "(Inicial/Primaria/Secundaria o I3, P1, S2...)."
    )
    uploaded_profesores = st.file_uploader(
        "Excel de profesores",
        type=["xlsx", "csv", "txt"],
        key="profesores_excel",
    )
    sheet_name = st.text_input(
        "Hoja (opcional)", value="", help="Nombre de la hoja si el Excel tiene varias."
    )
    remove_missing = st.checkbox(
        "Eliminar profesores que no estan en el Excel (solo clases evaluadas)",
        value=False,
        key="profesores_remove",
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
        if run_apply and not confirm_apply:
            st.error("Debes confirmar antes de aplicar cambios.")
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

            summary, warnings, errors = asignar_profesores_clases(
                token=token,
                empresa_id=DEFAULT_EMPRESA_ID,
                ciclo_id=int(ciclo_id),
                colegio_id=int(colegio_id),
                excel_path=tmp_path,
                sheet_name=sheet_name.strip() or None,
                timeout=int(timeout),
                dry_run=not run_apply,
                remove_missing=remove_missing,
                on_log=_on_log,
            )
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
            "Listo. Docentes: {docentes_procesados}, "
            "Sin match: {docentes_sin_match}, "
            "Clases: {clases_encontradas}, "
            "Asignaciones nuevas: {asignaciones_nuevas}, "
            "Omitidas: {asignaciones_omitidas}, "
            "Eliminaciones: {eliminaciones}, "
            "Errores API: {errores_api}.".format(**summary)
        )
        if warnings:
            st.warning("Advertencias:")
            st.write("\\n".join(f"- {item}" for item in warnings))
        if errors:
            st.error("Errores al asignar profesores:")
            st.dataframe(errors, use_container_width=True)
        if logs:
            st.text_area("Log de ejecucion", value="\\n".join(logs), height=300)

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

with tab_rs:
    st.subheader("Clases RS (Standalone)")
    st.write(
        "Sube un Excel (.xlsx) con columnas: Grado, Nivel, Producto, Secciones. "
        "El nombre de la clase se genera como 'Ingl\u00E9s {grado}{P/S}{secci\u00F3n}'. "
        "El proceso cambia la institucion, lista grupos existentes y crea solo los nuevos."
    )
    institution_id = st.text_input(
        "Institution UUID (ID del colegio)", key="rs_institution"
    )
    token_input = st.text_input(
        "Bearer Token", type="password", key="rs_token"
    )
    uploaded_rs = st.file_uploader(
        "Excel (.xlsx)",
        type=["xlsx"],
        key="rs_excel",
    )
    timeout = st.number_input(
        "Timeout (seg)",
        min_value=5,
        step=5,
        value=30,
        format="%d",
        key="rs_timeout",
    )

    if st.button("Procesar", type="primary", key="rs_procesar"):
        if not institution_id.strip():
            st.error("Ingresa el Institution UUID.")
            st.stop()
        if not token_input.strip():
            st.error("Ingresa el Bearer Token.")
            st.stop()
        if not uploaded_rs:
            st.error("Sube un Excel .xlsx.")
            st.stop()

        token = _clean_token(token_input)

        start_date = date.today()
        end_date = date(start_date.year, 12, 31)

        progress = st.progress(0)

        def _on_progress(current: int, total: int) -> None:
            percent = int((current / total) * 100) if total else 0
            progress.progress(percent)

        try:
            summary, results = process_rs_groups(
                token=token,
                institution_id=institution_id.strip(),
                excel_input=uploaded_rs,
                filename=uploaded_rs.name,
                start_date=start_date,
                end_date=end_date,
                timeout=int(timeout),
                on_progress=_on_progress,
            )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
            st.stop()

        st.success(
            "Listo. Procesados: {procesados}, Creados: {creados}, "
            "Omitidos: {omitidos}, Errores: {errores}.".format(**summary)
        )
        st.dataframe(results, use_container_width=True)
