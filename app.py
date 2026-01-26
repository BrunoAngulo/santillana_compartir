import os
import tempfile
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
from santillana_format.profesores import DEFAULT_CICLO_ID as PROFESORES_CICLO_ID_DEFAULT
from santillana_format.profesores_clases import asignar_profesores_clases


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

tab_clases, tab_profesores_clases, tab_clases_api = st.tabs(
    ["Crear clases", "Profesores con clases", "Clases API"]
)


def _clean_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


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
    st.subheader("Asignar profesores a clases")
    st.write(
        "Sube un Excel con columnas persona_id y CURSO, más niveles/grados "
        "(Inicial/Primaria/Secundaria o I3, P1, S2...)."
    )
    st.caption(
        "Proceso: 1) sube el Excel, 2) simula para revisar el log, "
        "3) activa 'Aplicar cambios' para ejecutar, 4) opcionalmente elimina "
        "profesores que no estén en el Excel."
    )
    st.info(
        "Necesitas un Excel ya listo con profesores activos. "
        "Puedes generarlo con el comando: python main.py profesores --colegio-id ... --ciclo-id ..."
    )
    uploaded_profesores = st.file_uploader(
        "Excel de profesores",
        type=["xlsx", "csv", "txt"],
        key="profesores_excel",
    )
    sheet_name = st.text_input(
        "Hoja (opcional)", value="", help="Nombre de la hoja si el Excel tiene varias."
    )

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
        aplicar_cambios = st.checkbox(
            "Aplicar cambios (desactiva modo simulación)",
            value=False,
            key="profesores_apply",
        )
        remove_missing = st.checkbox(
            "Eliminar profesores que no están en el Excel (solo clases evaluadas)",
            value=False,
            key="profesores_remove",
        )

    if st.button("Procesar profesores con clases", type="primary"):
        if not uploaded_profesores:
            st.error("Sube un Excel de profesores.")
            st.stop()

        token = _clean_token(token_input)
        if not token:
            token = _clean_token(os.environ.get("PEGASUS_TOKEN", ""))
        if not token:
            st.error("Falta el token. Usa el input o la variable de entorno.")
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
                dry_run=not aplicar_cambios,
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
            st.write("\n".join(f"- {item}" for item in warnings))
        if errors:
            st.error("Errores al asignar profesores:")
            st.dataframe(errors, use_container_width=True)
        if logs:
            st.text_area("Log de ejecución", value="\n".join(logs), height=300)


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
