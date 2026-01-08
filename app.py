import os
from pathlib import Path
from typing import Dict, List

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


st.set_page_config(page_title="Generador de Plantilla", layout="centered")
st.title("Generar Plantilla de Clases")
st.write("Elige si quieres crear clases, depurar alumnos o gestionar clases.")

tab_clases, tab_depurar, tab_clases_api = st.tabs(
    ["Crear clases", "Depurar alumnos", "Clases API"]
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
    colegio_id = st.number_input("Colegio ID", min_value=1, step=1, format="%d")
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
