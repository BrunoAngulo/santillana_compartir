from pathlib import Path

import streamlit as st

from santillana_format.alumnos import (
    DEFAULT_CICLO_ID,
    DEFAULT_EMPRESA_ID,
    GRADOS_POR_NIVEL,
    GRUPO_LETRA_TO_ID,
    NIVEL_MAP,
    build_alumnos_filename,
    listar_alumnos,
    parse_id_list,
)
from santillana_format.processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    process_excel,
)


st.set_page_config(page_title="Generador de Plantilla", layout="centered")
st.title("Generar Plantilla de Clases")
st.write("Elige si quieres crear clases o listar alumnos desde Pegasus.")

tab_clases, tab_alumnos = st.tabs(["Crear clases", "Listar alumnos"])


def _normalize_token(value: str) -> str:
    token = value.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


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

    with st.expander("Plantilla (opcional)", expanded=False):
        uploaded_template = st.file_uploader(
            "PlantillaClases.xlsx (opcional)",
            type=["xlsx"],
            help="Si no subes nada, se usara la plantilla local si existe.",
        )

    if st.button("Generar", type="primary"):
        if not uploaded_excel:
            st.error("Sube un Excel de entrada.")
            st.stop()
        if not codigo.strip():
            st.error("Ingresa un codigo.")
            st.stop()

        excel_bytes = uploaded_excel.read()
        plantilla_bytes = uploaded_template.read() if uploaded_template else None
        plantilla_path = (
            Path(OUTPUT_FILENAME)
            if not plantilla_bytes and Path(OUTPUT_FILENAME).exists()
            else None
        )

        if not plantilla_bytes and plantilla_path is None:
            st.warning(
                "No se encontro PlantillaClases.xlsx local ni se subio una plantilla. Se creara un archivo nuevo sin formato."
            )

        try:
            with st.spinner("Procesando..."):
                output_bytes, summary = process_excel(
                    excel_bytes,
                    codigo=codigo,
                    columna_codigo=columna_codigo,
                    hoja=hoja,
                    plantilla_bytes=plantilla_bytes,
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


with tab_alumnos:
    st.subheader("Listar alumnos")
    token_raw = st.text_input(
        "Authorization Bearer token",
        type="password",
        help="Pega solo el token, sin el prefijo 'Bearer'.",
    )
    col1, col2 = st.columns(2)
    empresa_id = col1.number_input(
        "Empresa ID", min_value=1, value=DEFAULT_EMPRESA_ID, step=1
    )
    ciclo_id = col2.number_input("Ciclo ID", min_value=1, value=DEFAULT_CICLO_ID, step=1)

    colegios_text = st.text_area(
        "Colegios (IDs separados por coma o salto de linea)",
        value="25947",
        help="Ejemplo: 13255, 25947",
    )
    niveles = st.multiselect(
        "Niveles",
        options=list(NIVEL_MAP.keys()),
        default=list(NIVEL_MAP.keys()),
    )
    grupos = st.multiselect(
        "Secciones (A-K)",
        options=sorted(GRUPO_LETRA_TO_ID.keys()),
        default=sorted(GRUPO_LETRA_TO_ID.keys()),
    )

    colegio_ids = parse_id_list(colegios_text)
    nivel_ids = [NIVEL_MAP[nivel] for nivel in niveles]
    grupo_ids = [GRUPO_LETRA_TO_ID[letra] for letra in grupos]
    total_solicitudes = len(colegio_ids) * sum(
        len(GRADOS_POR_NIVEL.get(nivel_id, {})) for nivel_id in nivel_ids
    ) * len(grupo_ids)

    if total_solicitudes:
        st.caption(f"Solicitudes estimadas: {total_solicitudes}")

    if st.button("Listar alumnos", type="primary"):
        token = _normalize_token(token_raw)
        if not token:
            st.error("Ingresa el token de autorizacion.")
            st.stop()
        if not colegio_ids:
            st.error("Ingresa al menos un colegio.")
            st.stop()
        if not nivel_ids or not grupo_ids:
            st.error("Selecciona niveles y secciones.")
            st.stop()

        progress = st.progress(0)
        status = st.empty()

        def _on_progress(actual: int, total: int) -> None:
            porcentaje = int((actual / max(total, 1)) * 100)
            progress.progress(porcentaje)
            status.caption(f"Solicitudes: {actual}/{total}")

        try:
            with st.spinner("Consultando alumnos..."):
                output_bytes, summary = listar_alumnos(
                    token=token,
                    colegio_ids=colegio_ids,
                    nivel_ids=nivel_ids,
                    grupo_ids=grupo_ids,
                    empresa_id=int(empresa_id),
                    ciclo_id=int(ciclo_id),
                    on_progress=_on_progress,
                )
            st.success(
                "Listo. Solicitudes: {total}, Errores: {errores}, Alumnos: {alumnos}.".format(
                    total=summary["solicitudes_total"],
                    errores=summary["solicitudes_error"],
                    alumnos=summary["alumnos_total"],
                )
            )
            download_name = build_alumnos_filename(colegio_ids)
            st.download_button(
                label="Descargar Excel",
                data=output_bytes,
                file_name=download_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as exc:  # pragma: no cover - UI
            st.error(f"Error: {exc}")
