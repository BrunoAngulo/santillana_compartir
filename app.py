from pathlib import Path

import streamlit as st

from processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    process_excel,
)


st.set_page_config(page_title="Generador de Plantilla", layout="centered")
st.title("Generar Plantilla de Clases")
st.write(
    "Sube el Excel de detalle, ingresa el código (CRM) y obtén la plantilla lista."
)

with st.expander("Opciones de entrada", expanded=True):
    uploaded_excel = st.file_uploader(
        "Excel de entrada", type=["xlsx"], help="Ejemplo: PreOnboarding_Detalle_20251212.xlsx"
    )
    col1, col2 = st.columns(2)
    codigo = col1.text_input("Código (CRM)", placeholder="00001053")
    columna_codigo = col2.text_input(
        "Columna de código",
        value=CODE_COLUMN_NAME,
        help="Nombre de la columna donde buscar el código",
    )
    hoja = col1.text_input(
        "Hoja a leer", value=SHEET_NAME, help="Nombre de la hoja de entrada"
    )

with st.expander("Plantilla (opcional)", expanded=False):
    uploaded_template = st.file_uploader(
        "PlantillaClases.xlsx (opcional)",
        type=["xlsx"],
        help="Si no subes nada, se usará la plantilla local si existe.",
    )

if st.button("Generar", type="primary"):
    if not uploaded_excel:
        st.error("Sube un Excel de entrada.")
        st.stop()
    if not codigo.strip():
        st.error("Ingresa un código.")
        st.stop()

    excel_bytes = uploaded_excel.read()
    plantilla_bytes = uploaded_template.read() if uploaded_template else None
    plantilla_path = (
        Path(OUTPUT_FILENAME) if not plantilla_bytes and Path(OUTPUT_FILENAME).exists() else None
    )

    if not plantilla_bytes and plantilla_path is None:
        st.warning(
            "No se encontró PlantillaClases.xlsx local ni se subió una plantilla. Se creará un archivo nuevo sin formato."
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
