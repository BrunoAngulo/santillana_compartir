from __future__ import annotations

from typing import Dict

import pandas as pd
import streamlit as st

from .service import (
    build_rlp_report_excel,
    build_rlp_template_excel,
    clean_cookie_header,
    has_rlp_session_cookie,
    load_rlp_tokens,
    verify_rlp_tokens,
)


def _show_report(report: Dict[str, object]) -> None:
    total = int(report.get("total") or 0)
    success_count = int(report.get("success_count") or 0)
    error_count = int(report.get("error_count") or 0)

    col_total, col_success, col_error = st.columns(3)
    col_total.metric("Tokens", total)
    col_success.metric("Correctos", success_count)
    col_error.metric("Con error", error_count)

    token_rows = report.get("token_rows")
    token_df = pd.DataFrame(token_rows if isinstance(token_rows, list) else [])
    if not token_df.empty:
        st.markdown("**Vista previa de token_details**")
        st.dataframe(token_df.head(300), use_container_width=True, hide_index=True)
        if len(token_df) > 300:
            st.caption(f"Mostrando 300 de {len(token_df)} tokens.")

    product_rows = report.get("product_rows")
    product_df = pd.DataFrame(product_rows if isinstance(product_rows, list) else [])
    if not product_df.empty:
        with st.expander(f"Productos asignados ({len(product_df)})"):
            st.dataframe(product_df.head(300), use_container_width=True, hide_index=True)


def render_rlp_view() -> None:
    st.subheader("RLP - Verificar tokens")
    st.caption(
        "Sube un Excel con la columna Token. RLP devuelve token_details, "
        "productos asignados y el conteo de suscripciones."
    )

    with st.container(border=True):
        st.markdown("**Sesion RLP**")
        st.caption(
            "La cookie se puede cargar desde Lectura Tokens con la extension. "
            "Tambien puedes pegar el Cookie header de una sesion abierta en richmondlp.com."
        )
        cookie_header = clean_cookie_header(
            st.text_input(
                "Cookie header RLP",
                key="rlp_cookie_header",
                type="password",
                placeholder=(
                    "recaptcha_verified=...; "
                    "_unity_web_session=...; _ga=..."
                ),
                help=(
                    "En DevTools > Network > access_codes > Request Headers, "
                    "copia solo el valor completo de Cookie."
                ),
            )
        )
        if cookie_header:
            if has_rlp_session_cookie(cookie_header):
                st.success("Cookie _unity_web_session detectada.")
            else:
                st.warning("No se encontro _unity_web_session en el Cookie header.")

    with st.container(border=True):
        col_file, col_template = st.columns([3, 1], gap="small")
        uploaded_file = col_file.file_uploader(
            "Excel de tokens",
            type=["xlsx", "xlsm", "csv"],
            key="rlp_tokens_upload",
        )
        col_template.download_button(
            "Descargar plantilla",
            data=build_rlp_template_excel(),
            file_name="plantilla_tokens_rlp.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="rlp_template_download",
        )

        timeout = st.number_input(
            "Timeout por token (segundos)",
            min_value=30,
            max_value=300,
            value=120,
            step=10,
            key="rlp_timeout",
            help="La respuesta puede ser grande cuando el token tiene muchas suscripciones.",
        )

        if st.button(
            "Verificar tokens",
            type="primary",
            use_container_width=True,
            key="rlp_verify_tokens",
        ):
            if not has_rlp_session_cookie(cookie_header):
                st.error("Carga una sesion RLP valida con _unity_web_session.")
            elif uploaded_file is None:
                st.error("Sube el Excel con la columna Token.")
            else:
                try:
                    tokens = load_rlp_tokens(
                        uploaded_file.getvalue(),
                        uploaded_file.name or "tokens.xlsx",
                    )
                except Exception as exc:
                    st.error(f"No se pudo leer el archivo: {exc}")
                else:
                    if len(tokens) > 20:
                        st.warning(
                            f"Se consultaran {len(tokens)} tokens. "
                            "RLP puede tardar bastante por el tamano de cada respuesta."
                        )
                    progress = st.progress(0.0)
                    status = st.empty()

                    def on_progress(index: int, total: int, token: str) -> None:
                        status.caption(f"Consultando {index}/{total}: {token}")
                        progress.progress(index / max(total, 1))

                    try:
                        report = verify_rlp_tokens(
                            tokens=tokens,
                            cookie_header=cookie_header,
                            timeout=int(timeout),
                            on_progress=on_progress,
                        )
                        report_bytes = build_rlp_report_excel(report)
                    except (RuntimeError, ValueError) as exc:
                        st.error(f"No se pudo ejecutar la verificacion: {exc}")
                    else:
                        st.session_state["rlp_report"] = report
                        st.session_state["rlp_report_bytes"] = report_bytes
                        status.caption("Verificacion terminada.")
                        progress.progress(1.0)

    report = st.session_state.get("rlp_report")
    if isinstance(report, dict):
        _show_report(report)
        report_bytes = bytes(st.session_state.get("rlp_report_bytes") or b"")
        if report_bytes:
            st.download_button(
                "Descargar resultados RLP",
                data=report_bytes,
                file_name="verificacion_tokens_rlp.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                key="rlp_report_download",
            )
