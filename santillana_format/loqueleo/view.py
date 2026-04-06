from __future__ import annotations

import pandas as pd
import streamlit as st

from .ssr import (
    LoqueleoUsersExport,
    build_loqueleo_users_excel_bytes,
    build_loqueleo_users_filename,
    fetch_loqueleo_users_listing,
)


def _ensure_loqueleo_session_state_defaults() -> None:
    current_session_id = str(st.session_state.get("loqueleo_session_id", "") or "").strip()
    if "loqueleo_session_id_input" not in st.session_state:
        st.session_state["loqueleo_session_id_input"] = current_session_id
    elif not str(st.session_state.get("loqueleo_session_id_input", "") or "").strip() and current_session_id:
        st.session_state["loqueleo_session_id_input"] = current_session_id


def _sync_loqueleo_session_id_from_input() -> None:
    session_id = str(
        st.session_state.get("loqueleo_session_id_input", "") or ""
    ).strip()
    st.session_state["loqueleo_session_id"] = session_id
    st.session_state["loqueleo_session_bridge_mode"] = "write"
    st.session_state["loqueleo_session_bridge_value"] = session_id


def _build_loqueleo_result_from_session_state() -> LoqueleoUsersExport | None:
    rows = st.session_state.get("loqueleo_last_rows") or []
    if not isinstance(rows, list) or not rows:
        return None

    return LoqueleoUsersExport(
        input_url="",
        first_response_url="",
        final_response_url="",
        organization_id="",
        organization_name=str(
            st.session_state.get("loqueleo_last_organization_name", "") or ""
        ).strip(),
        user_type="",
        user_type_label=str(
            st.session_state.get("loqueleo_last_user_type_label", "") or ""
        ).strip()
        or "Usuarios",
        locale="",
        year=str(st.session_state.get("loqueleo_last_year", "") or "").strip(),
        csrf_token="",
        reported_total=st.session_state.get("loqueleo_last_reported_total"),
        rows=rows,
        visited_pages=list(st.session_state.get("loqueleo_last_visited_pages") or []),
        page_count=int(st.session_state.get("loqueleo_last_page_count", 0) or 0),
        stop_reason=str(st.session_state.get("loqueleo_last_stop_reason", "") or "").strip(),
    )


def _render_loqueleo_result_preview() -> None:
    export_result = _build_loqueleo_result_from_session_state()
    if export_result is None:
        return
    rows = export_result.rows

    st.markdown("**Resultado**")
    st.caption(
        f"{export_result.organization_name} | {export_result.user_type_label} | "
        f"Filas: {len(rows)} | Paginas: {export_result.page_count}"
    )

    excel_bytes = build_loqueleo_users_excel_bytes(export_result)
    file_name = build_loqueleo_users_filename(export_result)
    with st.container(border=True):
        st.markdown("**Excel listo para descargar**")
        st.caption(f"Archivo: {file_name}")
        st.download_button(
            "Descargar Excel",
            data=bytes(excel_bytes),
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
            key="loqueleo_download_excel_btn",
        )

    preview_df = pd.DataFrame(rows)
    preview_df = preview_df.reindex(
        columns=["Nombre", "Cuenta", "Usuario ID"],
        fill_value="",
    ).head(200)
    if not preview_df.empty:
        preview_df.index = range(1, len(preview_df) + 1)
    st.caption("Vista previa: primeras 200 filas del Excel.")
    st.dataframe(preview_df, use_container_width=True)


def render_loqueleo_view() -> None:
    _ensure_loqueleo_session_state_defaults()

    st.subheader("Loqueleo")
    st.caption(
        "Pega una URL de listado de usuarios de Loqueleo. La app la lee por SSR, recorre la paginacion y genera un Excel."
    )

    current_session_id = str(st.session_state.get("loqueleo_session_id", "") or "").strip()
    if current_session_id:
        st.caption("Hay un _session_id cargado en la sesion actual.")
    else:
        st.warning("No hay _session_id cargado. Usa Lectura Tokens o pegalo manualmente aqui.")

    with st.container(border=True):
        session_col, save_col = st.columns([5.2, 1], gap="small")
        with session_col:
            st.text_input(
                "_session_id",
                key="loqueleo_session_id_input",
                help="Sesion de Loqueleo usada para autenticar el GET SSR.",
            )
        with save_col:
            st.markdown(
                "<div style='height: 1.85rem;' aria-hidden='true'></div>",
                unsafe_allow_html=True,
            )
            if st.button(
                "Usar",
                key="loqueleo_use_session_btn",
                use_container_width=True,
            ):
                _sync_loqueleo_session_id_from_input()
                st.rerun()

        st.text_input(
            "URL de listado",
            key="loqueleo_listing_url_input",
            placeholder=(
                "https://loqueleodigital.com/organizations/1943/users?"
                "locale=es&type=User%3A%3ATeacher&year=2026"
            ),
            help=(
                "Acepta URLs de usuarios, por ejemplo docentes o alumnos. "
                "La paginacion se recorre automaticamente."
            ),
        )
        max_pages = st.number_input(
            "Maximo de paginas",
            min_value=1,
            max_value=300,
            value=100,
            step=1,
            key="loqueleo_max_pages_input",
        )
        timeout = st.number_input(
            "Timeout por pagina (segundos)",
            min_value=5,
            max_value=120,
            value=30,
            step=1,
            key="loqueleo_timeout_input",
        )

        if st.button(
            "Leer URL y generar Excel",
            type="primary",
            key="loqueleo_fetch_users_btn",
            use_container_width=True,
        ):
            _sync_loqueleo_session_id_from_input()
            session_id = str(st.session_state.get("loqueleo_session_id", "") or "").strip()
            listing_url = str(st.session_state.get("loqueleo_listing_url_input", "") or "").strip()
            cookie_header = str(st.session_state.get("loqueleo_cookie_header", "") or "").strip()
            try:
                with st.spinner("Leyendo Loqueleo y recorriendo paginacion..."):
                    export_result = fetch_loqueleo_users_listing(
                        session_id=session_id,
                        listing_url=listing_url,
                        cookie_header=cookie_header,
                        timeout=int(timeout),
                        max_pages=int(max_pages),
                    )
                    excel_bytes = build_loqueleo_users_excel_bytes(export_result)
                    file_name = build_loqueleo_users_filename(export_result)
            except Exception as exc:
                st.error(str(exc))
            else:
                st.session_state["loqueleo_last_rows"] = export_result.rows
                st.session_state["loqueleo_last_excel_bytes"] = excel_bytes
                st.session_state["loqueleo_last_excel_filename"] = file_name
                st.session_state["loqueleo_last_page_count"] = export_result.page_count
                st.session_state["loqueleo_last_reported_total"] = export_result.reported_total
                st.session_state["loqueleo_last_stop_reason"] = export_result.stop_reason
                st.session_state["loqueleo_last_organization_name"] = export_result.organization_name
                st.session_state["loqueleo_last_user_type_label"] = export_result.user_type_label
                st.session_state["loqueleo_last_year"] = export_result.year
                st.session_state["loqueleo_last_visited_pages"] = export_result.visited_pages
                st.success(
                    f"Loqueleo leido correctamente. Filas: {len(export_result.rows)} | "
                    f"Paginas: {export_result.page_count}."
                )

    _render_loqueleo_result_preview()
