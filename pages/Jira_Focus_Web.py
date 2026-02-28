import streamlit as st

from santillana_format.jira_focus_web import render_jira_focus_web


st.set_page_config(page_title="Jira Focus Web", layout="wide")

st.title("Jira Focus Web")
st.caption(
    "Modulo frontend para Jira Cloud: conexion OAuth, timer laboral, worklogs, "
    "historial editable, comentarios, sugerencias y reportes."
)

render_jira_focus_web()
