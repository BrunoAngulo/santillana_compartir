from __future__ import annotations

import json
import os
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


def render_jira_focus_web(height: int = 240) -> None:
    components.html(_load_html(), height=height, scrolling=False)


def _load_html() -> str:
    html_path = Path(__file__).with_name("jira_focus_web.html")
    html = html_path.read_text(encoding="utf-8")

    def _cfg_value(key: str, default: str) -> str:
        try:
            if key in st.secrets:
                return str(st.secrets[key] or default)
        except Exception:
            pass
        return str(os.environ.get(key, default) or default)

    replacements = {
        "__BASE_OAUTH_URL__": _cfg_value(
            "JIRA_OAUTH_BASE_URL", "https://project-tools-santillana.atlassian.net/"
        ),
        "__BASE_OAUTH_CLIENT_ID__": _cfg_value(
            "JIRA_OAUTH_CLIENT_ID", "u6QCVXJ1fJpuflQxoeTuNdzN6nkVriIo"
        ),
        "__BASE_OAUTH_CLIENT_SECRET__": _cfg_value("JIRA_OAUTH_CLIENT_SECRET", ""),
        "__BASE_OAUTH_REDIRECT_URI__": _cfg_value(
            "JIRA_OAUTH_REDIRECT_URI", "https://santed.streamlit.app/"
        ),
    }
    for token, value in replacements.items():
        html = html.replace(token, json.dumps(str(value or "")))
    return html
