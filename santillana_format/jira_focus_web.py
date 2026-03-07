from __future__ import annotations

import json
import os
import re
from pathlib import Path

import streamlit as st


def render_jira_focus_web() -> None:
    st.html(_load_embedded_html(), unsafe_allow_javascript=True)


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


def _load_embedded_html() -> str:
    html = _load_html()
    head_match = re.search(r"<head[^>]*>([\s\S]*?)</head>", html, flags=re.IGNORECASE)
    body_match = re.search(r"<body[^>]*>([\s\S]*?)</body>", html, flags=re.IGNORECASE)
    parts = []
    if head_match:
        parts.extend(
            re.findall(
                r"<style[\s\S]*?</style>|<link[\s\S]*?>",
                head_match.group(1),
                flags=re.IGNORECASE,
            )
        )
    if body_match:
        parts.append(body_match.group(1))
    else:
        parts.append(html)
    return "\n".join(parts)
