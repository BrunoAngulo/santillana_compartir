from __future__ import annotations

from pathlib import Path

import streamlit.components.v1 as components


def render_jira_focus_web(height: int = 3200) -> None:
    components.html(_load_html(), height=height, scrolling=True)


def _load_html() -> str:
    html_path = Path(__file__).with_name("jira_focus_web.html")
    return html_path.read_text(encoding="utf-8")
