"""Facade for the Richmond Learning Platform domain."""

from .service import (
    build_rlp_report_excel,
    build_rlp_template_excel,
    fetch_rlp_access_code,
    load_rlp_tokens,
    normalize_rlp_access_code,
    verify_rlp_tokens,
)
from .view import render_rlp_view

__all__ = [
    "build_rlp_report_excel",
    "build_rlp_template_excel",
    "fetch_rlp_access_code",
    "load_rlp_tokens",
    "normalize_rlp_access_code",
    "render_rlp_view",
    "verify_rlp_tokens",
]
