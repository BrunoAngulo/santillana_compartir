from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from html.parser import HTMLParser
from io import BytesIO
from typing import Dict, List, Optional
from urllib.parse import urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests

IPA_COOKIE_NAME = "local-santadmin"
IPA_DEFAULT_NIVEL_ID = 45
IPA_DEFAULT_TIMEOUT = 30
IPA_GRADES_PATH_TEMPLATE = "/niveles/mantenedor_preguntas_nivel/{nivel_id}"
IPA_AREAS_PATH_TEMPLATE = "/areas/mantenedor_preguntas_areas/{grade_id}"
IPA_GRADE_LINK_RE = re.compile(r"/areas/mantenedor_preguntas_areas/(\d+)(?:/|$)")
IPA_AREA_LINK_RE = re.compile(
    r"/unidad/mantenedor_preguntas_unidades/(\d+)/(\d+)(?:/|$)"
)
IPA_EVAL_PREFIX = "/evaluaciones/mantenedor_preguntas_evaluaciones/"
IPA_EVAL_LINK_RE = re.compile(
    r"/evaluaciones/mantenedor_preguntas_evaluaciones/([^?#]+)"
)
IPA_EVALUATION_DETAIL_RE = re.compile(
    r"/preguntas/datos_pregunta/(\d+)/([^/]+)/([^/]+)/([^/\s]+)/([^/\s]+)"
)
IPA_MASSIVE_PLANNING_RE = re.compile(
    r"/evaluaciones/list_pre_planificar_masivo/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)"
)
IPA_ADD_COURSE_RE = re.compile(
    r"/evaluaciones/agregar_curso/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)"
)
IPA_SCHOOLS_BASE_URL = "https://santadmin.pleno.digital"
IPA_SCHOOLS_PATH = "/colegios/listado_colegios"
IPA_SCHOOLS_URL = f"{IPA_SCHOOLS_BASE_URL}{IPA_SCHOOLS_PATH}"
IPA_SCHOOL_LINK_RE = re.compile(r"/colegios/volver_colegio/(\d+)(?:/|$)")
IPA_COURSE_LINK_RE = re.compile(r"/cursos/ver_curso/(\d+)/(\d+)(?:/|$)")
IPA_TARGET_AREAS = {
    "matematica": "Matematica",
    "comunicacion": "Comunicacion",
}
IPA_PROGRESS_LABEL = "Progreso academico"


@dataclass
class IPAAnchor:
    text: str
    href: str
    url: str
    path: str


@dataclass
class IPAGradeLink:
    name: str
    grade_id: str
    url: str


@dataclass
class IPAAreaLink:
    name: str
    area_key: str
    area_id: str
    grade_id: str
    url: str


@dataclass
class IPAEvaluationLink:
    name: str
    eval_id: str
    url: str


@dataclass
class IPAEvaluationRow:
    name: str
    eval_id: str
    unit_id: str = ""
    grade_id: str = ""
    area_id: str = ""
    eval_type: str = ""
    item_count: str = ""
    score: str = ""
    detail_url: str = ""
    massive_planning_url: str = ""
    preplanning_url: str = ""
    edit_url: str = ""


@dataclass
class IPASchoolRow:
    school_id: str
    lms_id: str = ""
    name: str = ""
    timezone: str = ""
    pleno_offline: str = ""
    demo: str = ""
    url: str = ""


@dataclass
class IPASchoolCourseRow:
    grade_name: str
    course_id: str
    name: str
    school_year_id: str
    school_year: str
    students: str
    school_id: str = ""
    url: str = ""


@dataclass
class IPASchoolListingResult:
    listing_url: str
    listing_response_url: str
    schools: List[IPASchoolRow] = field(default_factory=list)
    visited_urls: List[str] = field(default_factory=list)


@dataclass
class IPASchoolCoursesResult:
    school_id: str
    school_name: str
    school_url: str
    response_url: str
    courses: List[IPASchoolCourseRow] = field(default_factory=list)
    visited_urls: List[str] = field(default_factory=list)


@dataclass
class IPACourseProgressResult:
    course_url: str
    course_response_url: str
    progress_url: str
    progress_response_url: str
    evaluations: List[IPAEvaluationRow] = field(default_factory=list)
    visited_urls: List[str] = field(default_factory=list)


@dataclass
class IPAGradeResult:
    name: str
    grade_id: str
    areas_url: str
    areas: List[IPAAreaLink] = field(default_factory=list)
    communication_progress: Optional[IPAEvaluationLink] = None


@dataclass
class IPAExtractionResult:
    base_url: str
    level_url: str
    level_response_url: str
    grades: List[IPAGradeResult] = field(default_factory=list)
    visited_urls: List[str] = field(default_factory=list)


@dataclass
class IPAGradeListingResult:
    base_url: str
    level_url: str
    level_response_url: str
    grades: List[IPAGradeLink] = field(default_factory=list)
    visited_urls: List[str] = field(default_factory=list)


@dataclass
class IPAGradeAreasResult:
    base_url: str
    grade_id: str
    areas_url: str
    areas_response_url: str
    areas: List[IPAAreaLink] = field(default_factory=list)
    visited_urls: List[str] = field(default_factory=list)


def _compact_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_lookup_text(value: object) -> str:
    text = unicodedata.normalize("NFD", _compact_text(value))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return _compact_text(text)


def normalize_ipa_base_url(base_url: str) -> str:
    text = _compact_text(base_url)
    if not text:
        raise ValueError("Ingresa la URL base del Santadmin.")
    if not re.match(r"^https?://", text, flags=re.IGNORECASE):
        text = f"https://{text}"

    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("La URL base IPA debe incluir un dominio valido.")
    return urlunparse(parsed._replace(path="", params="", query="", fragment="")).rstrip("/")


def extract_ipa_session_value(session_value: object) -> str:
    text = _compact_text(session_value)
    if not text:
        return ""
    if text.lower().startswith("cookie:"):
        text = text.split(":", 1)[1].strip()

    cookie_prefix = f"{IPA_COOKIE_NAME}="
    if cookie_prefix in text:
        for chunk in text.split(";"):
            name, separator, value = chunk.strip().partition("=")
            if separator and name.strip() == IPA_COOKIE_NAME:
                return value.strip()
    return text


def _build_ipa_session(session_value: str) -> requests.Session:
    value = extract_ipa_session_value(session_value)
    if not value:
        raise ValueError(f"Ingresa el valor de la cookie {IPA_COOKIE_NAME}.")

    session = requests.Session()
    session.headers.update(
        {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Cookie": f"{IPA_COOKIE_NAME}={value}",
            "Pragma": "no-cache",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        }
    )
    return session


def _build_ipa_url(base_url: str, path: str) -> str:
    return urljoin(f"{normalize_ipa_base_url(base_url)}/", str(path or "").lstrip("/"))


def _normalize_response_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    return urlunparse(parsed._replace(fragment=""))


def _fetch_ipa_html(
    session: requests.Session,
    url: str,
    timeout: int,
) -> tuple[str, str]:
    try:
        response = session.get(url, timeout=int(timeout), allow_redirects=True)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red consultando IPA: {exc}") from exc

    response.encoding = response.encoding or response.apparent_encoding or "utf-8"
    response_url = _normalize_response_url(response.url)
    response_path = (urlparse(response_url).path or "").lower()
    if response.status_code in {401, 403} or "login" in response_path:
        raise RuntimeError("Sesion IPA invalida o expirada.")
    if not response.ok:
        raise RuntimeError(f"IPA devolvio HTTP {response.status_code} para {url}")
    return response.text, response_url


def _post_ipa_json(
    session: requests.Session,
    url: str,
    data: Dict[str, object],
    timeout: int,
) -> Dict[str, object]:
    try:
        response = session.post(
            url,
            data=data,
            timeout=int(timeout),
            allow_redirects=True,
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red consultando IPA: {exc}") from exc

    response.encoding = response.encoding or response.apparent_encoding or "utf-8"
    if response.status_code in {401, 403}:
        raise RuntimeError("Sesion IPA invalida o expirada.")
    if not response.ok:
        raise RuntimeError(f"IPA devolvio HTTP {response.status_code} para {url}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("IPA no devolvio JSON valido.") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("IPA devolvio una respuesta inesperada.")
    return payload


class _IPAAnchorParser(HTMLParser):
    _SKIP_TAGS = {"script", "style", "noscript", "template"}

    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.anchors: List[IPAAnchor] = []
        self._skip_depth = 0
        self._current_href = ""
        self._current_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        tag_name = str(tag or "").lower()
        if tag_name in self._SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag_name != "a" or self._current_href:
            return

        attrs_map = {str(key or "").lower(): value or "" for key, value in attrs}
        href = _compact_text(attrs_map.get("href", ""))
        if href:
            self._current_href = href
            self._current_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag_name = str(tag or "").lower()
        if tag_name in self._SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag_name != "a" or not self._current_href:
            return

        absolute_url = urljoin(self.page_url, self._current_href)
        path = urlparse(absolute_url).path or ""
        self.anchors.append(
            IPAAnchor(
                text=_compact_text(" ".join(self._current_parts)),
                href=self._current_href,
                url=absolute_url,
                path=path,
            )
        )
        self._current_href = ""
        self._current_parts = []

    def handle_data(self, data: str) -> None:
        if self._skip_depth or not self._current_href:
            return
        self._current_parts.append(data)


def parse_ipa_anchors(page_url: str, html: str) -> List[IPAAnchor]:
    parser = _IPAAnchorParser(page_url=page_url)
    parser.feed(str(html or ""))
    parser.close()
    return parser.anchors


def _extract_onclick_url(value: object) -> str:
    text = str(value or "")
    match = re.search(
        r"(?:parent|window)?\.?location(?:\.href)?\s*=\s*['\"]([^'\"]+)['\"]",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return _compact_text(match.group(1))
    return ""


class _IPATableRowParser(HTMLParser):
    _SKIP_TAGS = {"script", "style", "noscript", "template"}

    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.rows: List[Dict[str, object]] = []
        self._skip_depth = 0
        self._inside_row = False
        self._row_text_parts: List[str] = []
        self._row_anchors: List[IPAAnchor] = []
        self._current_href = ""
        self._current_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        tag_name = str(tag or "").lower()
        if tag_name in self._SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag_name == "tr":
            self._inside_row = True
            self._row_text_parts = []
            self._row_anchors = []
            return
        if not self._inside_row or tag_name != "a" or self._current_href:
            return

        attrs_map = {str(key or "").lower(): value or "" for key, value in attrs}
        href = _compact_text(attrs_map.get("href", ""))
        onclick_url = _extract_onclick_url(attrs_map.get("onclick", ""))
        if not href or href == "#" or href.lower().startswith("javascript"):
            href = onclick_url
        if href:
            self._current_href = href
            self._current_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag_name = str(tag or "").lower()
        if tag_name in self._SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag_name == "a" and self._inside_row and self._current_href:
            absolute_url = urljoin(self.page_url, self._current_href)
            self._row_anchors.append(
                IPAAnchor(
                    text=_compact_text(" ".join(self._current_parts)),
                    href=self._current_href,
                    url=absolute_url,
                    path=urlparse(absolute_url).path or "",
                )
            )
            self._current_href = ""
            self._current_parts = []
            return
        if tag_name == "tr" and self._inside_row:
            self._inside_row = False
            row_text = _compact_text(" ".join(self._row_text_parts))
            if row_text or self._row_anchors:
                self.rows.append({"text": row_text, "anchors": list(self._row_anchors)})

    def handle_data(self, data: str) -> None:
        if self._skip_depth or not self._inside_row:
            return
        self._row_text_parts.append(data)
        if self._current_href:
            self._current_parts.append(data)


def _parse_ipa_table_rows(page_url: str, html: str) -> List[Dict[str, object]]:
    parser = _IPATableRowParser(page_url=page_url)
    parser.feed(str(html or ""))
    parser.close()
    return parser.rows


class _IPATableCellRowParser(HTMLParser):
    _SKIP_TAGS = {"script", "style", "noscript", "template"}

    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.rows: List[Dict[str, object]] = []
        self._skip_depth = 0
        self._inside_row = False
        self._inside_cell = False
        self._row_cells: List[Dict[str, object]] = []
        self._row_anchors: List[IPAAnchor] = []
        self._cell_parts: List[str] = []
        self._cell_anchors: List[IPAAnchor] = []
        self._current_href = ""
        self._current_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        tag_name = str(tag or "").lower()
        if tag_name in self._SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag_name == "tr":
            self._inside_row = True
            self._row_cells = []
            self._row_anchors = []
            return
        if not self._inside_row:
            return
        if tag_name in {"td", "th"}:
            self._inside_cell = True
            self._cell_parts = []
            self._cell_anchors = []
            return
        if tag_name != "a" or self._current_href:
            return

        attrs_map = {str(key or "").lower(): value or "" for key, value in attrs}
        href = _compact_text(attrs_map.get("href", ""))
        onclick_url = _extract_onclick_url(attrs_map.get("onclick", ""))
        if not href or href == "#" or href.lower().startswith("javascript"):
            href = onclick_url
        if href:
            self._current_href = href
            self._current_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag_name = str(tag or "").lower()
        if tag_name in self._SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag_name == "a" and self._inside_row and self._current_href:
            absolute_url = urljoin(self.page_url, self._current_href)
            anchor = IPAAnchor(
                text=_compact_text(" ".join(self._current_parts)),
                href=self._current_href,
                url=absolute_url,
                path=urlparse(absolute_url).path or "",
            )
            self._row_anchors.append(anchor)
            if self._inside_cell:
                self._cell_anchors.append(anchor)
            self._current_href = ""
            self._current_parts = []
            return
        if tag_name in {"td", "th"} and self._inside_cell:
            self._row_cells.append(
                {
                    "text": _compact_text(" ".join(self._cell_parts)),
                    "anchors": list(self._cell_anchors),
                }
            )
            self._inside_cell = False
            self._cell_parts = []
            self._cell_anchors = []
            return
        if tag_name == "tr" and self._inside_row:
            if self._row_cells or self._row_anchors:
                self.rows.append(
                    {
                        "cells": list(self._row_cells),
                        "anchors": list(self._row_anchors),
                    }
                )
            self._inside_row = False
            self._inside_cell = False

    def handle_data(self, data: str) -> None:
        if self._skip_depth or not self._inside_row:
            return
        if self._inside_cell:
            self._cell_parts.append(data)
        if self._current_href:
            self._current_parts.append(data)


def _parse_ipa_table_cell_rows(page_url: str, html: str) -> List[Dict[str, object]]:
    parser = _IPATableCellRowParser(page_url=page_url)
    parser.feed(str(html or ""))
    parser.close()
    return parser.rows


class _IPASchoolCourseParser(HTMLParser):
    _SKIP_TAGS = {"script", "style", "noscript", "template"}

    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.rows: List[Dict[str, object]] = []
        self._skip_depth = 0
        self._current_grade = ""
        self._inside_grade_anchor = False
        self._grade_parts: List[str] = []
        self._inside_row = False
        self._inside_cell = False
        self._row_cells: List[Dict[str, object]] = []
        self._row_anchors: List[IPAAnchor] = []
        self._cell_parts: List[str] = []
        self._cell_anchors: List[IPAAnchor] = []
        self._current_href = ""
        self._current_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        tag_name = str(tag or "").lower()
        if tag_name in self._SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return

        attrs_map = {str(key or "").lower(): value or "" for key, value in attrs}
        if tag_name == "a" and not self._inside_row:
            href = _compact_text(attrs_map.get("href", ""))
            css_class = str(attrs_map.get("class", "") or "")
            if href.startswith("#collapse-") or "accordion-toggle" in css_class:
                self._inside_grade_anchor = True
                self._grade_parts = []
                return

        if tag_name == "tr":
            self._inside_row = True
            self._row_cells = []
            self._row_anchors = []
            return
        if not self._inside_row:
            return
        if tag_name in {"td", "th"}:
            self._inside_cell = True
            self._cell_parts = []
            self._cell_anchors = []
            return
        if tag_name != "a" or self._current_href:
            return

        href = _compact_text(attrs_map.get("href", ""))
        onclick_url = _extract_onclick_url(attrs_map.get("onclick", ""))
        if not href or href == "#" or href.lower().startswith("javascript"):
            href = onclick_url
        if href:
            self._current_href = href
            self._current_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag_name = str(tag or "").lower()
        if tag_name in self._SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag_name == "a" and self._inside_grade_anchor:
            grade = _compact_text(" ".join(self._grade_parts))
            if grade:
                self._current_grade = grade
            self._inside_grade_anchor = False
            self._grade_parts = []
            return
        if tag_name == "a" and self._inside_row and self._current_href:
            absolute_url = urljoin(self.page_url, self._current_href)
            anchor = IPAAnchor(
                text=_compact_text(" ".join(self._current_parts)),
                href=self._current_href,
                url=absolute_url,
                path=urlparse(absolute_url).path or "",
            )
            self._row_anchors.append(anchor)
            if self._inside_cell:
                self._cell_anchors.append(anchor)
            self._current_href = ""
            self._current_parts = []
            return
        if tag_name in {"td", "th"} and self._inside_cell:
            self._row_cells.append(
                {
                    "text": _compact_text(" ".join(self._cell_parts)),
                    "anchors": list(self._cell_anchors),
                }
            )
            self._inside_cell = False
            self._cell_parts = []
            self._cell_anchors = []
            return
        if tag_name == "tr" and self._inside_row:
            if self._row_cells or self._row_anchors:
                self.rows.append(
                    {
                        "grade": self._current_grade,
                        "cells": list(self._row_cells),
                        "anchors": list(self._row_anchors),
                    }
                )
            self._inside_row = False
            self._inside_cell = False

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        if self._inside_grade_anchor:
            self._grade_parts.append(data)
        if not self._inside_row:
            return
        if self._inside_cell:
            self._cell_parts.append(data)
        if self._current_href:
            self._current_parts.append(data)


def _parse_ipa_school_course_table_rows(page_url: str, html: str) -> List[Dict[str, object]]:
    parser = _IPASchoolCourseParser(page_url=page_url)
    parser.feed(str(html or ""))
    parser.close()
    return parser.rows


class _IPAFormParser(HTMLParser):
    def __init__(self, page_url: str, form_id: str = "") -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.form_id = form_id
        self.action_url = ""
        self.inputs: Dict[str, str] = {}
        self._inside_target_form = not bool(form_id)

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        tag_name = str(tag or "").lower()
        attrs_map = {str(key or "").lower(): value or "" for key, value in attrs}
        if tag_name == "form":
            if not self.form_id or attrs_map.get("id") == self.form_id:
                self._inside_target_form = True
                action = _compact_text(attrs_map.get("action", ""))
                if action:
                    self.action_url = urljoin(self.page_url, action)
            return
        if not self._inside_target_form or tag_name != "input":
            return
        name = _compact_text(attrs_map.get("name", ""))
        if not name:
            return
        self.inputs[name] = _compact_text(attrs_map.get("value", ""))

    def handle_endtag(self, tag: str) -> None:
        if str(tag or "").lower() == "form" and self._inside_target_form and self.form_id:
            self._inside_target_form = False


def _parse_ipa_form(page_url: str, html: str, form_id: str = "") -> tuple[str, Dict[str, str]]:
    parser = _IPAFormParser(page_url=page_url, form_id=form_id)
    parser.feed(str(html or ""))
    parser.close()
    return parser.action_url, parser.inputs


def parse_ipa_grade_links(page_url: str, html: str) -> List[IPAGradeLink]:
    by_grade_id: Dict[str, IPAGradeLink] = {}
    for anchor in parse_ipa_anchors(page_url, html):
        match = IPA_GRADE_LINK_RE.search(anchor.path)
        if not match:
            continue
        grade_id = match.group(1)
        if grade_id in by_grade_id:
            continue
        by_grade_id[grade_id] = IPAGradeLink(
            name=anchor.text or f"Grado {grade_id}",
            grade_id=grade_id,
            url=anchor.url,
        )
    return list(by_grade_id.values())


def _target_area_key(text: str) -> str:
    normalized = _normalize_lookup_text(text)
    if normalized in IPA_TARGET_AREAS:
        return normalized
    for area_key in IPA_TARGET_AREAS:
        if re.search(rf"(^|\s){re.escape(area_key)}(\s|$)", normalized):
            return area_key
    return ""


def parse_ipa_area_links(
    page_url: str,
    html: str,
    *,
    grade_id: str = "",
    target_only: bool = False,
) -> List[IPAAreaLink]:
    by_area_id: Dict[str, IPAAreaLink] = {}
    for anchor in parse_ipa_anchors(page_url, html):
        match = IPA_AREA_LINK_RE.search(anchor.path)
        if not match:
            continue
        area_id, link_grade_id = match.group(1), match.group(2)
        if grade_id and str(link_grade_id) != str(grade_id):
            continue
        target_area_key = _target_area_key(anchor.text)
        if target_only and not target_area_key:
            continue
        area_key = target_area_key or _normalize_lookup_text(anchor.text).replace(" ", "_")
        if not area_key:
            area_key = f"curso_{area_id}"
        dedup_key = f"{link_grade_id}:{area_id}"
        if dedup_key in by_area_id:
            continue
        by_area_id[dedup_key] = IPAAreaLink(
            name=IPA_TARGET_AREAS.get(target_area_key, anchor.text or f"Curso {area_id}"),
            area_key=area_key,
            area_id=area_id,
            grade_id=link_grade_id,
            url=anchor.url,
        )
    return list(by_area_id.values())


def _eval_segments(path: str) -> List[str]:
    match = IPA_EVAL_LINK_RE.search(path)
    if not match:
        return []
    return [segment for segment in match.group(1).split("/") if segment]


def _extract_eval_id(path: str) -> str:
    segments = _eval_segments(path)
    if "6" in segments:
        return "6"
    for segment in segments:
        if segment.isdigit():
            return segment
    return ""


def parse_ipa_progress_links(page_url: str, html: str) -> List[IPAEvaluationLink]:
    links: List[tuple[int, IPAEvaluationLink]] = []
    for anchor in parse_ipa_anchors(page_url, html):
        if IPA_EVAL_PREFIX not in anchor.path:
            continue

        segments = _eval_segments(anchor.path)
        text_is_progress = "progreso academico" in _normalize_lookup_text(anchor.text)
        path_is_progress = "6" in segments
        if not text_is_progress and not path_is_progress:
            continue

        eval_id = _extract_eval_id(anchor.path)
        priority = 0 if text_is_progress else 1
        links.append(
            (
                priority,
                IPAEvaluationLink(
                    name=anchor.text or IPA_PROGRESS_LABEL,
                    eval_id=eval_id,
                    url=anchor.url,
                ),
            )
        )
    return [item for _priority, item in sorted(links, key=lambda entry: entry[0])]


def _first_anchor_url_matching(anchors: List[IPAAnchor], pattern: str) -> str:
    for anchor in anchors:
        if pattern in anchor.path:
            return anchor.url
    return ""


def parse_ipa_evaluation_rows(page_url: str, html: str) -> List[IPAEvaluationRow]:
    rows: List[IPAEvaluationRow] = []
    seen_eval_ids: set[str] = set()
    for row in _parse_ipa_table_rows(page_url, html):
        anchors_raw = row.get("anchors")
        if not isinstance(anchors_raw, list):
            continue
        anchors = [anchor for anchor in anchors_raw if isinstance(anchor, IPAAnchor)]
        detail_anchors = [
            anchor for anchor in anchors if IPA_EVALUATION_DETAIL_RE.search(anchor.path)
        ]
        if not detail_anchors:
            continue

        detail_match = IPA_EVALUATION_DETAIL_RE.search(detail_anchors[0].path)
        if not detail_match:
            continue
        eval_id, unit_id, grade_id, area_id, eval_type = detail_match.groups()
        if eval_id in seen_eval_ids:
            continue
        seen_eval_ids.add(eval_id)

        name_candidates = [
            anchor.text
            for anchor in detail_anchors
            if anchor.text and not anchor.text.strip().isdigit() and anchor.text.strip() != eval_id
        ]
        numeric_candidates = [
            anchor.text.strip()
            for anchor in detail_anchors
            if anchor.text and anchor.text.strip().isdigit() and anchor.text.strip() != eval_id
        ]
        name = max(name_candidates, key=len) if name_candidates else f"Evaluacion {eval_id}"
        item_count = numeric_candidates[-1] if numeric_candidates else ""

        rows.append(
            IPAEvaluationRow(
                name=name,
                eval_id=eval_id,
                unit_id=unit_id,
                grade_id=grade_id,
                area_id=area_id,
                eval_type=eval_type,
                item_count=item_count,
                detail_url=detail_anchors[0].url,
                massive_planning_url=_first_anchor_url_matching(
                    anchors,
                    "/evaluaciones/list_pre_planificar_masivo/",
                ),
                preplanning_url=_first_anchor_url_matching(
                    anchors,
                    "/evaluaciones/list_pre_planificar/",
                ),
                edit_url=_first_anchor_url_matching(anchors, "/evaluaciones/editar/"),
            )
        )
    return rows


def _ipa_row_cell_texts(row: Dict[str, object]) -> List[str]:
    cells_raw = row.get("cells")
    if not isinstance(cells_raw, list):
        return []
    cells: List[str] = []
    for cell in cells_raw:
        if isinstance(cell, dict):
            cells.append(_compact_text(cell.get("text", "")))
    return cells


def _ipa_row_anchors(row: Dict[str, object]) -> List[IPAAnchor]:
    anchors_raw = row.get("anchors")
    if not isinstance(anchors_raw, list):
        return []
    return [anchor for anchor in anchors_raw if isinstance(anchor, IPAAnchor)]


def parse_ipa_school_rows(page_url: str, html: str) -> List[IPASchoolRow]:
    schools: List[IPASchoolRow] = []
    seen_school_ids: set[str] = set()
    for row in _parse_ipa_table_cell_rows(page_url, html):
        anchors = _ipa_row_anchors(row)
        school_anchor = next(
            (anchor for anchor in anchors if IPA_SCHOOL_LINK_RE.search(anchor.path)),
            None,
        )
        if school_anchor is None:
            continue

        match = IPA_SCHOOL_LINK_RE.search(school_anchor.path)
        if not match:
            continue
        school_id = match.group(1)
        if school_id in seen_school_ids:
            continue
        seen_school_ids.add(school_id)

        cells = _ipa_row_cell_texts(row)
        id_index = next(
            (index for index, value in enumerate(cells) if value == school_id),
            -1,
        )
        if id_index < 0:
            id_index = 1 if len(cells) > 1 else 0

        name = cells[id_index + 2] if len(cells) > id_index + 2 else school_anchor.text
        schools.append(
            IPASchoolRow(
                school_id=school_id,
                lms_id=cells[id_index + 1] if len(cells) > id_index + 1 else "",
                name=name or school_anchor.text or f"Colegio {school_id}",
                timezone=cells[id_index + 3] if len(cells) > id_index + 3 else "",
                pleno_offline=cells[id_index + 4] if len(cells) > id_index + 4 else "",
                demo=cells[id_index + 5] if len(cells) > id_index + 5 else "",
                url=school_anchor.url,
            )
        )
    return schools


def parse_ipa_school_course_rows(page_url: str, html: str) -> List[IPASchoolCourseRow]:
    courses: List[IPASchoolCourseRow] = []
    seen_course_ids: set[str] = set()
    for row in _parse_ipa_school_course_table_rows(page_url, html):
        anchors = _ipa_row_anchors(row)
        course_anchor = next(
            (anchor for anchor in anchors if IPA_COURSE_LINK_RE.search(anchor.path)),
            None,
        )
        if course_anchor is None:
            continue

        match = IPA_COURSE_LINK_RE.search(course_anchor.path)
        if not match:
            continue
        course_id, school_id = match.groups()
        if course_id in seen_course_ids:
            continue
        seen_course_ids.add(course_id)

        cells = _ipa_row_cell_texts(row)
        courses.append(
            IPASchoolCourseRow(
                grade_name=_compact_text(row.get("grade", "")),
                course_id=course_id,
                name=cells[1] if len(cells) > 1 else course_anchor.text,
                school_year_id=cells[2] if len(cells) > 2 else "",
                school_year=cells[3] if len(cells) > 3 else "",
                students=cells[4] if len(cells) > 4 else "",
                school_id=school_id,
                url=course_anchor.url,
            )
        )
    return courses


def fetch_ipa_course_progress(
    session_value: str,
    course_url: str,
    *,
    timeout: int = IPA_DEFAULT_TIMEOUT,
) -> IPACourseProgressResult:
    course_url_clean = _normalize_response_url(course_url)
    if not course_url_clean:
        raise ValueError("No se recibio la URL del curso.")
    session = _build_ipa_session(session_value)
    course_html, course_response_url = _fetch_ipa_html(
        session,
        course_url_clean,
        timeout=int(timeout),
    )
    progress_links = parse_ipa_progress_links(course_response_url, course_html)
    if not progress_links:
        raise RuntimeError("No se encontro el enlace de Progreso academico para este curso.")

    progress_link = progress_links[0]
    progress_html, progress_response_url = _fetch_ipa_html(
        session,
        progress_link.url,
        timeout=int(timeout),
    )
    return IPACourseProgressResult(
        course_url=course_url_clean,
        course_response_url=course_response_url,
        progress_url=progress_link.url,
        progress_response_url=progress_response_url,
        evaluations=parse_ipa_evaluation_rows(progress_response_url, progress_html),
        visited_urls=[course_response_url, progress_response_url],
    )


def build_ipa_course_progress_payload(result: IPACourseProgressResult) -> Dict[str, object]:
    return {
        "courseUrl": result.course_url,
        "courseResponseUrl": result.course_response_url,
        "progressUrl": result.progress_url,
        "progressResponseUrl": result.progress_response_url,
        "visitedUrls": result.visited_urls,
        "evaluaciones": [
            {
                "nombre": evaluation.name,
                "evalId": evaluation.eval_id,
                "unidadId": evaluation.unit_id,
                "gradoId": evaluation.grade_id,
                "areaId": evaluation.area_id,
                "tipo": evaluation.eval_type,
                "items": evaluation.item_count,
                "detalleUrl": evaluation.detail_url,
                "planificacionMasivaUrl": evaluation.massive_planning_url,
                "preplanificarUrl": evaluation.preplanning_url,
                "editarUrl": evaluation.edit_url,
            }
            for evaluation in result.evaluations
        ],
    }


def build_ipa_massive_planificaciones_url(massive_planning_url: str) -> str:
    clean_url = _normalize_response_url(massive_planning_url)
    match = IPA_MASSIVE_PLANNING_RE.search(urlparse(clean_url).path)
    if not match:
        return ""
    eval_id, unit_id, area_id, grade_id, eval_type = match.groups()
    parsed = urlparse(clean_url)
    query = urlencode(
        {
            "id_prueba": eval_id,
            "id_unidad": unit_id,
            "id_area": area_id,
            "id_nivel_curriculum": grade_id,
            "tipo": eval_type,
        }
    )
    return urlunparse(
        parsed._replace(path="/evaluaciones/load_planificaciones", params="", query=query, fragment="")
    )


def build_ipa_add_course_url(
    massive_planning_url: str,
    id_lote: object,
    *,
    eval_id: object = "",
    unit_id: object = "",
    area_id: object = "",
    grade_id: object = "",
    eval_type: object = "",
) -> str:
    clean_url = _normalize_response_url(massive_planning_url)
    parsed = urlparse(clean_url)
    if not parsed.scheme or not parsed.netloc:
        return ""

    if not all(str(value or "").strip() for value in (eval_id, unit_id, area_id, grade_id, eval_type)):
        match = IPA_MASSIVE_PLANNING_RE.search(parsed.path)
        if match:
            eval_id, unit_id, area_id, grade_id, eval_type = match.groups()

    values = [
        str(id_lote or "").strip(),
        str(eval_id or "").strip(),
        str(unit_id or "").strip(),
        str(area_id or "").strip(),
        str(grade_id or "").strip(),
        str(eval_type or "").strip(),
    ]
    if not all(values):
        return ""
    path = "/evaluaciones/agregar_curso/" + "/".join(values)
    return urlunparse(parsed._replace(path=path, params="", query="", fragment=""))


def parse_ipa_add_course_url(add_course_url: str) -> Dict[str, str]:
    clean_url = _normalize_response_url(add_course_url)
    match = IPA_ADD_COURSE_RE.search(urlparse(clean_url).path)
    if not match:
        return {}
    id_lote, eval_id, unit_id, area_id, grade_id, eval_type = match.groups()
    return {
        "id_lote": id_lote,
        "id_prueba": eval_id,
        "id_unidad": unit_id,
        "id_area": area_id,
        "id_nivel": grade_id,
        "tipo": eval_type,
    }


def _build_ipa_form_post_url(add_course_url: str, action_path: str) -> str:
    parsed = urlparse(_normalize_response_url(add_course_url))
    return urlunparse(parsed._replace(path=action_path, params="", query="", fragment=""))


def fetch_ipa_add_course_context(
    session_value: str,
    add_course_url: str,
    *,
    timeout: int = IPA_DEFAULT_TIMEOUT,
) -> Dict[str, object]:
    clean_url = _normalize_response_url(add_course_url)
    if not clean_url:
        raise ValueError("No se recibio la URL para agregar cursos.")
    session = _build_ipa_session(session_value)
    html, response_url = _fetch_ipa_html(session, clean_url, timeout=int(timeout))
    action_url, inputs = _parse_ipa_form(response_url, html, "agregar_curso_masivo")
    params = parse_ipa_add_course_url(response_url)
    params.update({key: value for key, value in inputs.items() if value})
    if "id_pais" not in params:
        params["id_pais"] = "8"
    return {
        "addCourseUrl": clean_url,
        "responseUrl": response_url,
        "formActionUrl": action_url
        or _build_ipa_form_post_url(response_url, "/evaluaciones/agregar_curso_masivo"),
        "searchUrl": _build_ipa_form_post_url(response_url, "/evaluaciones/buscar_prueba_curso"),
        "params": params,
    }


def _normalize_add_course_context(context: Dict[str, object]) -> Dict[str, str]:
    params = context.get("params") if isinstance(context, dict) else {}
    if not isinstance(params, dict):
        params = {}
    normalized = {str(key): str(value or "").strip() for key, value in params.items()}
    required = ["id_prueba", "id_area", "id_nivel", "tipo", "id_lote", "id_pais"]
    missing = [key for key in required if not normalized.get(key)]
    if missing:
        raise ValueError(f"Faltan parametros para agregar curso: {', '.join(missing)}.")
    return normalized


def search_ipa_course_for_planificacion(
    session_value: str,
    context: Dict[str, object],
    curso_id: object,
    *,
    timeout: int = IPA_DEFAULT_TIMEOUT,
) -> Dict[str, object]:
    curso_id_clean = _compact_text(curso_id)
    if not curso_id_clean:
        raise ValueError("Ingresa el ID del curso.")
    params = _normalize_add_course_context(context)
    search_url = str(context.get("searchUrl") or "").strip()
    if not search_url:
        add_url = str(context.get("responseUrl") or context.get("addCourseUrl") or "").strip()
        search_url = _build_ipa_form_post_url(add_url, "/evaluaciones/buscar_prueba_curso")
    payload = {
        "curso_id": curso_id_clean,
        "id_prueba": params["id_prueba"],
        "id_area": params["id_area"],
        "id_nivel": params["id_nivel"],
        "tipo": params["tipo"],
        "id_lote": params["id_lote"],
        "id_pais": params["id_pais"],
    }
    return _post_ipa_json(
        _build_ipa_session(session_value),
        search_url,
        payload,
        timeout=int(timeout),
    )


def add_ipa_course_to_planificacion(
    session_value: str,
    context: Dict[str, object],
    curso_id: object,
    *,
    timeout: int = IPA_DEFAULT_TIMEOUT,
) -> Dict[str, object]:
    curso_id_clean = _compact_text(curso_id)
    if not curso_id_clean:
        raise ValueError("Ingresa el ID del curso.")
    params = _normalize_add_course_context(context)
    form_action_url = str(context.get("formActionUrl") or "").strip()
    if not form_action_url:
        add_url = str(context.get("responseUrl") or context.get("addCourseUrl") or "").strip()
        form_action_url = _build_ipa_form_post_url(add_url, "/evaluaciones/agregar_curso_masivo")
    payload = {
        "curso_id": curso_id_clean,
        "id_prueba": params["id_prueba"],
        "id_area": params["id_area"],
        "id_nivel": params["id_nivel"],
        "tipo": params["tipo"],
        "id_lote": params["id_lote"],
        "id_pais": params["id_pais"],
    }
    return _post_ipa_json(
        _build_ipa_session(session_value),
        form_action_url,
        payload,
        timeout=int(timeout),
    )


def fetch_ipa_massive_planificaciones(
    session_value: str,
    massive_planning_url: str,
    *,
    timeout: int = IPA_DEFAULT_TIMEOUT,
) -> Dict[str, object]:
    ajax_url = build_ipa_massive_planificaciones_url(massive_planning_url)
    if not ajax_url:
        raise ValueError("No se pudo construir la URL AJAX de planificaciones.")
    session = _build_ipa_session(session_value)
    try:
        response = session.get(ajax_url, timeout=int(timeout), allow_redirects=True)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red consultando planificaciones IPA: {exc}") from exc
    response.encoding = response.encoding or response.apparent_encoding or "utf-8"
    if response.status_code in {401, 403}:
        raise RuntimeError("Sesion IPA invalida o expirada.")
    if not response.ok:
        raise RuntimeError(f"IPA devolvio HTTP {response.status_code} para {ajax_url}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("IPA no devolvio JSON valido para planificaciones.") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("IPA devolvio una respuesta de planificaciones inesperada.")
    payload["ajaxUrl"] = ajax_url
    payload["responseUrl"] = _normalize_response_url(response.url)
    return payload


def fetch_ipa_schools_listing(
    session_value: str,
    base_url: str = "",
    *,
    timeout: int = IPA_DEFAULT_TIMEOUT,
) -> IPASchoolListingResult:
    listing_url = IPA_SCHOOLS_URL
    session = _build_ipa_session(session_value)
    html, response_url = _fetch_ipa_html(session, listing_url, timeout=int(timeout))
    schools = parse_ipa_school_rows(response_url, html)
    if not schools:
        raise RuntimeError("No se encontraron colegios en el listado.")
    return IPASchoolListingResult(
        listing_url=listing_url,
        listing_response_url=response_url,
        schools=schools,
        visited_urls=[response_url],
    )


def _build_ipa_school_url(base_url: str, school_id: str) -> str:
    school_id_clean = _compact_text(school_id)
    if not school_id_clean:
        return ""
    base_url_clean = base_url or IPA_SCHOOLS_BASE_URL
    return _build_ipa_url(base_url_clean, f"/colegios/volver_colegio/{school_id_clean}")


def fetch_ipa_school_courses(
    session_value: str,
    school_url_or_id: str,
    *,
    base_url: str = "",
    school_id: str = "",
    school_name: str = "",
    timeout: int = IPA_DEFAULT_TIMEOUT,
) -> IPASchoolCoursesResult:
    target = _compact_text(school_url_or_id)
    school_id_clean = _compact_text(school_id)
    if re.match(r"^https?://", target, flags=re.IGNORECASE):
        school_url = _normalize_response_url(target)
    elif target.startswith("/"):
        school_url = _build_ipa_url(base_url or IPA_SCHOOLS_BASE_URL, target)
        match = IPA_SCHOOL_LINK_RE.search(urlparse(school_url).path)
        school_id_clean = school_id_clean or (match.group(1) if match else "")
    else:
        school_id_clean = school_id_clean or target
        school_url = _build_ipa_school_url(base_url, school_id_clean)

    if not school_url:
        raise ValueError("No se recibio la URL o ID del colegio.")

    session = _build_ipa_session(session_value)
    html, response_url = _fetch_ipa_html(session, school_url, timeout=int(timeout))
    courses = parse_ipa_school_course_rows(response_url, html)
    if not school_id_clean:
        match = IPA_SCHOOL_LINK_RE.search(urlparse(response_url).path) or re.search(
            r"/colegios/[^/]+/(\d+)(?:/|$)",
            urlparse(response_url).path,
        )
        school_id_clean = match.group(1) if match else ""
    if not school_id_clean and courses:
        school_id_clean = courses[0].school_id

    return IPASchoolCoursesResult(
        school_id=school_id_clean,
        school_name=_compact_text(school_name),
        school_url=school_url,
        response_url=response_url,
        courses=courses,
        visited_urls=[response_url],
    )


def fetch_ipa_structure(
    session_value: str,
    base_url: str = "",
    *,
    nivel_id: int = IPA_DEFAULT_NIVEL_ID,
    timeout: int = IPA_DEFAULT_TIMEOUT,
    include_progress: bool = False,
    target_areas_only: bool = False,
) -> IPAExtractionResult:
    base_url_clean = normalize_ipa_base_url(base_url or IPA_SCHOOLS_BASE_URL)
    session = _build_ipa_session(session_value)

    level_url = _build_ipa_url(
        base_url_clean,
        IPA_GRADES_PATH_TEMPLATE.format(nivel_id=int(nivel_id)),
    )
    level_html, level_response_url = _fetch_ipa_html(
        session,
        level_url,
        timeout=int(timeout),
    )
    visited_urls = [level_response_url]
    grade_links = parse_ipa_grade_links(level_response_url, level_html)
    if not grade_links:
        raise RuntimeError("No se encontraron enlaces de grados en IPA.")

    grades: List[IPAGradeResult] = []
    for grade_link in grade_links:
        areas_html, areas_response_url = _fetch_ipa_html(
            session,
            grade_link.url,
            timeout=int(timeout),
        )
        visited_urls.append(areas_response_url)
        areas = parse_ipa_area_links(
            areas_response_url,
            areas_html,
            grade_id=grade_link.grade_id,
            target_only=target_areas_only,
        )

        communication_progress: Optional[IPAEvaluationLink] = None
        communication_area = next(
            (area for area in areas if area.area_key == "comunicacion"),
            None,
        )
        if include_progress and communication_area is not None:
            units_html, units_response_url = _fetch_ipa_html(
                session,
                communication_area.url,
                timeout=int(timeout),
            )
            visited_urls.append(units_response_url)
            progress_links = parse_ipa_progress_links(units_response_url, units_html)
            if progress_links:
                communication_progress = progress_links[0]

        grades.append(
            IPAGradeResult(
                name=grade_link.name,
                grade_id=grade_link.grade_id,
                areas_url=grade_link.url,
                areas=areas,
                communication_progress=communication_progress,
            )
        )

    return IPAExtractionResult(
        base_url=base_url_clean,
        level_url=level_url,
        level_response_url=level_response_url,
        grades=grades,
        visited_urls=visited_urls,
    )


def fetch_ipa_grade_listing(
    session_value: str,
    base_url: str = "",
    *,
    nivel_id: int = IPA_DEFAULT_NIVEL_ID,
    timeout: int = IPA_DEFAULT_TIMEOUT,
) -> IPAGradeListingResult:
    base_url_clean = normalize_ipa_base_url(base_url or IPA_SCHOOLS_BASE_URL)
    session = _build_ipa_session(session_value)

    level_url = _build_ipa_url(
        base_url_clean,
        IPA_GRADES_PATH_TEMPLATE.format(nivel_id=int(nivel_id)),
    )
    level_html, level_response_url = _fetch_ipa_html(
        session,
        level_url,
        timeout=int(timeout),
    )
    grades = parse_ipa_grade_links(level_response_url, level_html)
    if not grades:
        raise RuntimeError("No se encontraron grados en IPA.")

    return IPAGradeListingResult(
        base_url=base_url_clean,
        level_url=level_url,
        level_response_url=level_response_url,
        grades=grades,
        visited_urls=[level_response_url],
    )


def fetch_ipa_grade_areas(
    session_value: str,
    grade_id: object,
    base_url: str = "",
    *,
    timeout: int = IPA_DEFAULT_TIMEOUT,
    target_areas_only: bool = False,
) -> IPAGradeAreasResult:
    grade_id_clean = _compact_text(grade_id)
    if not grade_id_clean:
        raise ValueError("Ingresa el ID del grado.")

    base_url_clean = normalize_ipa_base_url(base_url or IPA_SCHOOLS_BASE_URL)
    session = _build_ipa_session(session_value)
    areas_url = _build_ipa_url(
        base_url_clean,
        IPA_AREAS_PATH_TEMPLATE.format(grade_id=grade_id_clean),
    )
    areas_html, areas_response_url = _fetch_ipa_html(
        session,
        areas_url,
        timeout=int(timeout),
    )
    areas = parse_ipa_area_links(
        areas_response_url,
        areas_html,
        grade_id=grade_id_clean,
        target_only=bool(target_areas_only),
    )
    if not areas:
        raise RuntimeError("No se encontraron areas para este grado.")

    return IPAGradeAreasResult(
        base_url=base_url_clean,
        grade_id=grade_id_clean,
        areas_url=areas_url,
        areas_response_url=areas_response_url,
        areas=areas,
        visited_urls=[areas_response_url],
    )


def build_ipa_schools_payload(result: IPASchoolListingResult) -> Dict[str, object]:
    return {
        "listingUrl": result.listing_url,
        "listingResponseUrl": result.listing_response_url,
        "visitedUrls": result.visited_urls,
        "colegios": [
            {
                "id": school.school_id,
                "idLms": school.lms_id,
                "nombre": school.name,
                "husoHorario": school.timezone,
                "plenoOffline": school.pleno_offline,
                "demo": school.demo,
                "url": school.url,
            }
            for school in result.schools
        ],
    }


def build_ipa_school_courses_payload(result: IPASchoolCoursesResult) -> Dict[str, object]:
    grouped: Dict[str, List[Dict[str, object]]] = {}
    courses_payload: List[Dict[str, object]] = []
    for course in result.courses:
        course_payload: Dict[str, object] = {
            "grado": course.grade_name,
            "id": course.course_id,
            "nombre": course.name,
            "idAnoLectivo": course.school_year_id,
            "anoLectivo": course.school_year,
            "alumnos": course.students,
            "colegioId": course.school_id,
            "url": course.url,
        }
        courses_payload.append(course_payload)
        grouped.setdefault(course.grade_name or "Sin grado", []).append(course_payload)

    return {
        "colegio": {
            "id": result.school_id,
            "nombre": result.school_name,
            "url": result.school_url,
            "responseUrl": result.response_url,
        },
        "visitedUrls": result.visited_urls,
        "cursos": courses_payload,
        "grados": [
            {"nombre": grade_name, "cursos": courses}
            for grade_name, courses in grouped.items()
        ],
    }


def build_ipa_result_payload(result: IPAExtractionResult) -> Dict[str, object]:
    grades_payload: List[Dict[str, object]] = []
    for grade in result.grades:
        areas_payload: List[Dict[str, object]] = []
        for area in grade.areas:
            area_payload: Dict[str, object] = {
                "nombre": area.name,
                "areaId": area.area_id,
                "gradoId": area.grade_id,
                "url": area.url,
            }
            if area.area_key == "comunicacion":
                area_payload["progresoAcademico"] = (
                    {
                        "evalId": grade.communication_progress.eval_id,
                        "url": grade.communication_progress.url,
                        "texto": grade.communication_progress.name,
                    }
                    if grade.communication_progress is not None
                    else None
                )
            areas_payload.append(area_payload)

        grades_payload.append(
            {
                "grado": {
                    "nombre": grade.name,
                    "id": grade.grade_id,
                    "url": grade.areas_url,
                },
                "areas": areas_payload,
            }
        )

    return {
        "baseUrl": result.base_url,
        "nivelUrl": result.level_url,
        "nivelResponseUrl": result.level_response_url,
        "visitedUrls": result.visited_urls,
        "grados": grades_payload,
    }


def build_ipa_result_rows(result: IPAExtractionResult) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for grade in result.grades:
        progress = grade.communication_progress
        for area in grade.areas:
            rows.append(
                {
                    "Grado": grade.name,
                    "Grado ID": grade.grade_id,
                    "Area": area.name,
                    "Area ID": area.area_id,
                    "Area URL": area.url,
                    "Eval ID": progress.eval_id
                    if area.area_key == "comunicacion" and progress is not None
                    else "",
                    "Evaluacion": progress.name
                    if area.area_key == "comunicacion" and progress is not None
                    else "",
                    "Evaluacion URL": progress.url
                    if area.area_key == "comunicacion" and progress is not None
                    else "",
                }
            )
    return rows


def build_ipa_result_json_bytes(result: IPAExtractionResult) -> bytes:
    return json.dumps(
        build_ipa_result_payload(result),
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")


def build_ipa_result_excel_bytes(result: IPAExtractionResult) -> bytes:
    output = BytesIO()
    rows = build_ipa_result_rows(result)
    export_df = pd.DataFrame(
        rows,
        columns=[
            "Grado",
            "Grado ID",
            "Area",
            "Area ID",
            "Area URL",
            "Eval ID",
            "Evaluacion",
            "Evaluacion URL",
        ],
    )
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="IPA")
    output.seek(0)
    return output.getvalue()


def build_ipa_result_filename(result: IPAExtractionResult, extension: str = "xlsx") -> str:
    extension_clean = str(extension or "xlsx").strip().lstrip(".") or "xlsx"
    match = re.search(r"/niveles/mantenedor_preguntas_nivel/(\d+)", result.level_url)
    nivel_id = match.group(1) if match else str(IPA_DEFAULT_NIVEL_ID)
    return f"ipa_progreso_academico_nivel_{nivel_id}.{extension_clean}"
