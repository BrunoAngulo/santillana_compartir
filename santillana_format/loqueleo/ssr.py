from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from html.parser import HTMLParser
from io import BytesIO
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests

LOQUELEO_BASE_URL = "https://loqueleodigital.com"
LOQUELEO_ALLOWED_HOST = "loqueleodigital.com"
LOQUELEO_DEFAULT_TIMEOUT = 30
LOQUELEO_MAX_PAGES = 100
LOQUELEO_USER_TYPE_LABELS = {
    "User::Teacher": "Profesores",
    "User::Student": "Alumnos",
    "User::Admin": "Administradores",
}


@dataclass
class LoqueleoParsedRow:
    name: str
    account: str
    detail_url: str = ""
    detail_path: str = ""
    user_id: str = ""
    action_label: str = ""
    action_url: str = ""
    action_path: str = ""


@dataclass
class LoqueleoUsersPage:
    page_url: str
    response_url: str
    title: str = ""
    heading: str = ""
    breadcrumb: List[str] = field(default_factory=list)
    total_text: str = ""
    total_records: Optional[int] = None
    csrf_token: str = ""
    next_url: str = ""
    rows: List[LoqueleoParsedRow] = field(default_factory=list)


@dataclass
class LoqueleoUsersExport:
    input_url: str
    first_response_url: str
    final_response_url: str
    organization_id: str
    organization_name: str
    user_type: str
    user_type_label: str
    locale: str
    year: str
    csrf_token: str
    reported_total: Optional[int]
    rows: List[Dict[str, object]]
    visited_pages: List[str]
    page_count: int
    stop_reason: str


def _compact_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_loqueleo_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        raise ValueError("Ingresa una URL de Loqueleo.")

    if text.startswith("/"):
        text = urljoin(LOQUELEO_BASE_URL, text)

    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("La URL debe comenzar con http:// o https://.")
    host = (parsed.hostname or "").lower()
    if host not in {LOQUELEO_ALLOWED_HOST, f"www.{LOQUELEO_ALLOWED_HOST}"}:
        raise ValueError("La URL debe pertenecer a loqueleodigital.com.")
    return urlunparse(parsed._replace(fragment=""))


def _build_cookie_dict(session_id: str, cookie_header: str = "") -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    for chunk in str(cookie_header or "").split(";"):
        name, separator, value = chunk.strip().partition("=")
        if separator and name.strip():
            cookies[name.strip()] = value.strip()
    cookies["_session_id"] = str(session_id or "").strip()
    return cookies


def _build_loqueleo_session(
    session_id: str,
    cookie_header: str = "",
) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
        }
    )
    for name, value in _build_cookie_dict(session_id, cookie_header).items():
        session.cookies.set(name, value, domain=LOQUELEO_ALLOWED_HOST)
    return session


def _extract_page_number(url: str) -> int:
    try:
        page_raw = parse_qs(urlparse(url).query).get("page", ["1"])[0]
        page_int = int(str(page_raw or "1").strip())
        return max(page_int, 1)
    except (TypeError, ValueError):
        return 1


def _increment_page_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["page"] = [str(_extract_page_number(url) + 1)]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def _extract_organization_id(url: str) -> str:
    match = re.search(r"/organizations/(\d+)", url)
    return str(match.group(1)) if match else ""


def _extract_user_id_from_path(path: str) -> str:
    match = re.search(r"/users/(\d+)", str(path or ""))
    return str(match.group(1)) if match else ""


def _normalize_filename_part(value: object, default: str) -> str:
    text = _compact_text(value)
    if not text:
        return default
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_") or default


def _metadata_rows(result: LoqueleoUsersExport) -> List[Dict[str, object]]:
    return [
        {"Campo": "URL inicial", "Valor": result.input_url},
        {"Campo": "Primera respuesta", "Valor": result.first_response_url},
        {"Campo": "Ultima respuesta", "Valor": result.final_response_url},
        {"Campo": "Organizacion ID", "Valor": result.organization_id},
        {"Campo": "Organizacion", "Valor": result.organization_name},
        {"Campo": "Tipo", "Valor": result.user_type},
        {"Campo": "Tipo etiqueta", "Valor": result.user_type_label},
        {"Campo": "Idioma", "Valor": result.locale},
        {"Campo": "Ano", "Valor": result.year},
        {"Campo": "CSRF token", "Valor": result.csrf_token},
        {"Campo": "Total reportado", "Valor": result.reported_total or ""},
        {"Campo": "Usuarios consolidados", "Valor": len(result.rows)},
        {"Campo": "Paginas visitadas", "Valor": result.page_count},
        {"Campo": "Motivo de parada", "Valor": result.stop_reason},
    ]


def build_loqueleo_users_excel_bytes(result: LoqueleoUsersExport) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(result.rows).to_excel(
            writer,
            index=False,
            sheet_name=result.user_type_label[:31] or "Usuarios",
        )
        pd.DataFrame(_metadata_rows(result)).to_excel(
            writer,
            index=False,
            sheet_name="Resumen",
        )
        pd.DataFrame(
            [{"Pagina": idx + 1, "URL": url} for idx, url in enumerate(result.visited_pages)]
        ).to_excel(
            writer,
            index=False,
            sheet_name="Paginas",
        )
    output.seek(0)
    return output.getvalue()


def build_loqueleo_users_filename(result: LoqueleoUsersExport) -> str:
    organization_part = _normalize_filename_part(result.organization_name, "organizacion")
    type_part = _normalize_filename_part(result.user_type_label, "usuarios")
    year_part = _normalize_filename_part(result.year, "sin_ano")
    return f"loqueleo_{organization_part}_{type_part}_{year_part}.xlsx"


class _LoqueleoUsersPageParser(HTMLParser):
    def __init__(self, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.title = ""
        self.heading = ""
        self.breadcrumb: List[str] = []
        self.total_text = ""
        self.total_records: Optional[int] = None
        self.csrf_token = ""
        self.next_url = ""
        self.rows: List[LoqueleoParsedRow] = []

        self._inside_title = False
        self._title_parts: List[str] = []
        self._inside_heading = False
        self._heading_parts: List[str] = []

        self._inside_breadcrumb = False
        self._inside_breadcrumb_item = False
        self._breadcrumb_parts: List[str] = []

        self._inside_total = False
        self._total_parts: List[str] = []

        self._inside_table = False
        self._table_headers: List[str] = []
        self._current_row_cells: List[Dict[str, str]] = []
        self._current_cell_parts: List[str] = []
        self._current_cell_href = ""
        self._current_cell_tag = ""

        self._inside_pagination = False
        self._pagination_anchor: Optional[Dict[str, object]] = None

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attrs_map = {key: value or "" for key, value in attrs}
        class_attr = attrs_map.get("class", "")

        if tag == "meta" and attrs_map.get("name") == "csrf-token":
            self.csrf_token = _compact_text(attrs_map.get("content", ""))
            return

        if tag == "title":
            self._inside_title = True
            self._title_parts = []
            return

        if tag == "h2" and not self.heading:
            self._inside_heading = True
            self._heading_parts = []
            return

        if tag == "ul" and "breadcrumb" in class_attr:
            self._inside_breadcrumb = True
            return

        if self._inside_breadcrumb and tag == "li":
            self._inside_breadcrumb_item = True
            self._breadcrumb_parts = []
            return

        if tag == "div" and "pull-right" in class_attr:
            self._inside_total = True
            self._total_parts = []
            return

        if (
            tag == "table"
            and not self._inside_table
            and "table" in class_attr
            and "table-condensed" in class_attr
        ):
            self._inside_table = True
            self._table_headers = []
            return

        if self._inside_table and tag == "tr":
            self._current_row_cells = []
            return

        if self._inside_table and tag in {"th", "td"}:
            self._current_cell_tag = tag
            self._current_cell_parts = []
            self._current_cell_href = ""
            return

        if self._inside_table and self._current_cell_tag and tag == "a":
            href = _compact_text(attrs_map.get("href", ""))
            if href and not self._current_cell_href:
                self._current_cell_href = urljoin(self.page_url, href)
            return

        if tag == "ul" and "pagination" in class_attr:
            self._inside_pagination = True
            return

        if self._inside_pagination and tag == "a":
            rel_values = {
                token.strip().lower()
                for token in _compact_text(attrs_map.get("rel", "")).split()
                if token.strip()
            }
            self._pagination_anchor = {
                "href": urljoin(self.page_url, _compact_text(attrs_map.get("href", ""))),
                "rel": rel_values,
                "text_parts": [],
            }

    def handle_endtag(self, tag: str) -> None:
        if tag == "title" and self._inside_title:
            self._inside_title = False
            self.title = _compact_text(" ".join(self._title_parts))
            return

        if tag == "h2" and self._inside_heading:
            self._inside_heading = False
            self.heading = _compact_text(" ".join(self._heading_parts))
            return

        if tag == "li" and self._inside_breadcrumb_item:
            self._inside_breadcrumb_item = False
            item_text = _compact_text(" ".join(self._breadcrumb_parts))
            if item_text:
                self.breadcrumb.append(item_text)
            return

        if tag == "ul" and self._inside_breadcrumb:
            self._inside_breadcrumb = False
            return

        if tag == "div" and self._inside_total:
            self._inside_total = False
            self.total_text = _compact_text(" ".join(self._total_parts))
            match = re.search(r"Total:\s*(\d+)", self.total_text, flags=re.IGNORECASE)
            if match:
                self.total_records = int(match.group(1))
            return

        if self._inside_table and tag in {"th", "td"} and self._current_cell_tag == tag:
            self._current_row_cells.append(
                {
                    "tag": self._current_cell_tag,
                    "text": _compact_text(" ".join(self._current_cell_parts)),
                    "href": self._current_cell_href,
                }
            )
            self._current_cell_tag = ""
            self._current_cell_parts = []
            self._current_cell_href = ""
            return

        if self._inside_table and tag == "tr":
            if self._current_row_cells:
                if all(cell.get("tag") == "th" for cell in self._current_row_cells):
                    self._table_headers = [cell.get("text", "") for cell in self._current_row_cells]
                else:
                    data_cells = [cell for cell in self._current_row_cells if cell.get("tag") == "td"]
                    if len(data_cells) >= 2:
                        detail_url = data_cells[0].get("href", "")
                        action_cell = data_cells[2] if len(data_cells) >= 3 else {}
                        detail_path = urlparse(detail_url).path if detail_url else ""
                        action_url = action_cell.get("href", "")
                        action_path = urlparse(action_url).path if action_url else ""
                        self.rows.append(
                            LoqueleoParsedRow(
                                name=data_cells[0].get("text", ""),
                                account=data_cells[1].get("text", ""),
                                detail_url=detail_url,
                                detail_path=detail_path,
                                user_id=_extract_user_id_from_path(detail_path),
                                action_label=action_cell.get("text", ""),
                                action_url=action_url,
                                action_path=action_path,
                            )
                        )
            self._current_row_cells = []
            return

        if tag == "table" and self._inside_table:
            self._inside_table = False
            return

        if self._inside_pagination and tag == "a" and self._pagination_anchor is not None:
            text = _compact_text(" ".join(self._pagination_anchor["text_parts"]))
            rel_values = set(self._pagination_anchor.get("rel", set()))
            href = _compact_text(self._pagination_anchor.get("href", ""))
            if href and (
                "next" in rel_values or "siguiente" in text.lower() or "next" in text.lower()
            ):
                self.next_url = href
            self._pagination_anchor = None
            return

        if tag == "ul" and self._inside_pagination:
            self._inside_pagination = False

    def handle_data(self, data: str) -> None:
        if self._inside_title:
            self._title_parts.append(data)
        if self._inside_heading:
            self._heading_parts.append(data)
        if self._inside_breadcrumb_item:
            self._breadcrumb_parts.append(data)
        if self._inside_total:
            self._total_parts.append(data)
        if self._inside_table and self._current_cell_tag:
            self._current_cell_parts.append(data)
        if self._pagination_anchor is not None:
            self._pagination_anchor["text_parts"].append(data)


def parse_loqueleo_users_page(page_url: str, html: str, response_url: str) -> LoqueleoUsersPage:
    parser = _LoqueleoUsersPageParser(page_url=response_url or page_url)
    parser.feed(str(html or ""))
    parser.close()
    return LoqueleoUsersPage(
        page_url=page_url,
        response_url=response_url,
        title=parser.title,
        heading=parser.heading,
        breadcrumb=parser.breadcrumb,
        total_text=parser.total_text,
        total_records=parser.total_records,
        csrf_token=parser.csrf_token,
        next_url=parser.next_url,
        rows=parser.rows,
    )


def _fetch_loqueleo_users_page(
    session: requests.Session,
    page_url: str,
    timeout: int,
) -> LoqueleoUsersPage:
    try:
        response = session.get(page_url, timeout=int(timeout), allow_redirects=True)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red consultando Loqueleo: {exc}") from exc

    response.encoding = response.encoding or "utf-8"
    response_url = _normalize_loqueleo_url(response.url)
    response_path = (urlparse(response_url).path or "").lower()
    if response.status_code in {401, 403} or response_path.startswith("/login"):
        raise RuntimeError("Sesion Loqueleo invalida o expirada.")
    if response.status_code == 404:
        raise FileNotFoundError(f"Loqueleo devolvio 404 para {page_url}")
    if not response.ok:
        raise RuntimeError(f"Loqueleo devolvio HTTP {response.status_code} para {page_url}")

    parsed_page = parse_loqueleo_users_page(
        page_url=page_url,
        html=response.text,
        response_url=response_url,
    )
    return parsed_page


def fetch_loqueleo_users_listing(
    session_id: str,
    listing_url: str,
    *,
    cookie_header: str = "",
    timeout: int = LOQUELEO_DEFAULT_TIMEOUT,
    max_pages: int = LOQUELEO_MAX_PAGES,
) -> LoqueleoUsersExport:
    session_id_clean = _compact_text(session_id)
    if not session_id_clean:
        raise ValueError("Ingresa el _session_id de Loqueleo.")

    current_url = _normalize_loqueleo_url(listing_url)
    session = _build_loqueleo_session(session_id_clean, cookie_header=cookie_header)

    visited_request_urls: set[str] = set()
    visited_response_urls: set[str] = set()
    seen_signatures: set[tuple[tuple[str, str, str], ...]] = set()
    visited_pages: List[str] = []
    all_rows: List[Dict[str, object]] = []
    first_page: Optional[LoqueleoUsersPage] = None
    last_page: Optional[LoqueleoUsersPage] = None
    stop_reason = "Sin resultados."

    for _ in range(max(1, int(max_pages))):
        request_key = _normalize_loqueleo_url(current_url)
        if request_key in visited_request_urls:
            stop_reason = "Se detecto una URL repetida en la paginacion."
            break
        visited_request_urls.add(request_key)

        try:
            page = _fetch_loqueleo_users_page(session, current_url, timeout=int(timeout))
        except FileNotFoundError:
            stop_reason = "La paginacion termino en una pagina inexistente."
            break

        response_key = _normalize_loqueleo_url(page.response_url)
        if response_key in visited_response_urls:
            stop_reason = "Loqueleo redirigio a una pagina ya visitada."
            break
        visited_response_urls.add(response_key)
        visited_pages.append(page.response_url)

        if first_page is None:
            first_page = page
        last_page = page

        if not page.rows:
            stop_reason = "La pagina ya no contiene filas para exportar."
            if len(visited_pages) == 1:
                raise RuntimeError(
                    "No se encontraron usuarios en la pagina indicada o la sesion no tiene acceso."
                )
            break

        page_signature = tuple((row.user_id, row.account, row.name) for row in page.rows)
        if page_signature in seen_signatures:
            stop_reason = "Se detecto una pagina repetida en la respuesta."
            break
        seen_signatures.add(page_signature)

        page_number = _extract_page_number(page.response_url)
        current_parsed = urlparse(page.response_url)
        current_query = parse_qs(current_parsed.query)
        user_type = str(current_query.get("type", [""])[0] or "").strip()
        locale = str(current_query.get("locale", [""])[0] or "").strip()
        year = str(current_query.get("year", [""])[0] or "").strip()
        organization_id = _extract_organization_id(page.response_url)
        organization_name = (
            page.breadcrumb[1]
            if len(page.breadcrumb) >= 2
            else re.sub(r"^Organizaci[oó]n\s*-\s*", "", page.title, flags=re.IGNORECASE)
        )
        user_type_label = (
            LOQUELEO_USER_TYPE_LABELS.get(user_type)
            or (page.breadcrumb[-1] if page.breadcrumb else "")
            or page.heading
            or "Usuarios"
        )

        for row in page.rows:
            all_rows.append(
                {
                    "Organizacion": organization_name,
                    "Organizacion ID": organization_id,
                    "Tipo": user_type,
                    "Tipo etiqueta": user_type_label,
                    "Ano": year,
                    "Idioma": locale,
                    "Pagina": page_number,
                    "Nombre": row.name,
                    "Cuenta": row.account,
                    "Usuario ID": row.user_id,
                    "Usuario URL": row.detail_url,
                    "Accion": row.action_label,
                    "Accion URL": row.action_url,
                    "URL origen": page.response_url,
                }
            )

        next_url = page.next_url or _increment_page_url(page.response_url)
        next_key = _normalize_loqueleo_url(next_url)
        if next_key in visited_request_urls:
            stop_reason = "No hay mas paginas nuevas por consultar."
            break
        current_url = next_url
    else:
        stop_reason = f"Se alcanzo el limite de {int(max_pages)} paginas."

    if first_page is None or last_page is None:
        raise RuntimeError("No se pudo obtener ninguna pagina de Loqueleo.")

    first_query = parse_qs(urlparse(first_page.response_url).query)
    first_user_type = str(first_query.get("type", [""])[0] or "").strip()
    first_locale = str(first_query.get("locale", [""])[0] or "").strip()
    first_year = str(first_query.get("year", [""])[0] or "").strip()
    first_organization_id = _extract_organization_id(first_page.response_url)
    first_organization_name = (
        first_page.breadcrumb[1]
        if len(first_page.breadcrumb) >= 2
        else re.sub(r"^Organizaci[oó]n\s*-\s*", "", first_page.title, flags=re.IGNORECASE)
    )
    first_user_type_label = (
        LOQUELEO_USER_TYPE_LABELS.get(first_user_type)
        or (first_page.breadcrumb[-1] if first_page.breadcrumb else "")
        or first_page.heading
        or "Usuarios"
    )

    return LoqueleoUsersExport(
        input_url=_normalize_loqueleo_url(listing_url),
        first_response_url=first_page.response_url,
        final_response_url=last_page.response_url,
        organization_id=first_organization_id,
        organization_name=_compact_text(first_organization_name),
        user_type=first_user_type,
        user_type_label=_compact_text(first_user_type_label),
        locale=first_locale,
        year=first_year,
        csrf_token=first_page.csrf_token,
        reported_total=first_page.total_records,
        rows=all_rows,
        visited_pages=visited_pages,
        page_count=len(visited_pages),
        stop_reason=stop_reason,
    )
