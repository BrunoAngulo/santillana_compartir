from __future__ import annotations

import json
import re
import unicodedata
from io import BytesIO, StringIO
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

import pandas as pd
import requests


RLP_ACCESS_CODES_URL = "https://richmondlp.com/components/administration/access_codes"

RLP_TOKEN_COLUMNS = [
    "consulta_ok",
    "error",
    "input_token",
    "batch_id",
    "token",
    "formatted_token",
    "status",
    "delivery_status",
    "times_to_be_redeemed",
    "redeem_expires_at",
    "redemption_expiration_date",
    "redemption_product_ids",
    "redemption_role",
    "expiration_date",
    "days_after_redeem_to_expire_token",
    "metadata",
    "subscription_count",
    "products_count",
    "product_ids",
    "product_names",
]

RLP_PRODUCT_COLUMNS = [
    "input_token",
    "token",
    "id",
    "name",
    "description",
    "isbn",
    "created_at",
    "updated_at",
    "serie_product_id",
    "position",
    "cover_image_url",
    "metadata",
]

TOKEN_COLUMN_ALIASES = {
    "accesscode",
    "codigo",
    "codigodeacceso",
    "token",
    "tokens",
}


def _normalize_header(value: object) -> str:
    text = unicodedata.normalize("NFD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return re.sub(r"[^a-z0-9]+", "", text)


def clean_rlp_token(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "").strip()).upper()


def clean_cookie_header(value: object) -> str:
    text = str(value or "").strip()
    if text.lower().startswith("cookie:"):
        text = text.split(":", 1)[1].strip()
    return text


def has_rlp_session_cookie(cookie_header: object) -> bool:
    return bool(
        re.search(
            r"(?:^|;\s*)_unity_web_session\s*=",
            clean_cookie_header(cookie_header),
            flags=re.IGNORECASE,
        )
    )


def load_rlp_tokens(file_bytes: bytes, file_name: str) -> List[str]:
    if not file_bytes:
        raise ValueError("El archivo esta vacio.")

    suffix = Path(str(file_name or "")).suffix.lower()
    if suffix in {".csv", ".txt"}:
        text = file_bytes.decode("utf-8-sig", errors="replace")
        dataframe = pd.read_csv(
            StringIO(text),
            dtype=str,
            sep=None,
            engine="python",
        )
    else:
        dataframe = pd.read_excel(BytesIO(file_bytes), dtype=str)

    if dataframe.empty and not list(dataframe.columns):
        raise ValueError("El archivo no contiene columnas.")

    token_column = next(
        (
            column
            for column in dataframe.columns
            if _normalize_header(column) in TOKEN_COLUMN_ALIASES
        ),
        None,
    )
    if token_column is None and len(dataframe.columns) == 1:
        token_column = dataframe.columns[0]
    if token_column is None:
        available = ", ".join(str(column) for column in dataframe.columns)
        raise ValueError(
            "No se encontro una columna Token. "
            f"Columnas disponibles: {available or '(ninguna)'}."
        )

    tokens: List[str] = []
    seen = set()
    for value in dataframe[token_column].tolist():
        token = clean_rlp_token(value)
        if not token or token in {"NAN", "NONE"} or token in seen:
            continue
        seen.add(token)
        tokens.append(token)

    if not tokens:
        raise ValueError("La columna Token no contiene valores validos.")
    return tokens


def fetch_rlp_access_code(
    token: str,
    cookie_header: str,
    timeout: int = 120,
    session: Optional[requests.Session] = None,
) -> Dict[str, object]:
    token_clean = clean_rlp_token(token)
    cookie_clean = clean_cookie_header(cookie_header)
    if not token_clean:
        raise ValueError("El token esta vacio.")
    if not has_rlp_session_cookie(cookie_clean):
        raise ValueError("Falta la cookie _unity_web_session de RLP.")

    client = session or requests.Session()
    headers = {
        "Accept": "application/json",
        "Cookie": cookie_clean,
        "Referer": "https://richmondlp.com/admin",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        response = client.get(
            RLP_ACCESS_CODES_URL,
            params={"token": token_clean},
            headers=headers,
            timeout=int(timeout),
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = int(response.status_code)
    if status_code in {401, 403}:
        raise RuntimeError("Sesion RLP invalida, expirada o sin permisos.")

    try:
        payload = response.json()
    except ValueError as exc:
        content_type = str(response.headers.get("content-type") or "").lower()
        if "text/html" in content_type or "login" in str(response.url or "").lower():
            raise RuntimeError("La sesion RLP expiro o redirigio al inicio de sesion.") from exc
        raise RuntimeError(f"RLP no devolvio JSON valido (HTTP {status_code}).") from exc

    if not response.ok:
        message = ""
        if isinstance(payload, dict):
            message = str(payload.get("message") or payload.get("error") or "").strip()
        raise RuntimeError(message or f"Error HTTP {status_code}.")
    if not isinstance(payload, dict):
        raise RuntimeError("RLP devolvio una respuesta JSON inesperada.")
    return payload


def _json_cell(value: object) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def normalize_rlp_access_code(
    input_token: str,
    payload: Dict[str, object],
) -> Dict[str, List[Dict[str, object]]]:
    details = payload.get("token_details")
    if not isinstance(details, dict):
        message = str(payload.get("message") or payload.get("error") or "").strip()
        raise RuntimeError(message or "La respuesta no contiene token_details.")

    redemption_payload = details.get("redemption_payload")
    if not isinstance(redemption_payload, dict):
        redemption_payload = {}

    subscriptions = payload.get("subscription")
    subscription_count = len(subscriptions) if isinstance(subscriptions, list) else 0

    products = payload.get("products_assigned_to_the_token")
    product_items = (
        [item for item in products if isinstance(item, dict)]
        if isinstance(products, list)
        else []
    )
    product_ids = [str(item.get("id") or "").strip() for item in product_items]
    product_names = [str(item.get("name") or "").strip() for item in product_items]

    detail_row: Dict[str, object] = {
        "consulta_ok": "SI",
        "error": "",
        "input_token": clean_rlp_token(input_token),
        "batch_id": str(details.get("batch_id") or "").strip(),
        "token": str(details.get("token") or "").strip(),
        "formatted_token": str(details.get("formatted_token") or "").strip(),
        "status": str(details.get("status") or "").strip(),
        "delivery_status": str(details.get("delivery_status") or "").strip(),
        "times_to_be_redeemed": details.get("times_to_be_redeemed"),
        "redeem_expires_at": details.get("redeem_expires_at"),
        "redemption_expiration_date": redemption_payload.get("expiration_date"),
        "redemption_product_ids": " | ".join(
            str(value).strip()
            for value in redemption_payload.get("product_ids", [])
            if str(value or "").strip()
        )
        if isinstance(redemption_payload.get("product_ids"), list)
        else "",
        "redemption_role": str(redemption_payload.get("role") or "").strip(),
        "expiration_date": details.get("expiration_date"),
        "days_after_redeem_to_expire_token": details.get(
            "days_after_redeem_to_expire_token"
        ),
        "metadata": _json_cell(details.get("metadata")),
        "subscription_count": subscription_count,
        "products_count": len(product_items),
        "product_ids": " | ".join(value for value in product_ids if value),
        "product_names": " | ".join(value for value in product_names if value),
    }

    product_rows: List[Dict[str, object]] = []
    for item in product_items:
        cover_image = item.get("cover_image")
        if not isinstance(cover_image, dict):
            cover_image = {}
        product_rows.append(
            {
                "input_token": clean_rlp_token(input_token),
                "token": str(details.get("token") or "").strip(),
                "id": str(item.get("id") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "description": str(item.get("description") or "").strip(),
                "isbn": str(item.get("isbn") or "").strip(),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "serie_product_id": str(item.get("serie_product_id") or "").strip(),
                "position": item.get("position"),
                "cover_image_url": str(cover_image.get("url") or "").strip(),
                "metadata": _json_cell(item.get("metadata")),
            }
        )

    return {"token_rows": [detail_row], "product_rows": product_rows}


def verify_rlp_tokens(
    tokens: Sequence[str],
    cookie_header: str,
    timeout: int = 120,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, object]:
    cookie_clean = clean_cookie_header(cookie_header)
    if not has_rlp_session_cookie(cookie_clean):
        raise ValueError("Falta la cookie _unity_web_session de RLP.")

    token_rows: List[Dict[str, object]] = []
    product_rows: List[Dict[str, object]] = []
    client = requests.Session()
    total = len(tokens)

    for index, token in enumerate(tokens, start=1):
        token_clean = clean_rlp_token(token)
        if on_progress:
            on_progress(index, total, token_clean)
        try:
            payload = fetch_rlp_access_code(
                token=token_clean,
                cookie_header=cookie_clean,
                timeout=int(timeout),
                session=client,
            )
            normalized = normalize_rlp_access_code(token_clean, payload)
            del payload
        except (RuntimeError, ValueError) as exc:
            error_row = {column: "" for column in RLP_TOKEN_COLUMNS}
            error_row.update(
                {
                    "consulta_ok": "NO",
                    "error": str(exc),
                    "input_token": token_clean,
                    "token": token_clean,
                }
            )
            token_rows.append(error_row)
            continue

        token_rows.extend(normalized["token_rows"])
        product_rows.extend(normalized["product_rows"])

    success_count = sum(row.get("consulta_ok") == "SI" for row in token_rows)
    return {
        "token_rows": token_rows,
        "product_rows": product_rows,
        "total": len(token_rows),
        "success_count": success_count,
        "error_count": len(token_rows) - success_count,
    }


def _format_worksheet(worksheet) -> None:
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions
    for column_cells in worksheet.columns:
        values = [str(cell.value or "") for cell in column_cells[:200]]
        width = min(max((len(value) for value in values), default=8) + 2, 60)
        worksheet.column_dimensions[column_cells[0].column_letter].width = width


def build_rlp_report_excel(report: Dict[str, object]) -> bytes:
    token_rows = report.get("token_rows")
    product_rows = report.get("product_rows")
    token_df = pd.DataFrame(
        token_rows if isinstance(token_rows, list) else [],
        columns=RLP_TOKEN_COLUMNS,
    )
    product_df = pd.DataFrame(
        product_rows if isinstance(product_rows, list) else [],
        columns=RLP_PRODUCT_COLUMNS,
    )
    error_df = token_df[token_df["consulta_ok"] != "SI"].copy()

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        token_df.to_excel(writer, index=False, sheet_name="Token details")
        product_df.to_excel(writer, index=False, sheet_name="Productos")
        error_df.to_excel(writer, index=False, sheet_name="Errores")
        for worksheet in writer.book.worksheets:
            _format_worksheet(worksheet)
    output.seek(0)
    return output.getvalue()


def build_rlp_template_excel() -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame({"Token": []}).to_excel(
            writer,
            index=False,
            sheet_name="Tokens",
        )
        pd.DataFrame(
            [
                {
                    "Instruccion": (
                        "Agrega un token RLP por fila en la columna Token. "
                        "No incluyas espacios entre bloques."
                    )
                }
            ]
        ).to_excel(writer, index=False, sheet_name="Instrucciones")
        for worksheet in writer.book.worksheets:
            _format_worksheet(worksheet)
    output.seek(0)
    return output.getvalue()
