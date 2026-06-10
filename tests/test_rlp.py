from io import BytesIO
import unittest

import pandas as pd

from santillana_format.rlp.service import (
    build_rlp_report_excel,
    clean_cookie_header,
    has_rlp_session_cookie,
    load_rlp_tokens,
    normalize_rlp_access_code,
)


def _excel_bytes(dataframe: pd.DataFrame) -> bytes:
    output = BytesIO()
    dataframe.to_excel(output, index=False, engine="openpyxl")
    return output.getvalue()


class RLPServiceTests(unittest.TestCase):
    def test_load_rlp_tokens_deduplicates_and_cleans_values(self) -> None:
        source = _excel_bytes(
            pd.DataFrame(
                {
                    "Tokens": [
                        "abc 123",
                        "ABC123",
                        "",
                        "xyz789",
                    ]
                }
            )
        )

        self.assertEqual(load_rlp_tokens(source, "tokens.xlsx"), ["ABC123", "XYZ789"])

    def test_cookie_header_detection_accepts_devtools_prefix(self) -> None:
        cookie = clean_cookie_header(
            "Cookie: recaptcha_verified=true; _unity_web_session=session-value"
        )

        self.assertTrue(cookie.startswith("recaptcha_verified="))
        self.assertTrue(has_rlp_session_cookie(cookie))
        self.assertFalse(has_rlp_session_cookie("recaptcha_verified=true"))

    def test_normalize_rlp_access_code_flattens_details_and_products(self) -> None:
        payload = {
            "subscription": [{"id": "sub-1"}, {"id": "sub-2"}],
            "token_details": {
                "batch_id": "batch-1",
                "token": "ABC123",
                "formatted_token": "ABC 123",
                "status": "active",
                "delivery_status": "available",
                "times_to_be_redeemed": 8,
                "redeem_expires_at": None,
                "redemption_payload": {
                    "expiration_date": None,
                    "product_ids": ["product-1"],
                    "role": "student",
                },
                "expiration_date": "2026-12-04T23:59:59Z",
                "days_after_redeem_to_expire_token": 180,
                "metadata": {"source": "test"},
            },
            "products_assigned_to_the_token": [
                {
                    "id": "product-1",
                    "name": "Product one",
                    "description": "Resources",
                    "isbn": "",
                    "created_at": "2020-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                    "serie_product_id": "serie-1",
                    "position": 12,
                    "cover_image": {"url": "https://example.com/cover.png"},
                    "metadata": {"level": "adult"},
                }
            ],
        }

        result = normalize_rlp_access_code("abc123", payload)

        token_row = result["token_rows"][0]
        self.assertEqual(token_row["consulta_ok"], "SI")
        self.assertEqual(token_row["batch_id"], "batch-1")
        self.assertEqual(token_row["redemption_product_ids"], "product-1")
        self.assertEqual(token_row["subscription_count"], 2)
        self.assertEqual(token_row["products_count"], 1)
        self.assertEqual(token_row["product_names"], "Product one")

        product_row = result["product_rows"][0]
        self.assertEqual(product_row["input_token"], "ABC123")
        self.assertEqual(
            product_row["cover_image_url"],
            "https://example.com/cover.png",
        )

    def test_build_rlp_report_excel_contains_expected_sheets(self) -> None:
        report = {
            "token_rows": [
                {
                    "consulta_ok": "NO",
                    "error": "Token no encontrado",
                    "input_token": "BAD",
                    "token": "BAD",
                }
            ],
            "product_rows": [],
        }

        workbook = pd.ExcelFile(BytesIO(build_rlp_report_excel(report)))

        self.assertEqual(
            workbook.sheet_names,
            ["Token details", "Productos", "Errores"],
        )


if __name__ == "__main__":
    unittest.main()
