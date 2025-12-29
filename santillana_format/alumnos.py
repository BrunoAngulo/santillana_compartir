import re
from io import BytesIO
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from openpyxl.utils import get_column_letter

DEFAULT_EMPRESA_ID = 11
DEFAULT_CICLO_ID = 206
BASE_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnos"
)

NIVEL_MAP = {
    "Inicial": 38,
    "Primaria": 39,
    "Secundaria": 40,
}

GRADOS_POR_NIVEL = {
    38: {
        113: "3 anos",
        114: "4 anos",
        115: "5 anos",
        116: "6 anos",
        117: "7 anos",
        118: "8 anos",
    },
    39: {
        119: "1ro primaria",
        120: "2do primaria",
        121: "3ro primaria",
        122: "4to primaria",
        123: "5to primaria",
    },
    40: {
        126: "1ro secundaria",
        127: "2do secundaria",
        128: "3ro secundaria",
        129: "4to secundaria",
        130: "5to secundaria",
    },
}

GRUPO_ID_TO_LETRA = {
    661: "A",
    662: "B",
    663: "C",
    664: "D",
    665: "E",
    666: "F",
    667: "G",
    668: "H",
    669: "I",
    670: "J",
    671: "K",
}
GRUPO_LETRA_TO_ID = {letra: grupo_id for grupo_id, letra in GRUPO_ID_TO_LETRA.items()}

ALUMNO_COLUMNS = [
    "colegio_id",
    "nivel_id",
    "nivel",
    "grado_id",
    "grado",
    "grupo_id",
    "grupo",
    "grupo_clave",
    "alumno_id",
    "persona_id",
    "nombre_completo",
    "nombre",
    "apellido_paterno",
    "apellido_materno",
    "sexo",
    "id_oficial",
    "fecha_nacimiento",
    "activo",
    "alumno_clave",
    "fecha_desde",
    "fecha_validado",
]

ERROR_COLUMNS = [
    "colegio_id",
    "nivel_id",
    "grado_id",
    "grupo_id",
    "url",
    "status_code",
    "error",
]


def build_alumnos_filename(colegio_ids: Sequence[int]) -> str:
    if not colegio_ids:
        return "alumnos.xlsx"
    ids = sorted({int(colegio_id) for colegio_id in colegio_ids})
    if len(ids) == 1:
        return f"alumnos_{ids[0]}.xlsx"
    joined = "_".join(str(colegio_id) for colegio_id in ids)
    return f"alumnos_{joined}.xlsx"


def parse_id_list(text: str) -> List[int]:
    if not text:
        return []
    ids: List[int] = []
    for token in re.split(r"[\s,]+", text.strip()):
        if not token:
            continue
        if "-" in token:
            inicio, fin = token.split("-", 1)
            if inicio.isdigit() and fin.isdigit():
                ids.extend(range(int(inicio), int(fin) + 1))
            continue
        if token.isdigit():
            ids.append(int(token))
    return sorted(set(ids))


def build_request_contexts(
    colegio_ids: Sequence[int],
    nivel_ids: Sequence[int],
    grupo_ids: Sequence[int],
    empresa_id: int = DEFAULT_EMPRESA_ID,
    ciclo_id: int = DEFAULT_CICLO_ID,
) -> List[Dict[str, int]]:
    contexts: List[Dict[str, int]] = []
    for colegio_id in colegio_ids:
        for nivel_id in nivel_ids:
            grados = GRADOS_POR_NIVEL.get(nivel_id, {})
            for grado_id in grados.keys():
                for grupo_id in grupo_ids:
                    contexts.append(
                        {
                            "colegio_id": colegio_id,
                            "nivel_id": nivel_id,
                            "grado_id": grado_id,
                            "grupo_id": grupo_id,
                            "empresa_id": empresa_id,
                            "ciclo_id": ciclo_id,
                        }
                    )
    return contexts


def _build_url(context: Dict[str, int]) -> Tuple[str, Dict[str, int]]:
    url = BASE_URL.format(
        empresa_id=context["empresa_id"],
        ciclo_id=context["ciclo_id"],
        colegio_id=context["colegio_id"],
    )
    params = {
        "nivelId": context["nivel_id"],
        "gradoId": context["grado_id"],
        "grupoId": context["grupo_id"],
    }
    return url, params


def _fetch_alumnos(
    session: requests.Session,
    token: str,
    context: Dict[str, int],
    timeout: int = 30,
) -> Tuple[List[Dict[str, object]], Optional[str], Optional[int], str]:
    url, params = _build_url(context)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    try:
        response = session.get(url, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as exc:
        return [], str(exc), None, url

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError:
        return [], f"Respuesta no JSON (status {status_code})", status_code, url

    if not response.ok:
        message = ""
        if isinstance(payload, dict):
            message = payload.get("message") or payload.get("error") or ""
        return [], message or f"HTTP {status_code}", status_code, url

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        return [], message or "Respuesta invalida", status_code, url

    data = payload.get("data") or []
    if not isinstance(data, list):
        return [], "Campo data no es lista", status_code, url
    return data, None, status_code, url


def _flatten_alumno(item: Dict[str, object], context: Dict[str, int]) -> Dict[str, object]:
    persona = item.get("persona") or {}
    nivel = item.get("nivel") or {}
    grado = item.get("grado") or {}
    grupo = item.get("grupo") or {}
    return {
        "colegio_id": context["colegio_id"],
        "nivel_id": nivel.get("nivelId", context["nivel_id"]),
        "nivel": nivel.get("nivel", ""),
        "grado_id": grado.get("gradoId", context["grado_id"]),
        "grado": grado.get("grado", ""),
        "grupo_id": grupo.get("grupoId", context["grupo_id"]),
        "grupo": grupo.get("grupo", ""),
        "grupo_clave": grupo.get("grupoClave", ""),
        "alumno_id": item.get("alumnoId", ""),
        "persona_id": persona.get("personaId", ""),
        "nombre_completo": persona.get("nombreCompleto", ""),
        "nombre": persona.get("nombre", ""),
        "apellido_paterno": persona.get("apellidoPaterno", ""),
        "apellido_materno": persona.get("apellidoMaterno", ""),
        "sexo": persona.get("sexoMoral", ""),
        "id_oficial": persona.get("idOficial", ""),
        "fecha_nacimiento": persona.get("fechaNacimiento", ""),
        "activo": item.get("activo", ""),
        "alumno_clave": item.get("alumnoClave", ""),
        "fecha_desde": item.get("fechaDesde", ""),
        "fecha_validado": item.get("fechaValidado", ""),
    }


def _ensure_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=columns)
    return df.reindex(columns=columns)


def export_alumnos_excel(
    alumnos: List[Dict[str, object]],
    errores: List[Dict[str, object]],
) -> bytes:
    output = BytesIO()
    df_alumnos = _ensure_columns(pd.DataFrame(alumnos), ALUMNO_COLUMNS)
    df_errores = _ensure_columns(pd.DataFrame(errores), ERROR_COLUMNS)

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_alumnos.to_excel(writer, index=False, sheet_name="Alumnos")
        df_errores.to_excel(writer, index=False, sheet_name="Errores")
        for sheet_name, df in (("Alumnos", df_alumnos), ("Errores", df_errores)):
            ws = writer.book[sheet_name]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            for idx, col in enumerate(df.columns, start=1):
                sample = df[col].astype(str).head(200).tolist()
                max_len = max([len(str(col))] + [len(val) for val in sample])
                ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)

    output.seek(0)
    return output.getvalue()


def listar_alumnos(
    token: str,
    colegio_ids: Sequence[int],
    nivel_ids: Optional[Sequence[int]] = None,
    grupo_ids: Optional[Sequence[int]] = None,
    empresa_id: int = DEFAULT_EMPRESA_ID,
    ciclo_id: int = DEFAULT_CICLO_ID,
    timeout: int = 30,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> Tuple[bytes, Dict[str, int]]:
    nivel_ids = list(nivel_ids) if nivel_ids else list(NIVEL_MAP.values())
    grupo_ids = list(grupo_ids) if grupo_ids else list(GRUPO_ID_TO_LETRA.keys())

    contexts = build_request_contexts(
        colegio_ids=colegio_ids,
        nivel_ids=nivel_ids,
        grupo_ids=grupo_ids,
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
    )

    alumnos: List[Dict[str, object]] = []
    errores: List[Dict[str, object]] = []

    with requests.Session() as session:
        for index, context in enumerate(contexts, start=1):
            data, error, status_code, url = _fetch_alumnos(
                session=session,
                token=token,
                context=context,
                timeout=timeout,
            )
            if error:
                errores.append(
                    {
                        "colegio_id": context["colegio_id"],
                        "nivel_id": context["nivel_id"],
                        "grado_id": context["grado_id"],
                        "grupo_id": context["grupo_id"],
                        "url": url,
                        "status_code": status_code or "",
                        "error": error,
                    }
                )
            else:
                for item in data:
                    alumnos.append(_flatten_alumno(item, context))

            if on_progress:
                on_progress(index, len(contexts))

    output_bytes = export_alumnos_excel(alumnos, errores)
    summary = {
        "solicitudes_total": len(contexts),
        "solicitudes_error": len(errores),
        "alumnos_total": len(alumnos),
    }
    return output_bytes, summary
