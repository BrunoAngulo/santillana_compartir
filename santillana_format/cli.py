import argparse
import os
import re
import sys
import warnings as warnings_module
from pathlib import Path
from typing import Dict, List, Optional

import requests

from .alumnos import (
    DEFAULT_CICLO_ID as ALUMNOS_CICLO_ID_DEFAULT,
    DEFAULT_EMPRESA_ID,
    descargar_plantilla_edicion_masiva,
)
from .alumnos_compare import (
    DEFAULT_SHEET_ACTUALIZADA as ALUMNOS_SHEET_ACTUALIZADA,
    DEFAULT_SHEET_BD as ALUMNOS_SHEET_BD,
    comparar_plantillas,
)
from .processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    process_excel,
)
from .profesores import (
    DEFAULT_CICLO_ID as PROFESORES_CICLO_ID_DEFAULT,
    NIVEL_MAP as PROFESORES_NIVEL_MAP,
    build_profesores_filename,
    listar_profesores,
)
from .profesores_clases import asignar_profesores_clases
from .profesores_password import actualizar_passwords_docentes

OUTPUT_DIR_CLASES = Path("salidas") / "Clases"
OUTPUT_DIR_PROFESORES = Path("salidas") / "Profesores"
OUTPUT_DIR_ALUMNOS = Path("salidas") / "Alumnos"
GESTION_ESCOLAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases"
)
GESTION_ESCOLAR_ALUMNOS_CLASE_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases/{clase_id}/alumnos"
)
GESTION_ESCOLAR_CICLO_ID_DEFAULT = 207


def _print_progress(current: int, total: int, message: str) -> None:
    if total <= 0:
        return
    width = 26
    ratio = min(max(current / total, 0), 1)
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    percent = int(ratio * 100)
    sys.stderr.write(f"\r[{bar}] {percent:3d}% {message}")
    sys.stderr.flush()
    if current >= total:
        sys.stderr.write("\n")


def _clean_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token


def _parse_grupo_letras(value: str) -> List[str]:
    if not value:
        return []
    letras: List[str] = []
    invalid: List[str] = []
    for token in re.split(r"[\s,]+", value.strip()):
        if not token:
            continue
        upper = token.upper()
        if len(upper) == 1 and upper.isalpha():
            if upper not in letras:
                letras.append(upper)
        else:
            invalid.append(token)
    if invalid:
        raise ValueError(f"Grupos invalidos: {', '.join(invalid)}")
    return letras


def _parse_niveles(value: str) -> List[int]:
    if not value:
        return list(PROFESORES_NIVEL_MAP.values())
    ids: List[int] = []
    invalid: List[str] = []
    niveles_lookup = {key.lower(): val for key, val in PROFESORES_NIVEL_MAP.items()}
    for token in re.split(r"[\s,]+", value.strip()):
        if not token:
            continue
        lower = token.strip().lower()
        if lower.isdigit():
            level_id = int(lower)
            if level_id not in ids:
                ids.append(level_id)
            continue
        if lower in niveles_lookup:
            level_id = niveles_lookup[lower]
            if level_id not in ids:
                ids.append(level_id)
            continue
        invalid.append(token)
    if invalid:
        raise ValueError(f"Niveles invalidos: {', '.join(invalid)}")
    return ids

def _fetch_clases_gestion_escolar(
    session: requests.Session,
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id)
    try:
        response = session.get(
            url, headers=headers, params={"colegioId": colegio_id}, timeout=timeout
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        raise RuntimeError(message or "Respuesta invalida")

    data = payload.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")
    return data


def _fetch_alumnos_clase_gestion_escolar(
    session: requests.Session,
    token: str,
    clase_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> Dict[str, object]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_ALUMNOS_CLASE_URL.format(
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        clase_id=clase_id,
    )
    try:
        response = session.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        raise RuntimeError(message or "Respuesta invalida")

    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError("Campo data no es objeto")
    return data


def _delete_clase_gestion_escolar(
    session: requests.Session,
    token: str,
    clase_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> None:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = GESTION_ESCOLAR_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id)
    url = f"{url}/{clase_id}"
    try:
        response = session.delete(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red: {exc}") from exc

    if not response.ok:
        status_code = response.status_code
        try:
            payload = response.json()
            message = payload.get("message") if isinstance(payload, dict) else ""
        except ValueError:
            message = ""
        raise RuntimeError(message or f"HTTP {status_code}")

    if response.content:
        try:
            payload = response.json()
        except ValueError:
            return
        if isinstance(payload, dict) and payload.get("success") is False:
            message = payload.get("message") or "Respuesta invalida"
            raise RuntimeError(message)


def _collect_colegios(clases: List[Dict[str, object]]) -> List[Dict[str, object]]:
    colegios: Dict[int, str] = {}
    for item in clases:
        cnc = item.get("colegioNivelCiclo") if isinstance(item, dict) else None
        colegio = cnc.get("colegio") if isinstance(cnc, dict) else None
        if isinstance(colegio, dict):
            colegio_id = colegio.get("colegioId")
            colegio_nombre = colegio.get("colegio", "")
            if colegio_id is not None:
                colegios[int(colegio_id)] = str(colegio_nombre or "")
    return [
        {"colegioId": colegio_id, "colegio": nombre}
        for colegio_id, nombre in sorted(colegios.items())
    ]


def _resolve_output_path_clases(output: str, codigo: str) -> Path:
    if output:
        path = Path(output)
        if path.is_dir():
            return path / f"{Path(OUTPUT_FILENAME).stem}_{codigo}.xlsx"
        return path
    return OUTPUT_DIR_CLASES / f"{Path(OUTPUT_FILENAME).stem}_{codigo}.xlsx"


def _resolve_output_path_profesores(output: str, colegio_id: int) -> Path:
    if output:
        path = Path(output)
        if path.is_dir():
            return path / build_profesores_filename(colegio_id)
        return path
    return OUTPUT_DIR_PROFESORES / build_profesores_filename(colegio_id)


def _resolve_output_path_alumnos(output: str, colegio_id: int) -> Path:
    if output:
        path = Path(output)
        if path.is_dir():
            return path / f"plantilla_edicion_alumnos_{colegio_id}.xlsx"
        return path
    return OUTPUT_DIR_ALUMNOS / f"plantilla_edicion_alumnos_{colegio_id}.xlsx"


def _resolve_output_path_comparar(output: str, excel_path: Path) -> Path:
    if output:
        path = Path(output)
        if path.is_dir():
            return path / f"alumnos_resultados_{excel_path.stem}.xlsx"
        return path
    return OUTPUT_DIR_ALUMNOS / f"alumnos_resultados_{excel_path.stem}.xlsx"

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Herramientas para crear clases, gestionar profesores y alumnos."
    )
    subparsers = parser.add_subparsers(dest="command")

    parser_clases = subparsers.add_parser(
        "clases", help="Crear clases desde un Excel de detalle."
    )
    parser_clases.add_argument("ruta_excel", help="Ruta del archivo XLSX de entrada")
    parser_clases.add_argument(
        "codigo", help="Codigo a filtrar (se respetan ceros a la izquierda)"
    )
    parser_clases.add_argument(
        "grupos",
        nargs="?",
        default="",
        help="Letras de grupos a crear, separadas por coma (ej: A,B,C).",
    )
    parser_clases.add_argument(
        "--columna-codigo",
        dest="columna_codigo",
        default=CODE_COLUMN_NAME,
        help=f"Nombre de la columna que contiene el codigo (default: {CODE_COLUMN_NAME})",
    )
    parser_clases.add_argument(
        "--hoja",
        dest="hoja",
        default=SHEET_NAME,
        help=f"Nombre de la hoja a leer (default: {SHEET_NAME})",
    )
    parser_clases.add_argument(
        "--output",
        default="",
        help="Ruta del Excel de salida (default: salidas/Clases/).",
    )

    parser_clases_api = subparsers.add_parser(
        "clases-api", help="Listar o eliminar clases desde gestion escolar."
    )
    parser_clases_api.add_argument(
        "--token",
        default="",
        help="Bearer token (sin el prefijo 'Bearer').",
    )
    parser_clases_api.add_argument(
        "--token-env",
        default="PEGASUS_TOKEN",
        help="Nombre de la variable de entorno con el token.",
    )
    parser_clases_api.add_argument(
        "--colegio-id",
        type=int,
        required=True,
        help="ID del colegio.",
    )
    parser_clases_api.add_argument(
        "--empresa-id",
        type=int,
        default=DEFAULT_EMPRESA_ID,
        help="Empresa ID (default: 11).",
    )
    parser_clases_api.add_argument(
        "--ciclo-id",
        type=int,
        default=GESTION_ESCOLAR_CICLO_ID_DEFAULT,
        help="Ciclo ID (default: 207).",
    )
    parser_clases_api.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Elimina todas las clases listadas.",
    )
    parser_clases_api.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )

    parser_profesores = subparsers.add_parser(
        "profesores", help="Listar profesores y generar Excel."
    )
    parser_profesores.add_argument(
        "--token",
        default="",
        help="Bearer token (sin el prefijo 'Bearer').",
    )
    parser_profesores.add_argument(
        "--token-env",
        default="PEGASUS_TOKEN",
        help="Nombre de la variable de entorno con el token.",
    )
    parser_profesores.add_argument(
        "--colegio-id",
        type=int,
        required=True,
        help="ID del colegio.",
    )
    parser_profesores.add_argument(
        "--empresa-id",
        type=int,
        default=DEFAULT_EMPRESA_ID,
        help="Empresa ID (default: 11).",
    )
    parser_profesores.add_argument(
        "--ciclo-id",
        type=int,
        default=PROFESORES_CICLO_ID_DEFAULT,
        help="Ciclo ID (default: 207).",
    )
    parser_profesores.add_argument(
        "--niveles",
        default="",
        help="Niveles a consultar (Inicial,Primaria,Secundaria o IDs).",
    )
    parser_profesores.add_argument(
        "--output",
        default="",
        help="Ruta del Excel de salida (default: salidas/Profesores/).",
    )
    parser_profesores.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )

    parser_alumnos = subparsers.add_parser(
        "alumnos-plantilla",
        help="Descargar plantilla de edicion masiva de alumnos registrados.",
    )
    parser_alumnos.add_argument(
        "--token",
        default="",
        help="Bearer token (sin el prefijo 'Bearer').",
    )
    parser_alumnos.add_argument(
        "--token-env",
        default="PEGASUS_TOKEN",
        help="Nombre de la variable de entorno con el token.",
    )
    parser_alumnos.add_argument(
        "--colegio-id",
        type=int,
        required=True,
        help="ID del colegio.",
    )
    parser_alumnos.add_argument(
        "--empresa-id",
        type=int,
        default=DEFAULT_EMPRESA_ID,
        help="Empresa ID (default: 11).",
    )
    parser_alumnos.add_argument(
        "--ciclo-id",
        type=int,
        default=ALUMNOS_CICLO_ID_DEFAULT,
        help=f"Ciclo ID (default: {ALUMNOS_CICLO_ID_DEFAULT}).",
    )
    parser_alumnos.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )
    parser_alumnos.add_argument(
        "--output",
        default="",
        help="Ruta del Excel de salida (default: salidas/Alumnos/).",
    )

    parser_alumno_clases = subparsers.add_parser(
        "alumno-clases",
        help="Buscar clases asociadas a un login de alumno.",
    )
    parser_alumno_clases.add_argument(
        "login",
        help="Login del alumno a buscar (coincidencia exacta, sin distincion de mayusculas).",
    )
    parser_alumno_clases.add_argument(
        "--token",
        default="",
        help="Bearer token (sin el prefijo 'Bearer').",
    )
    parser_alumno_clases.add_argument(
        "--token-env",
        default="PEGASUS_TOKEN",
        help="Nombre de la variable de entorno con el token.",
    )
    parser_alumno_clases.add_argument(
        "--colegio-id",
        type=int,
        required=True,
        help="ID del colegio.",
    )
    parser_alumno_clases.add_argument(
        "--empresa-id",
        type=int,
        default=DEFAULT_EMPRESA_ID,
        help="Empresa ID (default: 11).",
    )
    parser_alumno_clases.add_argument(
        "--ciclo-id",
        type=int,
        default=GESTION_ESCOLAR_CICLO_ID_DEFAULT,
        help="Ciclo ID (default: 207).",
    )
    parser_alumno_clases.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )
    parser_alumno_clases.add_argument(
        "--solo-activos",
        action="store_true",
        help="Solo muestra coincidencias con alumno activo en clase y en censo.",
    )

    parser_alumnos_cmp = subparsers.add_parser(
        "alumnos-comparar",
        help="Comparar Plantilla_BD vs Plantilla_Actualizada y generar filtros.",
    )
    parser_alumnos_cmp.add_argument(
        "ruta_excel",
        help="Ruta del Excel con Plantilla_BD y Plantilla_Actualizada.",
    )
    parser_alumnos_cmp.add_argument(
        "--sheet-bd",
        default=ALUMNOS_SHEET_BD,
        help=f"Hoja base (default: {ALUMNOS_SHEET_BD}).",
    )
    parser_alumnos_cmp.add_argument(
        "--sheet-actualizada",
        default=ALUMNOS_SHEET_ACTUALIZADA,
        help=f"Hoja actualizada (default: {ALUMNOS_SHEET_ACTUALIZADA}).",
    )
    parser_alumnos_cmp.add_argument(
        "--output",
        default="",
        help="Ruta del Excel de salida (default: salidas/Alumnos/).",
    )

    parser_profesores_clases = subparsers.add_parser(
        "profesores-clases",
        help="Asignar profesores a clases segun un Excel (modo simulacion por defecto).",
    )
    parser_profesores_clases.add_argument(
        "ruta_excel",
        help="Ruta del archivo Excel con docentes.",
    )
    parser_profesores_clases.add_argument(
        "--sheet",
        default="",
        help="Hoja del Excel (default: primera hoja).",
    )
    parser_profesores_clases.add_argument(
        "--token",
        default="",
        help="Bearer token (sin el prefijo 'Bearer').",
    )
    parser_profesores_clases.add_argument(
        "--token-env",
        default="PEGASUS_TOKEN",
        help="Nombre de la variable de entorno con el token.",
    )
    parser_profesores_clases.add_argument(
        "--colegio-id",
        type=int,
        required=True,
        help="ID del colegio.",
    )
    parser_profesores_clases.add_argument(
        "--empresa-id",
        type=int,
        default=DEFAULT_EMPRESA_ID,
        help="Empresa ID (default: 11).",
    )
    parser_profesores_clases.add_argument(
        "--ciclo-id",
        type=int,
        default=GESTION_ESCOLAR_CICLO_ID_DEFAULT,
        help="Ciclo ID (default: 207).",
    )
    parser_profesores_clases.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )
    parser_profesores_clases.add_argument(
        "--apply",
        action="store_true",
        help="Aplica los cambios (por defecto es simulacion).",
    )
    parser_profesores_clases.add_argument(
        "--remove-missing",
        action="store_true",
        help="Elimina del staff a profesores que no estan en el Excel (solo clases evaluadas).",
    )
    parser_profesores_clases.add_argument(
        "--no-grupos",
        action="store_true",
        help="No asigna colegio-grado-grupo segun Secciones.",
    )
    parser_profesores_clases.add_argument(
        "--solo-estado",
        action="store_true",
        help="Muestra solo los profesores con cambio de Estado (sin otros logs).",
    )
    parser_profesores_clases.add_argument(
        "--compact",
        action="store_true",
        help="Muestra un listado compacto de acciones en una sola linea por categoria.",
    )

    parser_profesores_password = subparsers.add_parser(
        "profesores-password",
        help="Actualizar login/password de profesores segun un Excel.",
    )
    parser_profesores_password.add_argument(
        "ruta_excel",
        help="Ruta del archivo Excel con docentes.",
    )
    parser_profesores_password.add_argument(
        "--sheet",
        default="",
        help="Hoja del Excel (default: primera hoja).",
    )
    parser_profesores_password.add_argument(
        "--token",
        default="",
        help="Bearer token (sin el prefijo 'Bearer').",
    )
    parser_profesores_password.add_argument(
        "--token-env",
        default="PEGASUS_TOKEN",
        help="Nombre de la variable de entorno con el token.",
    )
    parser_profesores_password.add_argument(
        "--colegio-id",
        type=int,
        required=True,
        help="ID del colegio.",
    )
    parser_profesores_password.add_argument(
        "--empresa-id",
        type=int,
        default=DEFAULT_EMPRESA_ID,
        help="Empresa ID (default: 11).",
    )
    parser_profesores_password.add_argument(
        "--ciclo-id",
        type=int,
        default=PROFESORES_CICLO_ID_DEFAULT,
        help="Ciclo ID (default: 207).",
    )
    parser_profesores_password.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )
    parser_profesores_password.add_argument(
        "--apply",
        action="store_true",
        help="Aplica los cambios (por defecto es simulacion).",
    )

    return parser

def _run_clases(args: argparse.Namespace) -> int:
    ruta_archivo = Path(args.ruta_excel)
    if not ruta_archivo.exists():
        print(f"Error: no existe el archivo: {ruta_archivo}", file=sys.stderr)
        return 1

    try:
        grupos = _parse_grupo_letras(args.grupos) if args.grupos else ["A"]
        plantilla_path = Path(OUTPUT_FILENAME) if Path(OUTPUT_FILENAME).exists() else None
        output_bytes, summary = process_excel(
            ruta_archivo.read_bytes(),
            codigo=args.codigo,
            columna_codigo=args.columna_codigo,
            hoja=args.hoja,
            plantilla_path=plantilla_path,
            grupos=grupos,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    OUTPUT_DIR_CLASES.mkdir(parents=True, exist_ok=True)
    output_path = _resolve_output_path_clases(args.output, args.codigo)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(output_bytes)
    except Exception as exc:
        print(f"Error al escribir salida: {exc}", file=sys.stderr)
        return 1

    print(
        "Listo. Filtradas: {filas_filtradas}, Salida: {filas_salida} filas.".format(
            **summary
        )
    )
    print(f"Archivo generado: {output_path.resolve()}")
    return 0


def _run_clases_api(args: argparse.Namespace) -> int:
    token = _clean_token(args.token)
    if not token:
        token = _clean_token(os.environ.get(args.token_env, ""))
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    try:
        with requests.Session() as session:
            clases = _fetch_clases_gestion_escolar(
                session=session,
                token=token,
                colegio_id=int(args.colegio_id),
                empresa_id=int(args.empresa_id),
                ciclo_id=int(args.ciclo_id),
                timeout=int(args.timeout),
            )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not clases:
        print("No se encontraron clases.")
        return 0

    print(f"Clases encontradas: {len(clases)}")
    rows = []
    for item in clases:
        clase_id = item.get("geClaseId") if isinstance(item, dict) else None
        clase_nombre = ""
        if isinstance(item, dict):
            clase_nombre = item.get("geClase") or item.get("geClaseClave") or ""
        if clase_id is not None:
            rows.append((int(clase_id), str(clase_nombre)))
    for clase_id, clase_nombre in sorted(rows, key=lambda row: row[0]):
        print(f"{clase_id}\t{clase_nombre}")

    if not args.confirm_delete:
        print("No se elimino nada. Usa --confirm-delete para borrar.")
        return 0

    errors: List[str] = []
    with requests.Session() as session:
        for item in clases:
            clase_id = item.get("geClaseId") if isinstance(item, dict) else None
            if clase_id is None:
                errors.append("Clase sin geClaseId.")
                continue
            try:
                _delete_clase_gestion_escolar(
                    session=session,
                    token=token,
                    clase_id=int(clase_id),
                    empresa_id=int(args.empresa_id),
                    ciclo_id=int(args.ciclo_id),
                    timeout=int(args.timeout),
                )
            except Exception as exc:
                errors.append(f"{clase_id}: {exc}")

    colegios = _collect_colegios(clases)
    if colegios:
        print("Colegios eliminados (id, nombre):")
        for item in colegios:
            print(f"{item['colegioId']}\t{item['colegio']}")

    eliminadas = len(clases) - len(errors)
    print(f"Clases eliminadas: {eliminadas}")
    if errors:
        print("Errores al eliminar:", file=sys.stderr)
        for item in errors:
            print(f"- {item}", file=sys.stderr)
        return 1

    return 0


def _run_profesores(args: argparse.Namespace) -> int:
    token = _clean_token(args.token)
    if not token:
        token = _clean_token(os.environ.get(args.token_env, ""))
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    try:
        nivel_ids = _parse_niveles(args.niveles)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    try:
        output_bytes, summary, errores = listar_profesores(
            token=token,
            colegio_id=int(args.colegio_id),
            nivel_ids=nivel_ids,
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            timeout=int(args.timeout),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    output_path = _resolve_output_path_profesores(args.output, int(args.colegio_id))
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(output_bytes)
    except Exception as exc:
        print(f"Error al escribir salida: {exc}", file=sys.stderr)
        return 1

    print(f"Archivo generado: {output_path.resolve()}")
    print(
        "Profesores: {profesores_total}, Errores detalle: {detalle_error}.".format(
            **summary
        )
    )
    if errores:
        print("Errores API:", file=sys.stderr)
        for err in errores[:10]:
            tipo = err.get("tipo", "")
            persona = err.get("persona_id", "")
            nivel = err.get("nivel_id", "")
            mensaje = err.get("error", "")
            print(f"- {tipo} persona={persona} nivel={nivel}: {mensaje}", file=sys.stderr)
        restantes = len(errores) - 10
        if restantes > 0:
            print(f"... y {restantes} mas.", file=sys.stderr)
    return 0


def _run_alumnos_plantilla(args: argparse.Namespace) -> int:
    token = _clean_token(args.token)
    if not token:
        token = _clean_token(os.environ.get(args.token_env, ""))
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    try:
        output_bytes, summary = descargar_plantilla_edicion_masiva(
            token=token,
            colegio_id=int(args.colegio_id),
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            timeout=int(args.timeout),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    output_path = _resolve_output_path_alumnos(args.output, int(args.colegio_id))
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(output_bytes)
    except Exception as exc:
        print(f"Error al escribir salida: {exc}", file=sys.stderr)
        return 1

    print(f"Archivo generado: {output_path.resolve()}")
    print(f"Alumnos: {summary['alumnos_total']}.")
    return 0


def _run_alumno_clases(args: argparse.Namespace) -> int:
    token = _clean_token(args.token)
    if not token:
        token = _clean_token(os.environ.get(args.token_env, ""))
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    login_target = str(args.login or "").strip()
    if not login_target:
        print("Error: el login no puede estar vacio.", file=sys.stderr)
        return 1
    login_target_lower = login_target.lower()

    try:
        with requests.Session() as session:
            clases = _fetch_clases_gestion_escolar(
                session=session,
                token=token,
                colegio_id=int(args.colegio_id),
                empresa_id=int(args.empresa_id),
                ciclo_id=int(args.ciclo_id),
                timeout=int(args.timeout),
            )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not clases:
        print("No se encontraron clases para ese colegio/ciclo.")
        return 0

    total = len(clases)
    errors: List[str] = []
    matches: List[Dict[str, object]] = []

    with requests.Session() as session:
        for index, item in enumerate(clases, start=1):
            if not isinstance(item, dict):
                errors.append("Clase con formato invalido.")
                _print_progress(index, total, "Clase con formato invalido")
                continue

            clase_id_raw = item.get("geClaseId")
            if clase_id_raw is None:
                errors.append("Clase sin geClaseId.")
                _print_progress(index, total, "Clase sin geClaseId")
                continue
            try:
                clase_id = int(clase_id_raw)
            except (TypeError, ValueError):
                errors.append(f"Clase con geClaseId invalido: {clase_id_raw}")
                _print_progress(index, total, "Clase con geClaseId invalido")
                continue

            clase_name = str(item.get("geClase") or item.get("geClaseClave") or "")
            try:
                clase_data = _fetch_alumnos_clase_gestion_escolar(
                    session=session,
                    token=token,
                    clase_id=clase_id,
                    empresa_id=int(args.empresa_id),
                    ciclo_id=int(args.ciclo_id),
                    timeout=int(args.timeout),
                )
            except Exception as exc:
                errors.append(f"{clase_id}: {exc}")
                _print_progress(index, total, f"{clase_id} error")
                continue

            alumnos_data = clase_data.get("claseAlumnos") or []
            if not isinstance(alumnos_data, list):
                errors.append(f"{clase_id}: campo claseAlumnos no es lista")
                _print_progress(index, total, f"{clase_id} claseAlumnos invalido")
                continue

            cgg = clase_data.get("colegioGradoGrupo") if isinstance(clase_data, dict) else None
            grado_info = cgg.get("grado") if isinstance(cgg, dict) else None
            grupo_info = cgg.get("grupo") if isinstance(cgg, dict) else None
            grado = str(grado_info.get("grado") or "") if isinstance(grado_info, dict) else ""
            grupo = str(grupo_info.get("grupo") or "") if isinstance(grupo_info, dict) else ""

            for entry in alumnos_data:
                if not isinstance(entry, dict):
                    continue
                alumno = entry.get("alumno")
                if not isinstance(alumno, dict):
                    continue
                persona = alumno.get("persona")
                if not isinstance(persona, dict):
                    continue
                persona_login = persona.get("personaLogin")
                if not isinstance(persona_login, dict):
                    continue

                login_value = str(persona_login.get("login") or "").strip()
                if login_value.lower() != login_target_lower:
                    continue
                if args.solo_activos and (
                    not bool(entry.get("activo", False))
                    or not bool(alumno.get("activo", False))
                ):
                    continue

                matches.append(
                    {
                        "clase_id": clase_id,
                        "clase": clase_name or str(clase_data.get("geClase") or ""),
                        "grado": grado,
                        "grupo": grupo,
                        "ge_clase_alumno_id": entry.get("geClaseAlumnoId", ""),
                        "alumno_id": alumno.get("alumnoId", ""),
                        "persona_id": persona.get("personaId", ""),
                        "login": login_value,
                        "nombre": persona.get("nombreCompleto") or "",
                        "activo_en_censo": alumno.get("activo", ""),
                        "activo_en_clase": entry.get("activo", ""),
                    }
                )

            _print_progress(index, total, f"{clase_id} {clase_name}".strip())

    print(f"Login buscado: {login_target}")
    print(f"Clases evaluadas: {total}")
    print(f"Clases con error: {len(errors)}")
    print(f"Coincidencias encontradas: {len(matches)}")

    if matches:
        print("clase_id\tclase\tgrado\tgrupo\talumno_id\tlogin\tnombre\tactivo_en_censo\tactivo_en_clase")
        for row in sorted(matches, key=lambda value: (int(value["clase_id"]), str(value["nombre"]))):
            print(
                "{clase_id}\t{clase}\t{grado}\t{grupo}\t{alumno_id}\t{login}\t{nombre}\t{activo_en_censo}\t{activo_en_clase}".format(
                    **row
                )
            )
    else:
        print("No se encontro ese login en las clases consultadas.")

    if errors:
        print("Errores por clase:", file=sys.stderr)
        for err in errors[:20]:
            print(f"- {err}", file=sys.stderr)
        restantes = len(errors) - 20
        if restantes > 0:
            print(f"... y {restantes} mas.", file=sys.stderr)

    return 0


def _run_alumnos_comparar(args: argparse.Namespace) -> int:
    excel_path = Path(args.ruta_excel)
    if not excel_path.exists():
        print(f"Error: no existe el archivo: {excel_path}", file=sys.stderr)
        return 1

    try:
        output_bytes, summary = comparar_plantillas(
            excel_path=excel_path,
            sheet_bd=args.sheet_bd,
            sheet_actualizada=args.sheet_actualizada,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    output_path = _resolve_output_path_comparar(args.output, excel_path)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(output_bytes)
    except Exception as exc:
        print(f"Error al escribir salida: {exc}", file=sys.stderr)
        return 1

    print(f"Archivo generado: {output_path.resolve()}")
    print(
        "Base: {base_total}, Actualizada: {actualizados_total}, "
        "Match NUIP: {nuip_match}, Nuevos: {nuevos_total}.".format(**summary)
    )
    return 0

def _run_profesores_clases(args: argparse.Namespace) -> int:
    token = _clean_token(args.token)
    if not token:
        token = _clean_token(os.environ.get(args.token_env, ""))
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    excel_path = Path(args.ruta_excel)
    if not excel_path.exists():
        print(f"Error: no existe el archivo: {excel_path}", file=sys.stderr)
        return 1

    dry_run = not bool(args.apply)
    do_grupos = not bool(args.no_grupos)
    if dry_run and not args.solo_estado and not args.compact:
        print("Modo simulacion: no se aplican cambios.")

    if args.solo_estado:
        warnings_module.filterwarnings(
            "ignore",
            message="Data Validation extension is not supported and will be removed",
            category=UserWarning,
        )
        ids: List[int] = []
        seen: set = set()

        def _on_estado_change(
            persona_id: int,
            nivel_id: int,
            desired_active: bool,
            current_active: Optional[bool],
        ) -> None:
            if persona_id in seen:
                return
            seen.add(persona_id)
            ids.append(int(persona_id))

        try:
            asignar_profesores_clases(
                token=token,
                empresa_id=int(args.empresa_id),
                ciclo_id=int(args.ciclo_id),
                colegio_id=int(args.colegio_id),
                excel_path=excel_path,
                sheet_name=args.sheet or None,
                timeout=int(args.timeout),
                dry_run=not bool(args.apply),
                remove_missing=False,
                on_log=None,
                list_estado_only=True,
                on_estado_change=_on_estado_change,
                do_grupos=do_grupos,
            )
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1

        ids_text = ",".join(str(pid) for pid in ids)
        print(f"Total a actualizar: {len(ids)}")
        print(f"IDs: ({ids_text})")
        return 0

    if args.compact:
        warnings_module.filterwarnings(
            "ignore",
            message="Data Validation extension is not supported and will be removed",
            category=UserWarning,
        )

    try:
        summary, warnings, errors = asignar_profesores_clases(
            token=token,
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            colegio_id=int(args.colegio_id),
            excel_path=excel_path,
            sheet_name=args.sheet or None,
            timeout=int(args.timeout),
            dry_run=dry_run,
            remove_missing=bool(args.remove_missing),
            on_log=None if args.compact else print,
            collect_compact=bool(args.compact),
            on_progress=lambda phase, current, total, msg: _print_progress(
                current, total, f"{phase}: {msg}"
            ),
            do_grupos=do_grupos,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.compact:
        listado = summary.get("listado_compacto") or {}

        def _join_ids(values) -> str:
            if not values:
                return "()"
            ordered = sorted(int(v) for v in values)
            return "(" + ",".join(str(v) for v in ordered) + ")"

        def _join_clases(mapping) -> str:
            if not mapping:
                return "()"
            parts = []
            for key in sorted(mapping.keys()):
                ids_text = _join_ids(mapping[key])
                parts.append(f"{key}={ids_text}")
            return "; ".join(parts)

        asignar = listado.get("asignar") or {}
        eliminar = listado.get("eliminar") or {}
        activar = listado.get("activar") or set()
        inactivar = listado.get("inactivar") or set()
        niveles = listado.get("niveles") or set()

        print(f"Niveles: {_join_ids(niveles)}")
        print(f"Activar: {_join_ids(activar)}")
        print(f"Inactivar: {_join_ids(inactivar)}")
        print(f"Asignar: {_join_clases(asignar)}")
        print(f"Eliminar: {_join_clases(eliminar)}")
        if errors:
            print("Errores API:")
            for err in errors:
                tipo = err.get("tipo", "")
                persona = err.get("persona_id", "")
                clase_id = err.get("clase_id", "")
                clase = err.get("clase", "")
                nivel = err.get("nivel_id", "")
                mensaje = err.get("error", "")
                detail_parts = [f"tipo={tipo}"]
                if persona:
                    detail_parts.append(f"persona={persona}")
                if nivel:
                    detail_parts.append(f"nivel={nivel}")
                if clase_id:
                    detail_parts.append(f"clase={clase_id}")
                if clase:
                    detail_parts.append(str(clase))
                if mensaje:
                    detail_parts.append(str(mensaje))
                print("- " + " ".join(detail_parts))
        return 0

    print("")
    print(
        "Docentes procesados: {docentes_procesados}, "
        "Omitidos (no colegio): {docentes_omitidos_no_colegio}, "
        "Clases encontradas: {clases_encontradas}, "
        "Asignaciones nuevas: {asignaciones_nuevas}, "
        "Asignaciones omitidas: {asignaciones_omitidas}, "
        "Grupos asignados: {grupos_asignados}, "
        "Grupos omitidos: {grupos_omitidos}, "
        "Docentes sin match: {docentes_sin_match}, "
        "Eliminaciones: {eliminaciones}, "
        "Estado activaciones: {estado_activaciones}, "
        "Estado inactivaciones: {estado_inactivaciones}, "
        "Estado omitidas: {estado_omitidas}.".format(**summary)
    )
    if summary.get("docentes_invalidos"):
        print(f"Docentes invalidos: {summary['docentes_invalidos']}", file=sys.stderr)
    if warnings:
        print("Avisos:", file=sys.stderr)
        for warn in warnings[:10]:
            print(f"- {warn}", file=sys.stderr)
        restantes = len(warnings) - 10
        if restantes > 0:
            print(f"... y {restantes} mas.", file=sys.stderr)
    if errors:
        print("Errores API:", file=sys.stderr)
        for err in errors:
            tipo = err.get("tipo", "")
            persona = err.get("persona_id", "")
            clase_id = err.get("clase_id", "")
            clase = err.get("clase", "")
            nivel = err.get("nivel_id", "")
            mensaje = err.get("error", "")
            detail_parts = [f"tipo={tipo}"]
            if persona:
                detail_parts.append(f"persona={persona}")
            if nivel:
                detail_parts.append(f"nivel={nivel}")
            if clase_id:
                detail_parts.append(f"clase={clase_id}")
            if clase:
                detail_parts.append(str(clase))
            if mensaje:
                detail_parts.append(str(mensaje))
            print("- " + " ".join(detail_parts), file=sys.stderr)

    return 0

def _run_profesores_password(args: argparse.Namespace) -> int:
    token = _clean_token(args.token)
    if not token:
        token = _clean_token(os.environ.get(args.token_env, ""))
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    excel_path = Path(args.ruta_excel)
    if not excel_path.exists():
        print(f"Error: no existe el archivo: {excel_path}", file=sys.stderr)
        return 1

    dry_run = not bool(args.apply)
    if dry_run:
        print("Modo simulacion: no se aplican cambios.")

    try:
        summary, warnings, errors = actualizar_passwords_docentes(
            token=token,
            colegio_id=int(args.colegio_id),
            excel_path=excel_path,
            sheet_name=args.sheet or None,
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            timeout=int(args.timeout),
            dry_run=dry_run,
            on_progress=lambda current, total, msg: _print_progress(
                current, total, f"Passwords: {msg}"
            ),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(
        "Docentes: {docentes_total}, "
        "Niveles: {niveles_total}, "
        "Actualizaciones: {actualizaciones}, "
        "Errores API: {errores_api}.".format(**summary)
    )
    if warnings:
        print("Avisos:", file=sys.stderr)
        for warn in warnings[:10]:
            print(f"- {warn}", file=sys.stderr)
        restantes = len(warnings) - 10
        if restantes > 0:
            print(f"... y {restantes} mas.", file=sys.stderr)
    if errors:
        print("Errores API:", file=sys.stderr)
        for err in errors:
            persona = err.get("persona_id", "")
            nivel = err.get("nivel_id", "")
            mensaje = err.get("error", "")
            detail_parts = []
            if persona:
                detail_parts.append(f"persona={persona}")
            if nivel:
                detail_parts.append(f"nivel={nivel}")
            if mensaje:
                detail_parts.append(str(mensaje))
            print("- " + " ".join(detail_parts), file=sys.stderr)

    return 0


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if (
        argv
        and argv[0]
        not in {
            "clases",
            "clases-api",
            "alumnos-plantilla",
            "alumno-clases",
            "alumnos-comparar",
            "profesores",
            "profesores-clases",
            "profesores-password",
        }
        and not argv[0].startswith("-")
    ):
        argv = ["clases", *argv]
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "clases":
        return _run_clases(args)
    if args.command == "clases-api":
        return _run_clases_api(args)
    if args.command == "profesores":
        return _run_profesores(args)
    if args.command == "alumnos-plantilla":
        return _run_alumnos_plantilla(args)
    if args.command == "alumno-clases":
        return _run_alumno_clases(args)
    if args.command == "alumnos-comparar":
        return _run_alumnos_comparar(args)
    if args.command == "profesores-clases":
        return _run_profesores_clases(args)
    if args.command == "profesores-password":
        return _run_profesores_password(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
