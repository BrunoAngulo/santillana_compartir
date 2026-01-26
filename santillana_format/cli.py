import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import requests

from .alumnos import DEFAULT_EMPRESA_ID
from .duplicados import (
    BASE_SHEET_NAME,
    NUEVO_SHEET_NAME,
    build_comparacion_clave,
    build_comparacion_grado_seccion_diferente,
    build_comparacion_nombre,
    compare_alumnos,
    export_alumnos_excel,
    read_alumnos_file,
    select_comparacion_basica,
    select_comparacion_con_grado,
)
from .processor import (
    CODE_COLUMN_NAME,
    OUTPUT_FILENAME,
    SHEET_NAME,
    cargar_excel,
    exportar_excel,
    filtrar_codigo,
    transformar,
)
from .profesores import (
    DEFAULT_CICLO_ID as PROFESORES_CICLO_ID_DEFAULT,
    NIVEL_MAP as PROFESORES_NIVEL_MAP,
    build_profesores_filename,
    listar_profesores,
)
from .profesores_sync import sync_profesores
from .profesores_clases import asignar_profesores_clases

OUTPUT_DIR_CLASES = Path("salidas") / "Clases"
OUTPUT_DIR_PROFESORES = Path("salidas") / "Profesores"
NEWLIST_DIR = Path("alumnos_newList")
OLDLIST_DIR = Path("alumnos_oldList")
REGISTERLIST_DIR = Path("alumnos_registerList")
SUMMARY_COLUMNS = [
    "Institucion",
    "Nivel Educativo",
    "Grado",
    "Asignatura Producto",
    "Producto",
    "Plataforma",
    "Razon Estado",
]
GESTION_ESCOLAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/"
    "{empresa_id}/ciclos/{ciclo_id}/clases"
)
GESTION_ESCOLAR_CICLO_ID_DEFAULT = 207


def _print_summary(df) -> None:
    columns_present = [col for col in SUMMARY_COLUMNS if col in df.columns]
    if not columns_present:
        print("No se pueden mostrar columnas solicitadas; ninguna esta presente.")
        return
    resumen = df[columns_present]
    print("\nResumen filtrado por codigo:\n")
    print(resumen.to_string(index=False))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Herramientas para crear clases, depurar alumnos o gestionar clases."
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
        help="Ruta del Excel de salida (default: salidas/profesores_<colegio_id>.xlsx).",
    )
    parser_profesores.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )

    parser_profesores_sync = subparsers.add_parser(
        "profesores-sync",
        help="Sincronizar profesores con un Excel (activar/inactivar y asignar niveles).",
    )
    parser_profesores_sync.add_argument(
        "ruta_excel",
        help="Ruta del archivo Excel con profesores activos.",
    )
    parser_profesores_sync.add_argument(
        "--sheet",
        default="",
        help="Hoja del Excel (default: primera hoja).",
    )
    parser_profesores_sync.add_argument(
        "--token",
        default="",
        help="Bearer token (sin el prefijo 'Bearer').",
    )
    parser_profesores_sync.add_argument(
        "--token-env",
        default="PEGASUS_TOKEN",
        help="Nombre de la variable de entorno con el token.",
    )
    parser_profesores_sync.add_argument(
        "--colegio-id",
        type=int,
        required=True,
        help="ID del colegio.",
    )
    parser_profesores_sync.add_argument(
        "--empresa-id",
        type=int,
        default=DEFAULT_EMPRESA_ID,
        help="Empresa ID (default: 11).",
    )
    parser_profesores_sync.add_argument(
        "--ciclo-id",
        type=int,
        default=PROFESORES_CICLO_ID_DEFAULT,
        help="Ciclo ID (default: 207).",
    )
    parser_profesores_sync.add_argument(
        "--niveles",
        default="",
        help="Niveles a consultar (Inicial,Primaria,Secundaria o IDs).",
    )
    parser_profesores_sync.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )
    parser_profesores_sync.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo muestra el resumen; no aplica cambios en API.",
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

    parser_depurar = subparsers.add_parser(
        "depurar", help="Comparar alumnos y quitar repetidos."
    )
    parser_depurar.add_argument("base", help="Ruta del archivo base.")
    parser_depurar.add_argument("nuevo", help="Ruta del archivo nuevo.")
    parser_depurar.add_argument(
        "--sheet-base",
        dest="sheet_base",
        default=BASE_SHEET_NAME,
        help=f"Hoja del archivo base (default: {BASE_SHEET_NAME}).",
    )
    parser_depurar.add_argument(
        "--sheet-nuevo",
        dest="sheet_nuevo",
        default=NUEVO_SHEET_NAME,
        help=f"Hoja del archivo nuevo (default: {NUEVO_SHEET_NAME}).",
    )
    parser_depurar.add_argument(
        "--output",
        default="",
        help="Ruta del Excel de salida (default: alumnos_registerList/<nuevo>_sin_repetidos.xlsx).",
    )

    return parser


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


def _build_gestion_escolar_url(
    empresa_id: int, ciclo_id: int, colegio_id: Optional[int] = None, clase_id: Optional[int] = None
) -> str:
    base = GESTION_ESCOLAR_URL.format(empresa_id=empresa_id, ciclo_id=ciclo_id)
    if clase_id is not None:
        return f"{base}/{clase_id}"
    if colegio_id is not None:
        return f"{base}?colegioId={colegio_id}"
    return base


def _fetch_clases_gestion_escolar(
    session: requests.Session,
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> List[Dict[str, object]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = _build_gestion_escolar_url(
        empresa_id=empresa_id, ciclo_id=ciclo_id, colegio_id=colegio_id
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

    data = payload.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError("Campo data no es lista")
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
    url = _build_gestion_escolar_url(
        empresa_id=empresa_id, ciclo_id=ciclo_id, clase_id=clase_id
    )
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


def _resolve_output_path_depurar(output: str, nuevo_path: Path) -> Path:
    if output:
        path = Path(output)
        if path.is_dir():
            return path / f"{nuevo_path.stem}_sin_repetidos.xlsx"
        return path
    return REGISTERLIST_DIR / f"{nuevo_path.stem}_sin_repetidos.xlsx"


def _resolve_output_path_profesores(output: str, colegio_id: int) -> Path:
    if output:
        path = Path(output)
        if path.is_dir():
            return path / build_profesores_filename(colegio_id)
        return path
    return OUTPUT_DIR_PROFESORES / build_profesores_filename(colegio_id)


def _looks_like_file_path(value: str) -> bool:
    if not value:
        return False
    return Path(value).suffix.lower() in {".xlsx", ".csv", ".txt"}


def _format_compare_rows(df) -> str:
    if df.empty:
        return ""
    base_cols = [col for col in df.columns if col.startswith("base_")]
    nuevo_cols = [col for col in df.columns if col.startswith("nuevo_")]
    lines: List[str] = []
    for _, row in df.iterrows():
        base_parts = [f"{col[5:]}: {row[col]}" for col in base_cols]
        nuevo_parts = [f"{col[6:]}: {row[col]}" for col in nuevo_cols]
        lines.append("Base -> " + ", ".join(base_parts))
        lines.append("Nuevo -> " + ", ".join(nuevo_parts))
        lines.append("")
    return "\n".join(lines).rstrip()


def _resolve_input_path(path_value: str, fallback_dir: Path) -> Path:
    path = Path(path_value)
    if path.exists():
        return path
    if not path.is_absolute():
        candidate = fallback_dir / path
        if candidate.exists():
            return candidate
    return path


def _run_clases(args: argparse.Namespace) -> int:
    ruta_archivo = Path(args.ruta_excel)
    try:
        grupos = _parse_grupo_letras(args.grupos) if args.grupos else ["A"]
        df = cargar_excel(ruta_archivo, hoja=args.hoja, columna_codigo=args.columna_codigo)
        df_filtrado = filtrar_codigo(df, args.codigo, args.columna_codigo)

        if df_filtrado.empty:
            print(f"No se encontraron filas para el codigo {args.codigo}.")
            return 0

        _print_summary(df_filtrado)

        df_transformado = transformar(df_filtrado, grupos=grupos)
        if df_transformado.empty:
            print("No hay filas que cumplan con las reglas de transformacion.")
            return 0

        plantilla_path = Path(OUTPUT_FILENAME)
        plantilla_path = plantilla_path if plantilla_path.exists() else None

        output_bytes = exportar_excel(
            df_transformado,
            plantilla_path=plantilla_path,
        )

        OUTPUT_DIR_CLASES.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR_CLASES / f"{Path(OUTPUT_FILENAME).stem}_{args.codigo}.xlsx"
        output_path.write_bytes(output_bytes)
        print(f"\nArchivo generado: {output_path.resolve()}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _run_clases_api(args: argparse.Namespace) -> int:
    token = args.token.strip()
    if not token:
        token = os.environ.get(args.token_env, "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
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
    token = args.token.strip()
    if not token:
        token = os.environ.get(args.token_env, "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
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
    print(f"Profesores encontrados: {summary['profesores_total']}")
    if summary["niveles_error"] or summary["detalle_error"]:
        print(
            f"Errores listado: {summary['niveles_error']}, detalle: {summary['detalle_error']}",
            file=sys.stderr,
        )
        max_errors = 10
        for error in errores[:max_errors]:
            tipo = error.get("tipo", "")
            nivel_id = error.get("nivel_id", "")
            persona_id = error.get("persona_id", "")
            mensaje = error.get("error", "")
            print(
                f"- {tipo} nivel={nivel_id} persona={persona_id}: {mensaje}",
                file=sys.stderr,
            )
        restantes = len(errores) - max_errors
        if restantes > 0:
            print(f"... y {restantes} errores mas.", file=sys.stderr)

    return 0


def _run_profesores_sync(args: argparse.Namespace) -> int:
    token = args.token.strip()
    if not token:
        token = os.environ.get(args.token_env, "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    excel_path = Path(args.ruta_excel)
    if not excel_path.exists():
        print(f"Error: no existe el archivo: {excel_path}", file=sys.stderr)
        return 1

    try:
        nivel_ids = _parse_niveles(args.niveles)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    try:
        summary, warnings, errors = sync_profesores(
            token=token,
            colegio_id=int(args.colegio_id),
            excel_path=excel_path,
            sheet_name=args.sheet or None,
            nivel_ids=nivel_ids,
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            timeout=int(args.timeout),
            dry_run=bool(args.dry_run),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print("Modo dry-run: no se aplicaron cambios.")

    print(
        "Excel grupos: {excel_grupos}, API profesores: {api_profesores}, "
        "Activar: {activar}, Inactivar: {inactivar}, Asignar nivel: {asignar_nivel}.".format(
            **summary
        )
    )
    if summary.get("excel_no_en_api"):
        print(f"Excel sin match en API: {summary['excel_no_en_api']}", file=sys.stderr)

    if warnings:
        print("Avisos:", file=sys.stderr)
        for warn in warnings[:10]:
            print(f"- {warn}", file=sys.stderr)
        restantes = len(warnings) - 10
        if restantes > 0:
            print(f"... y {restantes} mas.", file=sys.stderr)

    if errors:
        print("Errores API:", file=sys.stderr)
        for err in errors[:10]:
            tipo = err.get("tipo", "")
            persona = err.get("persona_id", "")
            nivel = err.get("nivel_id", "")
            mensaje = err.get("error", "")
            print(f"- {tipo} persona={persona} nivel={nivel}: {mensaje}", file=sys.stderr)
        restantes = len(errors) - 10
        if restantes > 0:
            print(f"... y {restantes} mas.", file=sys.stderr)

    return 0


def _run_profesores_clases(args: argparse.Namespace) -> int:
    token = args.token.strip()
    if not token:
        token = os.environ.get(args.token_env, "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    excel_path = Path(args.ruta_excel)
    if not excel_path.exists():
        print(f"Error: no existe el archivo: {excel_path}", file=sys.stderr)
        return 1

    dry_run = not bool(args.apply)
    if dry_run:
        print("Modo simulacion: no se aplican POST.")

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
            on_log=print,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print("")
    print(
        "Docentes procesados: {docentes_procesados}, "
        "Clases encontradas: {clases_encontradas}, "
        "Asignaciones nuevas: {asignaciones_nuevas}, "
        "Asignaciones omitidas: {asignaciones_omitidas}, "
        "Docentes sin match: {docentes_sin_match}, "
        "Eliminaciones: {eliminaciones}.".format(**summary)
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
        for err in errors[:10]:
            tipo = err.get("tipo", "")
            persona = err.get("persona_id", "")
            clase_id = err.get("clase_id", "")
            clase = err.get("clase", "")
            mensaje = err.get("error", "")
            print(
                f"- {tipo} persona={persona} clase={clase_id} {clase}: {mensaje}",
                file=sys.stderr,
            )
        restantes = len(errors) - 10
        if restantes > 0:
            print(f"... y {restantes} mas.", file=sys.stderr)

    return 0


def _run_depurar(args: argparse.Namespace) -> int:
    base_path = _resolve_input_path(args.base, OLDLIST_DIR)
    nuevo_path = _resolve_input_path(args.nuevo, NEWLIST_DIR)
    if not base_path.exists():
        hint = ""
        if not Path(args.base).is_absolute():
            hint = f" (tampoco en {OLDLIST_DIR / args.base})"
        print(f"Error: no existe el archivo base: {base_path}{hint}", file=sys.stderr)
        return 1
    if not nuevo_path.exists():
        hint = ""
        if not Path(args.nuevo).is_absolute():
            hint = f" (tampoco en {NEWLIST_DIR / args.nuevo})"
        print(f"Error: no existe el archivo nuevo: {nuevo_path}{hint}", file=sys.stderr)
        return 1

    try:
        df_base = read_alumnos_file(
            base_path.read_bytes(),
            base_path.name,
            sheet_name=args.sheet_base,
        )
        df_nuevo = read_alumnos_file(
            nuevo_path.read_bytes(),
            nuevo_path.name,
            sheet_name=args.sheet_nuevo,
        )
        repetidos, filtrados, summary = compare_alumnos(df_base, df_nuevo)
        comparacion_clave = build_comparacion_clave(df_base, df_nuevo)
        comparacion_nombre = build_comparacion_nombre(df_base, df_nuevo)
        comparacion_diferente = build_comparacion_grado_seccion_diferente(
            df_base, df_nuevo
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(
        "Listo. Base: {base_total}, Nuevo: {nuevo_total}, Repetidos: {repetidos}, "
        "Sin repetir: {sin_repetir}.".format(**summary)
    )
    if summary["base_sin_clave"] or summary["nuevo_sin_clave"]:
        print(
            "Filas sin clave de comparacion. Base: {base}, Nuevo: {nuevo}.".format(
                base=summary["base_sin_clave"],
                nuevo=summary["nuevo_sin_clave"],
            )
        )

    if comparacion_clave.empty:
        print("No se encontraron alumnos repetidos.")
    else:
        print("\nAlumnos repetidos (base vs nuevo):")
        clean = select_comparacion_basica(comparacion_clave)
        print(_format_compare_rows(clean))

    if comparacion_nombre.empty:
        print("No se encontraron coincidencias por nombre y apellidos.")
    else:
        print("\nCoincidencias por nombre y apellidos (base vs nuevo):")
        clean = select_comparacion_basica(comparacion_nombre)
        print(_format_compare_rows(clean))

    if comparacion_diferente.empty:
        print("No se encontraron repetidos con diferente grado/seccion.")
    else:
        print("\nRepetidos con diferente grado/seccion (base vs nuevo):")
        clean = select_comparacion_con_grado(comparacion_diferente)
        print(_format_compare_rows(clean))

    output_path = _resolve_output_path_depurar(args.output, nuevo_path)
    try:
        output_bytes = export_alumnos_excel(filtrados)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(output_bytes)
    except Exception as exc:
        print(f"Error al escribir salida: {exc}", file=sys.stderr)
        return 1

    print(f"Archivo generado: {output_path.resolve()}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if (
        argv
        and argv[0]
        not in {
            "clases",
            "depurar",
            "clases-api",
            "profesores",
            "profesores-sync",
            "profesores-clases",
        }
        and not argv[0].startswith("-")
    ):
        if len(argv) >= 2 and _looks_like_file_path(argv[0]) and _looks_like_file_path(argv[1]):
            argv = ["depurar", *argv]
        else:
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
    if args.command == "profesores-sync":
        return _run_profesores_sync(args)
    if args.command == "profesores-clases":
        return _run_profesores_clases(args)
    if args.command == "depurar":
        return _run_depurar(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
