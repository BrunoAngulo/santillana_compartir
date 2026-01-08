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

OUTPUT_DIR = Path("salidas")
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

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / f"{Path(OUTPUT_FILENAME).stem}_{args.codigo}.xlsx"
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
        and argv[0] not in {"clases", "depurar", "clases-api"}
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
    if args.command == "depurar":
        return _run_depurar(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
