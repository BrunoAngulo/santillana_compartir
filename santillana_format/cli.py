import argparse
import os
import re
import sys
from pathlib import Path
from typing import List, Optional

from .alumnos import (
    DEFAULT_CICLO_ID,
    DEFAULT_EMPRESA_ID,
    GRADOS_POR_NIVEL,
    GRUPO_LETRA_TO_ID,
    NIVEL_MAP,
    build_alumnos_filename,
    listar_alumnos,
    parse_id_list,
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
SUMMARY_COLUMNS = [
    "Institucion",
    "Nivel Educativo",
    "Grado",
    "Asignatura Producto",
    "Producto",
    "Plataforma",
    "Razon Estado",
]


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
        description="Herramientas para crear clases o listar alumnos."
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

    parser_alumnos = subparsers.add_parser(
        "alumnos", help="Listar alumnos desde Pegasus y exportar a Excel."
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
        "--colegios",
        required=True,
        help="IDs de colegios, separados por coma o rango (ej: 25947,13255 o 25947-25949).",
    )
    parser_alumnos.add_argument(
        "--niveles",
        default="",
        help="Niveles por nombre o ID (Inicial,Primaria,Secundaria o 38,39,40).",
    )
    parser_alumnos.add_argument(
        "--grupos",
        default="",
        help="Secciones por letra o ID (A,B,C o 661,662).",
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
        default=DEFAULT_CICLO_ID,
        help="Ciclo ID (default: 206).",
    )
    parser_alumnos.add_argument(
        "--output",
        default="",
        help="Ruta del Excel de salida (default: salidas/alumnos_<ciclo>.xlsx).",
    )
    parser_alumnos.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout HTTP en segundos (default: 30).",
    )

    return parser


def _parse_niveles(value: str) -> List[int]:
    if not value:
        return []
    nivel_lookup = {key.lower(): nivel_id for key, nivel_id in NIVEL_MAP.items()}
    ids: List[int] = []
    invalid: List[str] = []
    for token in re.split(r"[\s,]+", value.strip()):
        if not token:
            continue
        if token.isdigit():
            ids.append(int(token))
            continue
        key = token.lower()
        if key in nivel_lookup:
            ids.append(nivel_lookup[key])
        else:
            invalid.append(token)
    if invalid:
        raise ValueError(f"Niveles invalidos: {', '.join(invalid)}")
    return sorted(set(ids))


def _parse_grupos(value: str) -> List[int]:
    if not value:
        return []
    ids: List[int] = []
    invalid: List[str] = []
    for token in re.split(r"[\s,]+", value.strip()):
        if not token:
            continue
        upper = token.upper()
        if upper in GRUPO_LETRA_TO_ID:
            ids.append(GRUPO_LETRA_TO_ID[upper])
        elif token.isdigit():
            ids.append(int(token))
        else:
            invalid.append(token)
    if invalid:
        raise ValueError(f"Grupos invalidos: {', '.join(invalid)}")
    return sorted(set(ids))


def _resolve_output_path(output: str, colegio_ids: List[int]) -> Path:
    if output:
        path = Path(output)
        if path.is_dir():
            return path / build_alumnos_filename(colegio_ids)
        return path
    return OUTPUT_DIR / build_alumnos_filename(colegio_ids)


def _run_clases(args: argparse.Namespace) -> int:
    ruta_archivo = Path(args.ruta_excel)
    try:
        df = cargar_excel(ruta_archivo, hoja=args.hoja, columna_codigo=args.columna_codigo)
        df_filtrado = filtrar_codigo(df, args.codigo, args.columna_codigo)

        if df_filtrado.empty:
            print(f"No se encontraron filas para el codigo {args.codigo}.")
            return 0

        _print_summary(df_filtrado)

        df_transformado = transformar(df_filtrado)
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


def _run_alumnos(args: argparse.Namespace) -> int:
    token = args.token.strip()
    if not token:
        token = os.environ.get(args.token_env, "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        print("Error: falta el token. Usa --token o la variable de entorno.", file=sys.stderr)
        return 1

    try:
        colegio_ids = parse_id_list(args.colegios)
        if not colegio_ids:
            raise ValueError("No se detectaron IDs de colegios.")
        nivel_ids = _parse_niveles(args.niveles) if args.niveles else list(NIVEL_MAP.values())
        grupo_ids = _parse_grupos(args.grupos) if args.grupos else sorted(GRUPO_LETRA_TO_ID.values())
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    total = len(colegio_ids) * sum(
        len(GRADOS_POR_NIVEL.get(nivel_id, {})) for nivel_id in nivel_ids
    ) * len(grupo_ids)
    print(f"Solicitudes estimadas: {total}")

    def _on_progress(actual: int, total_requests: int) -> None:
        total_safe = max(total_requests, 1)
        percent = int((actual / total_safe) * 100)
        bar_width = 30
        filled = int(bar_width * percent / 100)
        bar = "#" * filled + "-" * (bar_width - filled)
        sys.stdout.write(f"\rProgreso: [{bar}] {percent}% ({actual}/{total_requests})")
        sys.stdout.flush()
        if actual >= total_requests:
            sys.stdout.write("\n")

    try:
        output_bytes, summary = listar_alumnos(
            token=token,
            colegio_ids=colegio_ids,
            nivel_ids=nivel_ids,
            grupo_ids=grupo_ids,
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            timeout=int(args.timeout),
            on_progress=_on_progress,
        )
        output_path = _resolve_output_path(args.output, colegio_ids)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(output_bytes)
        print(
            "Listo. Solicitudes: {total}, Errores: {errores}, Alumnos: {alumnos}.".format(
                total=summary["solicitudes_total"],
                errores=summary["solicitudes_error"],
                alumnos=summary["alumnos_total"],
            )
        )
        print(f"Archivo generado: {output_path.resolve()}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] not in {"clases", "alumnos"} and not argv[0].startswith("-"):
        argv = ["clases", *argv]
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "clases":
        return _run_clases(args)
    if args.command == "alumnos":
        return _run_alumnos(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
