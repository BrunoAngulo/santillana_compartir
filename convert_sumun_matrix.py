from __future__ import annotations

import argparse
from pathlib import Path

from santillana_format.sumun import generate_sumun_template_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convierte una matriz SUMUN en la plantilla plana de carga."
    )
    parser.add_argument("matrix", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--area")
    parser.add_argument("--grade", type=int)
    parser.add_argument("--level", default="Secundaria")
    parser.add_argument("--course-code")
    parser.add_argument(
        "--sheet",
        action="append",
        dest="sheets",
        help="Nombre de hoja a procesar. Se puede repetir.",
    )
    args = parser.parse_args()

    summary = generate_sumun_template_file(
        args.matrix,
        args.output,
        area=args.area,
        grade=args.grade,
        level=args.level,
        course_code=args.course_code,
        sheet_names=args.sheets,
    )
    print(f"Archivo creado: {args.output}")
    print(f"Filas generadas: {summary.generated_rows}")
    print(f"Prefijo ID: {summary.prefix}")
    print(f"Hojas procesadas: {', '.join(summary.processed_sheets)}")
    if summary.nonnumber_station_rows:
        print(
            "Filas omitidas por estacion no identificable: "
            + ", ".join(summary.nonnumber_station_rows)
        )


if __name__ == "__main__":
    main()
