import argparse
import re
import sys
import unicodedata
from typing import Dict, List, Optional, Set, Tuple

import requests

from santillana_format.clases_api import listar_y_mapear_clases

CENSO_ALUMNOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnos"
)
CENSO_NIVELES_GRADOS_GRUPOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/alumnos/nivelesGradosGrupos"
)
ALUMNO_ACTIVAR_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/grados/{grado_id}/grupos/{grupo_id}/alumnos/{alumno_id}/activarInactivar"
)
ALUMNO_MOVER_URL = (
    "https://www.uno-internacional.com/pegasus-api/censo/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/colegios/{colegio_id}/niveles/{nivel_id}"
    "/grados/{grado_id}/grupos/{grupo_id}/alumnos/{alumno_id}/mover"
)
CLASE_ALUMNOS_URL = (
    "https://www.uno-internacional.com/pegasus-api/gestionEscolar/empresas/{empresa_id}"
    "/ciclos/{ciclo_id}/clases/{clase_id}/alumnos"
)

GRADOS_POR_NIVEL = {
    38: [112, 113, 114, 115],
    39: [119, 120, 121, 122, 123, 124],
    40: [126, 127, 128, 129, 130],
}
MOVE_ONLY_NIVEL_ID = 40
MOVE_ONLY_GRADO_ID = 130
MOVE_ONLY_GRUPO_ID = 685


def _clean_token(token: str) -> str:
    text = str(token or "").strip()
    if text.lower().startswith("bearer "):
        return text[7:].strip()
    return text


def _safe_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_true(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    text = str(value or "").strip().upper()
    return text in {"1", "TRUE", "SI", "YES"}


def _normalize_text(value: object) -> str:
    text = str(value or "").strip().upper()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_id_oficial(value: object) -> str:
    return re.sub(r"\W+", "", _normalize_text(value))


def _fetch_alumnos_censo(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    timeout: int,
) -> List[Dict[str, object]]:
    url = CENSO_ALUMNOS_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {
        "nivelId": int(nivel_id),
        "gradoId": int(grado_id),
        "grupoId": int(grupo_id),
    }
    try:
        response = requests.get(url, headers=headers, params=params, timeout=int(timeout))
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Error de red al listar alumnos (nivel={nivel_id}, grado={grado_id}, grupo={grupo_id}): {exc}"
        ) from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Alumnos: respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"Alumnos HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        raise RuntimeError(message or "Alumnos: respuesta invalida")

    data = payload.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError("Alumnos: campo data no es lista")
    return data


def _fetch_niveles_grados_grupos_censo(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    timeout: int,
) -> List[Dict[str, object]]:
    url = CENSO_NIVELES_GRADOS_GRUPOS_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers, timeout=int(timeout))
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de red al listar niveles/grados/grupos: {exc}") from exc

    status_code = response.status_code
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Niveles: respuesta no JSON (status {status_code})") from exc

    if not response.ok:
        message = payload.get("message") if isinstance(payload, dict) else ""
        raise RuntimeError(message or f"Niveles HTTP {status_code}")

    if not isinstance(payload, dict) or not payload.get("success", False):
        message = payload.get("message") if isinstance(payload, dict) else "Respuesta invalida"
        raise RuntimeError(message or "Niveles: respuesta invalida")

    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError("Niveles: campo data no es objeto")
    niveles = data.get("niveles") or []
    if not isinstance(niveles, list):
        raise RuntimeError("Niveles: campo data.niveles no es lista")
    return niveles


def _print_secciones_disponibles(niveles: List[Dict[str, object]]) -> None:
    print("")
    print("SECCIONES DISPONIBLES DEL COLEGIO")
    print(
        "NivelId\tNivel\tGradoId\tGrado\tGrupoId\tSeccion\tColegioGradoGrupoId\t"
        "Alumnos\tInactivos\tContratados\tIsContratado"
    )
    rows_count = 0
    for nivel_entry in niveles:
        if not isinstance(nivel_entry, dict):
            continue
        nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id = _safe_int(nivel.get("nivelId"))
        nivel_nombre = str(nivel.get("nivel") or "").strip()
        grados = nivel_entry.get("grados") or []
        if not isinstance(grados, list):
            continue
        for grado_entry in grados:
            if not isinstance(grado_entry, dict):
                continue
            grado = grado_entry.get("grado") if isinstance(grado_entry.get("grado"), dict) else {}
            grado_id = _safe_int(grado.get("gradoId"))
            grado_nombre = str(grado.get("grado") or "").strip()
            is_contratado = _is_true(grado_entry.get("isContratado"))
            grupos = grado_entry.get("grupos") or []
            if not isinstance(grupos, list):
                continue
            for grupo_entry in grupos:
                if not isinstance(grupo_entry, dict):
                    continue
                grupo = grupo_entry.get("grupo") if isinstance(grupo_entry.get("grupo"), dict) else {}
                grupo_id = _safe_int(grupo.get("grupoId"))
                seccion = str(grupo.get("grupoClave") or grupo.get("grupo") or "").strip()
                cgg = (
                    grupo_entry.get("colegioGradoGrupo")
                    if isinstance(grupo_entry.get("colegioGradoGrupo"), dict)
                    else {}
                )
                colegio_grado_grupo_id = _safe_int(cgg.get("colegioGradoGrupoId"))
                totales = grupo_entry.get("totales") if isinstance(grupo_entry.get("totales"), dict) else {}
                alumnos = _safe_int(totales.get("alumnos")) or 0
                alumnos_inactivos = _safe_int(totales.get("alumnosInactivos")) or 0
                alumnos_contratados = _safe_int(totales.get("alumnosContratados")) or 0
                print(
                    f"{nivel_id if nivel_id is not None else ''}\t"
                    f"{nivel_nombre}\t"
                    f"{grado_id if grado_id is not None else ''}\t"
                    f"{grado_nombre}\t"
                    f"{grupo_id if grupo_id is not None else ''}\t"
                    f"{seccion}\t"
                    f"{colegio_grado_grupo_id if colegio_grado_grupo_id is not None else ''}\t"
                    f"{alumnos}\t"
                    f"{alumnos_inactivos}\t"
                    f"{alumnos_contratados}\t"
                    f"{'SI' if is_contratado else 'NO'}"
                )
                rows_count += 1
    print(f"Total secciones/grupos: {rows_count}")


def _set_alumno_activo(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    activo: int,
    observaciones: str,
    timeout: int,
) -> Tuple[bool, str]:
    url = ALUMNO_ACTIVAR_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
        alumno_id=int(alumno_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"activo": int(activo), "razonInactivoId": 0, "observaciones": str(observaciones or "")}
    method_calls = [
        ("PUT", requests.put),
        ("POST", requests.post),
        ("PATCH", requests.patch),
    ]
    last_error = "HTTP 405"

    for method_name, method_fn in method_calls:
        try:
            response = method_fn(url, headers=headers, json=payload, timeout=int(timeout))
        except requests.RequestException as exc:
            last_error = f"{method_name} error de red: {exc}"
            continue

        status_code = response.status_code
        if status_code == 405:
            last_error = f"{method_name} HTTP 405"
            continue

        try:
            body = response.json()
        except ValueError:
            if response.ok:
                return True, method_name
            return False, f"{method_name} respuesta no JSON (status {status_code})"

        if not response.ok:
            msg = body.get("message") if isinstance(body, dict) else ""
            return False, msg or f"{method_name} HTTP {status_code}"

        if isinstance(body, dict) and body.get("success", True) is False:
            msg = body.get("message") if isinstance(body, dict) else ""
            return False, msg or f"{method_name} respuesta invalida"
        return True, method_name

    return False, last_error


def _activar_alumno(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    timeout: int,
) -> Tuple[bool, str]:
    return _set_alumno_activo(
        token=token,
        colegio_id=colegio_id,
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        nivel_id=nivel_id,
        grado_id=grado_id,
        grupo_id=grupo_id,
        alumno_id=alumno_id,
        activo=1,
        observaciones="",
        timeout=timeout,
    )


def _inactivar_alumno(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    timeout: int,
) -> Tuple[bool, str]:
    return _set_alumno_activo(
        token=token,
        colegio_id=colegio_id,
        empresa_id=empresa_id,
        ciclo_id=ciclo_id,
        nivel_id=nivel_id,
        grado_id=grado_id,
        grupo_id=grupo_id,
        alumno_id=alumno_id,
        activo=0,
        observaciones="Inactivado automatico por posible duplicado no pagado (idOficial+apellidos).",
        timeout=timeout,
    )


def _resolver_grupo_destino_automatico(
    niveles: List[Dict[str, object]],
    nivel_id: int,
    grado_id: int,
    grupo_origen_id: int,
) -> Optional[Tuple[int, str]]:
    candidatos: List[Tuple[str, int]] = []
    for nivel_entry in niveles:
        if not isinstance(nivel_entry, dict):
            continue
        nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id_tmp = _safe_int(nivel.get("nivelId"))
        if nivel_id_tmp != int(nivel_id):
            continue
        grados = nivel_entry.get("grados") or []
        if not isinstance(grados, list):
            continue
        for grado_entry in grados:
            if not isinstance(grado_entry, dict):
                continue
            grado = grado_entry.get("grado") if isinstance(grado_entry.get("grado"), dict) else {}
            grado_id_tmp = _safe_int(grado.get("gradoId"))
            if grado_id_tmp != int(grado_id):
                continue
            grupos = grado_entry.get("grupos") or []
            if not isinstance(grupos, list):
                continue
            for grupo_entry in grupos:
                if not isinstance(grupo_entry, dict):
                    continue
                grupo = grupo_entry.get("grupo") if isinstance(grupo_entry.get("grupo"), dict) else {}
                grupo_id = _safe_int(grupo.get("grupoId"))
                if grupo_id is None or int(grupo_id) == int(grupo_origen_id):
                    continue
                clave = str(grupo.get("grupoClave") or "").strip().upper()
                candidatos.append((clave, int(grupo_id)))
    if not candidatos:
        return None
    candidatos.sort(key=lambda item: (item[0] or "ZZZ", item[1]))
    return candidatos[0][1], candidatos[0][0]


def _build_contexts_for_grade_all_sections(
    niveles: List[Dict[str, object]], nivel_id: int, grado_id: int
) -> List[Dict[str, object]]:
    contexts: List[Dict[str, object]] = []
    seen: Set[Tuple[int, int, int]] = set()
    for nivel_entry in niveles:
        if not isinstance(nivel_entry, dict):
            continue
        nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id_tmp = _safe_int(nivel.get("nivelId"))
        if nivel_id_tmp != int(nivel_id):
            continue
        nivel_nombre = str(nivel.get("nivel") or "").strip()
        grados = nivel_entry.get("grados") or []
        if not isinstance(grados, list):
            continue
        for grado_entry in grados:
            if not isinstance(grado_entry, dict):
                continue
            grado = grado_entry.get("grado") if isinstance(grado_entry.get("grado"), dict) else {}
            grado_id_tmp = _safe_int(grado.get("gradoId"))
            if grado_id_tmp != int(grado_id):
                continue
            grado_nombre = str(grado.get("grado") or "").strip()
            grupos = grado_entry.get("grupos") or []
            if not isinstance(grupos, list):
                continue
            for grupo_entry in grupos:
                if not isinstance(grupo_entry, dict):
                    continue
                grupo = grupo_entry.get("grupo") if isinstance(grupo_entry.get("grupo"), dict) else {}
                grupo_id = _safe_int(grupo.get("grupoId"))
                if grupo_id is None:
                    continue
                key = (int(nivel_id), int(grado_id), int(grupo_id))
                if key in seen:
                    continue
                seen.add(key)
                contexts.append(
                    {
                        "nivel_id": int(nivel_id),
                        "grado_id": int(grado_id),
                        "grupo_id": int(grupo_id),
                        "nivel": nivel_nombre,
                        "grado": grado_nombre,
                        "seccion": str(grupo.get("grupoClave") or grupo.get("grupo") or "").strip(),
                    }
                )
    contexts.sort(key=lambda c: (str(c.get("seccion") or ""), int(c.get("grupo_id") or 0)))
    return contexts


def _build_grupo_id_by_seccion(
    niveles: List[Dict[str, object]], nivel_id: int, grado_id: int
) -> Dict[str, int]:
    mapping: Dict[str, int] = {}
    for nivel_entry in niveles:
        if not isinstance(nivel_entry, dict):
            continue
        nivel = nivel_entry.get("nivel") if isinstance(nivel_entry.get("nivel"), dict) else {}
        nivel_id_tmp = _safe_int(nivel.get("nivelId"))
        if nivel_id_tmp != int(nivel_id):
            continue
        grados = nivel_entry.get("grados") or []
        if not isinstance(grados, list):
            continue
        for grado_entry in grados:
            if not isinstance(grado_entry, dict):
                continue
            grado = grado_entry.get("grado") if isinstance(grado_entry.get("grado"), dict) else {}
            grado_id_tmp = _safe_int(grado.get("gradoId"))
            if grado_id_tmp != int(grado_id):
                continue
            grupos = grado_entry.get("grupos") or []
            if not isinstance(grupos, list):
                continue
            for grupo_entry in grupos:
                if not isinstance(grupo_entry, dict):
                    continue
                grupo = grupo_entry.get("grupo") if isinstance(grupo_entry.get("grupo"), dict) else {}
                grupo_id = _safe_int(grupo.get("grupoId"))
                seccion = _normalize_text(grupo.get("grupoClave") or grupo.get("grupo") or "")
                if grupo_id is None or not seccion:
                    continue
                if len(seccion) > 1:
                    seccion = seccion[-1]
                mapping[seccion] = int(grupo_id)
    return mapping


def _mover_alumno(
    token: str,
    colegio_id: int,
    empresa_id: int,
    ciclo_id: int,
    nivel_id: int,
    grado_id: int,
    grupo_id: int,
    alumno_id: int,
    nuevo_nivel_id: int,
    nuevo_grado_id: int,
    nuevo_grupo_id: int,
    timeout: int,
) -> Tuple[bool, str]:
    url = ALUMNO_MOVER_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        colegio_id=int(colegio_id),
        nivel_id=int(nivel_id),
        grado_id=int(grado_id),
        grupo_id=int(grupo_id),
        alumno_id=int(alumno_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "nuevoNivelId": int(nuevo_nivel_id),
        "nuevoGradoId": int(nuevo_grado_id),
        "nuevoGrupoId": int(nuevo_grupo_id),
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        if response.ok:
            return True, ""
        return False, f"Respuesta no JSON (status {status_code})"

    if not response.ok:
        msg = body.get("message") if isinstance(body, dict) else ""
        return False, msg or f"HTTP {status_code}"

    if isinstance(body, dict) and body.get("success", True) is False:
        msg = body.get("message") if isinstance(body, dict) else ""
        return False, msg or "Respuesta invalida"
    return True, ""


def _asignar_alumno_a_clase(
    token: str,
    empresa_id: int,
    ciclo_id: int,
    clase_id: int,
    alumno_id: int,
    timeout: int,
) -> Tuple[bool, str]:
    url = CLASE_ALUMNOS_URL.format(
        empresa_id=int(empresa_id),
        ciclo_id=int(ciclo_id),
        clase_id=int(clase_id),
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"alumnoId": int(alumno_id)}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=int(timeout))
    except requests.RequestException as exc:
        return False, f"Error de red: {exc}"

    status_code = response.status_code
    try:
        body = response.json()
    except ValueError:
        if response.ok:
            return True, ""
        return False, f"Respuesta no JSON (status {status_code})"

    if not response.ok:
        msg = body.get("message") if isinstance(body, dict) else ""
        return False, msg or f"HTTP {status_code}"

    if isinstance(body, dict) and body.get("success", True) is False:
        msg = body.get("message") if isinstance(body, dict) else ""
        return False, msg or "Respuesta invalida"
    return True, ""


def _flatten_usuario(item: Dict[str, object], fallback: Dict[str, object]) -> Dict[str, object]:
    persona = item.get("persona") if isinstance(item.get("persona"), dict) else {}
    nivel = item.get("nivel") if isinstance(item.get("nivel"), dict) else {}
    grado = item.get("grado") if isinstance(item.get("grado"), dict) else {}
    grupo = item.get("grupo") if isinstance(item.get("grupo"), dict) else {}
    return {
        "persona_id": _safe_int(persona.get("personaId")),
        "alumno_id": _safe_int(item.get("alumnoId")),
        "nombre_completo": str(persona.get("nombreCompleto") or "").strip(),
        "nivel": str(nivel.get("nivel") or fallback.get("nivel") or "").strip(),
        "grado": str(grado.get("grado") or fallback.get("grado") or "").strip(),
        "seccion": str(grupo.get("grupoClave") or fallback.get("seccion") or "").strip(),
        "grupo_id": _safe_int(grupo.get("grupoId")) or _safe_int(fallback.get("grupo_id")),
        "activo": _is_true(item.get("activo", False)),
        "con_pago": _is_true(item.get("conPago", False)),
        "nivel_id": _safe_int(nivel.get("nivelId")) or _safe_int(fallback.get("nivel_id")),
        "grado_id": _safe_int(grado.get("gradoId")) or _safe_int(fallback.get("grado_id")),
        "apellido_paterno": str(persona.get("apellidoPaterno") or "").strip(),
        "apellido_materno": str(persona.get("apellidoMaterno") or "").strip(),
        "id_oficial": str(persona.get("idOficial") or "").strip(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Lista clases Pegasus por colegio y ademas lista usuarios de una seccion "
            "(por defecto Y / grupoId 685). En simulacion por defecto, solo muestra "
            "activaciones/inactivaciones/movimientos propuestos."
        )
    )
    parser.add_argument("colegio_id", type=int, help="Colegio clave ID (ej: 9039)")
    parser.add_argument("bearer_token", help="Bearer token Pegasus")
    parser.add_argument("--empresa-id", type=int, default=11, help="Empresa ID")
    parser.add_argument("--ciclo-id", type=int, default=207, help="Ciclo ID")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout en segundos")
    parser.add_argument(
        "--listar-secciones",
        action="store_true",
        help="Lista secciones/grupos disponibles del colegio (con IDs)",
    )
    parser.add_argument(
        "--solo-secciones",
        action="store_true",
        help="Solo lista secciones/grupos y termina",
    )
    parser.add_argument(
        "--seccion",
        default="Y",
        help="Seccion objetivo para listar alumnos (default: Y)",
    )
    parser.add_argument(
        "--grupo-id",
        type=int,
        default=685,
        help="GrupoId de la seccion objetivo (default: 685)",
    )
    parser.add_argument(
        "--nivel-id",
        type=int,
        default=None,
        help="Opcional: fuerza un nivelId especifico para consultar alumnos",
    )
    parser.add_argument(
        "--grado-id",
        type=int,
        default=None,
        help="Opcional: fuerza un gradoId especifico para consultar alumnos",
    )
    parser.add_argument(
        "--listar-alumnos-grado-todas-secciones",
        action="store_true",
        help="Lista alumnos de un grado en todas sus secciones (requiere --nivel-id y --grado-id)",
    )
    parser.add_argument(
        "--mover-ahora",
        action="store_true",
        help="Ejecuta el POST /mover. Sin este flag, solo muestra a quien moveria.",
    )
    parser.add_argument(
        "--aplicar-cambios",
        action="store_true",
        help="Aplica activaciones/inactivaciones reales. Sin este flag, es simulacion.",
    )
    args = parser.parse_args(argv)

    token = _clean_token(args.bearer_token)
    if not token:
        print("Error: bearer token vacio.", file=sys.stderr)
        return 2

    niveles: List[Dict[str, object]] = []
    try:
        niveles = _fetch_niveles_grados_grupos_censo(
            token=token,
            colegio_id=int(args.colegio_id),
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            timeout=int(args.timeout),
        )
        _print_secciones_disponibles(niveles)
    except Exception as exc:
        print(f"Error listando secciones: {exc}", file=sys.stderr)
        if args.solo_secciones:
            return 1
    if args.solo_secciones:
        return 0

    if args.listar_alumnos_grado_todas_secciones:
        if args.nivel_id is None or args.grado_id is None:
            print(
                "Error: --listar-alumnos-grado-todas-secciones requiere --nivel-id y --grado-id.",
                file=sys.stderr,
            )
            return 2
        contexts = _build_contexts_for_grade_all_sections(
            niveles=niveles,
            nivel_id=int(args.nivel_id),
            grado_id=int(args.grado_id),
        )
        if not contexts:
            print(
                f"Sin secciones configuradas para nivelId={args.nivel_id}, gradoId={args.grado_id}.",
                file=sys.stderr,
            )
            return 1

        usuarios_rows: List[Dict[str, object]] = []
        for context in contexts:
            try:
                data = _fetch_alumnos_censo(
                    token=token,
                    colegio_id=int(args.colegio_id),
                    empresa_id=int(args.empresa_id),
                    ciclo_id=int(args.ciclo_id),
                    nivel_id=int(context["nivel_id"]),
                    grado_id=int(context["grado_id"]),
                    grupo_id=int(context["grupo_id"]),
                    timeout=int(args.timeout),
                )
            except Exception as exc:
                print(
                    "Error alumnos "
                    f"(nivelId={context['nivel_id']}, gradoId={context['grado_id']}, grupoId={context['grupo_id']}): {exc}",
                    file=sys.stderr,
                )
                continue
            for item in data:
                if not isinstance(item, dict):
                    continue
                usuarios_rows.append(_flatten_usuario(item, fallback=context))

        dedup: List[Dict[str, object]] = []
        seen_users: Set[Tuple[Optional[int], Optional[int], int]] = set()
        for row in usuarios_rows:
            persona_id = _safe_int(row.get("persona_id"))
            alumno_id = _safe_int(row.get("alumno_id"))
            grupo_id = _safe_int(row.get("grupo_id")) or 0
            key = (persona_id, alumno_id, int(grupo_id))
            if key in seen_users:
                continue
            seen_users.add(key)
            dedup.append(row)

        print("")
        print(
            f"ALUMNOS DEL GRADO nivelId={args.nivel_id}, gradoId={args.grado_id} (TODAS LAS SECCIONES)"
        )
        print("personaId\talumnoId\tNombre\tNivel\tGrado\tSeccion\tGrupoId\tActivo\tConPago")
        for row in sorted(
            dedup,
            key=lambda it: (
                str(it.get("seccion") or ""),
                str(it.get("nombre_completo") or ""),
            ),
        ):
            print(
                f"{row.get('persona_id', '')}\t"
                f"{row.get('alumno_id', '')}\t"
                f"{row.get('nombre_completo', '')}\t"
                f"{row.get('nivel', '')}\t"
                f"{row.get('grado', '')}\t"
                f"{row.get('seccion', '')}\t"
                f"{row.get('grupo_id', '')}\t"
                f"{'SI' if row.get('activo') else 'NO'}\t"
                f"{'SI' if row.get('con_pago') else 'NO'}"
            )
        print(f"Total alumnos encontrados: {len(dedup)}")
        return 0

    rows: List[Dict[str, object]] = []
    try:
        rows, _ = listar_y_mapear_clases(
            token=token,
            colegio_id=int(args.colegio_id),
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            timeout=int(args.timeout),
            ordered=True,
        )
    except Exception as exc:
        print(f"Advertencia: no se pudieron listar clases: {exc}", file=sys.stderr)

    print("geClaseClave\tNivel\tGrado\tSeccion\tGradoId\tGrupoId")
    for row in rows:
        print(
            f"{row.get('clase', '')}\t"
            f"{row.get('nivel', '')}\t"
            f"{row.get('grado', '')}\t"
            f"{row.get('seccion', '')}\t"
            f"{row.get('grado_id', '')}\t"
            f"{row.get('grupo_id', '')}"
        )
    if not rows:
        print("(sin clases o no disponibles)")

    all_dedup: List[Dict[str, object]] = []
    contexts_all_sections = _build_contexts_for_grade_all_sections(
        niveles=niveles,
        nivel_id=MOVE_ONLY_NIVEL_ID,
        grado_id=MOVE_ONLY_GRADO_ID,
    )
    if contexts_all_sections:
        alumnos_all_rows: List[Dict[str, object]] = []
        for context in contexts_all_sections:
            try:
                data = _fetch_alumnos_censo(
                    token=token,
                    colegio_id=int(args.colegio_id),
                    empresa_id=int(args.empresa_id),
                    ciclo_id=int(args.ciclo_id),
                    nivel_id=int(context["nivel_id"]),
                    grado_id=int(context["grado_id"]),
                    grupo_id=int(context["grupo_id"]),
                    timeout=int(args.timeout),
                )
            except Exception as exc:
                print(
                    "Error alumnos grado todas secciones "
                    f"(nivelId={context['nivel_id']}, gradoId={context['grado_id']}, grupoId={context['grupo_id']}): {exc}",
                    file=sys.stderr,
                )
                continue
            for item in data:
                if not isinstance(item, dict):
                    continue
                alumnos_all_rows.append(_flatten_usuario(item, fallback=context))

        all_dedup = []
        seen_all: Set[Tuple[Optional[int], Optional[int], int]] = set()
        for row in alumnos_all_rows:
            persona_id = _safe_int(row.get("persona_id"))
            alumno_id = _safe_int(row.get("alumno_id"))
            grupo_id = _safe_int(row.get("grupo_id")) or 0
            key = (persona_id, alumno_id, int(grupo_id))
            if key in seen_all:
                continue
            seen_all.add(key)
            all_dedup.append(row)

        print("")
        print("ALUMNOS 5TO SECUNDARIA (TODAS LAS SECCIONES)")
        print("personaId\talumnoId\tNombre\tNivel\tGrado\tSeccion\tGrupoId\tActivo\tConPago")
        for row in sorted(
            all_dedup,
            key=lambda it: (
                str(it.get("seccion") or ""),
                str(it.get("nombre_completo") or ""),
            ),
        ):
            print(
                f"{row.get('persona_id', '')}\t"
                f"{row.get('alumno_id', '')}\t"
                f"{row.get('nombre_completo', '')}\t"
                f"{row.get('nivel', '')}\t"
                f"{row.get('grado', '')}\t"
                f"{row.get('seccion', '')}\t"
                f"{row.get('grupo_id', '')}\t"
                f"{'SI' if row.get('activo') else 'NO'}\t"
                f"{'SI' if row.get('con_pago') else 'NO'}"
            )
        print(f"Total alumnos 5to secundaria (todas secciones): {len(all_dedup)}")
    else:
        print("")
        print("ALUMNOS 5TO SECUNDARIA (TODAS LAS SECCIONES): sin secciones configuradas.")

    print("")
    print("COMPARACION DUPLICADOS (1) apellidos, luego (2) DNI, EN 5TO SEC")
    by_apellidos: Dict[Tuple[str, str], List[Dict[str, object]]] = {}
    for row in all_dedup:
        ape_pat = _normalize_text(row.get("apellido_paterno"))
        ape_mat = _normalize_text(row.get("apellido_materno"))
        if not ape_pat or not ape_mat:
            continue
        by_apellidos.setdefault((ape_pat, ape_mat), []).append(row)

    candidatos_inactivar: List[Dict[str, object]] = []
    referencia_pagado_por_alumno: Dict[int, Dict[str, object]] = {}
    plan_mover_por_pagado: Dict[int, Dict[str, object]] = {}
    grupo_destino_por_seccion = _build_grupo_id_by_seccion(
        niveles=niveles,
        nivel_id=MOVE_ONLY_NIVEL_ID,
        grado_id=MOVE_ONLY_GRADO_ID,
    )
    for apellidos_key, rows_mismo_apellido in by_apellidos.items():
        by_dni: Dict[str, List[Dict[str, object]]] = {}
        for row in rows_mismo_apellido:
            dni = _normalize_id_oficial(row.get("id_oficial"))
            if not dni:
                continue
            by_dni.setdefault(dni, []).append(row)

        for dni_key, rows_mismo_dni in by_dni.items():
            pagados = [row for row in rows_mismo_dni if _is_true(row.get("con_pago"))]
            no_pagados = [row for row in rows_mismo_dni if not _is_true(row.get("con_pago"))]
            if not pagados or not no_pagados:
                continue
            pagado_ref = pagados[0]
            for row in no_pagados:
                candidatos_inactivar.append(row)
                alumno_id_tmp = _safe_int(row.get("alumno_id"))
                if alumno_id_tmp is not None:
                    referencia_pagado_por_alumno[int(alumno_id_tmp)] = pagado_ref
                destino_seccion = _normalize_text(row.get("seccion"))
                if len(destino_seccion) > 1:
                    destino_seccion = destino_seccion[-1]
                if not destino_seccion:
                    destino_seccion = "A"
                destino_grupo_id = _safe_int(grupo_destino_por_seccion.get(destino_seccion))
                if destino_grupo_id is None:
                    destino_seccion = "A"
                    destino_grupo_id = _safe_int(grupo_destino_por_seccion.get("A"))
                pagado_alumno_id = _safe_int(pagado_ref.get("alumno_id"))
                if pagado_alumno_id is not None and destino_grupo_id is not None:
                    plan_mover_por_pagado.setdefault(
                        int(pagado_alumno_id),
                        {
                            "pagado": pagado_ref,
                            "destino_seccion": destino_seccion,
                            "destino_grupo_id": int(destino_grupo_id),
                            "referencia_no_pagado": row,
                        },
                    )
                print(
                    "ALUMNO PARECIDO (pago): "
                    f"{pagado_ref.get('nombre_completo', '')} "
                    f"[personaId={pagado_ref.get('persona_id', '')}, alumnoId={pagado_ref.get('alumno_id', '')}, "
                    f"seccion={pagado_ref.get('seccion', '')}] | "
                    "ALUMNO A INACTIVAR (no pago): "
                    f"{row.get('nombre_completo', '')} "
                    f"[personaId={row.get('persona_id', '')}, alumnoId={row.get('alumno_id', '')}, "
                    f"seccion={row.get('seccion', '')}] | "
                    "ALUMNO A MOVER: "
                    f"alumnoId={pagado_ref.get('alumno_id', '')} -> seccion {destino_seccion} "
                    f"(grupoId={destino_grupo_id if destino_grupo_id is not None else ''}) "
                    f"(dni={dni_key}, apellidos={apellidos_key[0]} {apellidos_key[1]})"
                )

    if not candidatos_inactivar:
        print("Sin casos para inactivar por esta regla.")
        print(
            "Accion siguiente: solo movimiento de seccion para alumnos elegibles "
            "(5to secundaria, seccion Y, conPago=true)."
        )
    else:
        print("Inactivando no pagados de casos duplicados...")
        seen_inactivate_ids: Set[int] = set()
        inact_ok = 0
        inact_err = 0
        inact_sim = 0
        for row in candidatos_inactivar:
            alumno_id = _safe_int(row.get("alumno_id"))
            nivel_id = _safe_int(row.get("nivel_id"))
            grado_id = _safe_int(row.get("grado_id"))
            grupo_id = _safe_int(row.get("grupo_id"))
            if (
                alumno_id is None
                or nivel_id is None
                or grado_id is None
                or grupo_id is None
                or int(alumno_id) in seen_inactivate_ids
            ):
                continue
            seen_inactivate_ids.add(int(alumno_id))
            if not _is_true(row.get("activo")):
                print(
                    f"SKIP ya inactivo alumnoId={alumno_id} personaId={row.get('persona_id', '')}"
                )
                continue
            if not args.aplicar_cambios:
                inact_sim += 1
                pagado_ref = referencia_pagado_por_alumno.get(int(alumno_id)) if alumno_id is not None else None
                pagado_txt = (
                    f" | referente pagado alumnoId={pagado_ref.get('alumno_id', '')} "
                    f"personaId={pagado_ref.get('persona_id', '')}"
                    if isinstance(pagado_ref, dict)
                    else ""
                )
                print(
                    "SIMULACION inactivar "
                    f"alumnoId={alumno_id} personaId={row.get('persona_id', '')} "
                    f"idOficial={row.get('id_oficial', '')}{pagado_txt}"
                )
            else:
                ok, err = _inactivar_alumno(
                    token=token,
                    colegio_id=int(args.colegio_id),
                    empresa_id=int(args.empresa_id),
                    ciclo_id=int(args.ciclo_id),
                    nivel_id=int(nivel_id),
                    grado_id=int(grado_id),
                    grupo_id=int(grupo_id),
                    alumno_id=int(alumno_id),
                    timeout=int(args.timeout),
                )
                if ok:
                    inact_ok += 1
                    print(
                        "OK inactivar "
                        f"alumnoId={alumno_id} personaId={row.get('persona_id', '')} "
                        f"idOficial={row.get('id_oficial', '')}"
                    )
                else:
                    inact_err += 1
                    print(
                        "ERROR inactivar "
                        f"alumnoId={alumno_id} personaId={row.get('persona_id', '')}: {err}",
                        file=sys.stderr,
                    )
        if not args.aplicar_cambios:
            print(f"Resultado inactivacion por duplicado (simulacion): SE HARIA={inact_sim}")
        else:
            print(f"Resultado inactivacion por duplicado: OK={inact_ok} ERROR={inact_err}")

    seccion_objetivo = str(args.seccion or "").strip().upper()
    grupo_objetivo = int(args.grupo_id)

    target_contexts: List[Dict[str, object]] = []
    seen_contexts: Set[Tuple[int, int, int]] = set()
    if (args.nivel_id is None) != (args.grado_id is None):
        print("Error: usa --nivel-id y --grado-id juntos.", file=sys.stderr)
        return 2

    if args.nivel_id is not None and args.grado_id is not None:
        key = (int(args.nivel_id), int(args.grado_id), int(grupo_objetivo))
        seen_contexts.add(key)
        target_contexts.append(
            {
                "nivel_id": int(args.nivel_id),
                "grado_id": int(args.grado_id),
                "grupo_id": int(grupo_objetivo),
                "nivel": "",
                "grado": "",
                "seccion": seccion_objetivo,
            }
        )
    else:
        for row in rows:
            nivel_id = _safe_int(row.get("nivel_id"))
            grado_id = _safe_int(row.get("grado_id"))
            grupo_id = _safe_int(row.get("grupo_id"))
            seccion = str(row.get("seccion") or "").strip().upper()
            if nivel_id is None or grado_id is None:
                continue
            # Prioriza grupoId explicito; si no viene en la clase, usa la letra de seccion.
            if grupo_id is not None:
                if int(grupo_id) != grupo_objetivo:
                    continue
            elif seccion_objetivo and seccion != seccion_objetivo:
                continue
            key = (int(nivel_id), int(grado_id), int(grupo_objetivo))
            if key in seen_contexts:
                continue
            seen_contexts.add(key)
            target_contexts.append(
                {
                    "nivel_id": int(nivel_id),
                    "grado_id": int(grado_id),
                    "grupo_id": int(grupo_objetivo),
                    "nivel": str(row.get("nivel") or ""),
                    "grado": str(row.get("grado") or ""),
                    "seccion": seccion_objetivo or seccion,
                }
            )

    if not target_contexts:
        for nivel_id, grado_ids in GRADOS_POR_NIVEL.items():
            for grado_id in grado_ids:
                key = (int(nivel_id), int(grado_id), int(grupo_objetivo))
                if key in seen_contexts:
                    continue
                seen_contexts.add(key)
                target_contexts.append(
                    {
                        "nivel_id": int(nivel_id),
                        "grado_id": int(grado_id),
                        "grupo_id": int(grupo_objetivo),
                        "nivel": "",
                        "grado": "",
                        "seccion": seccion_objetivo,
                    }
                )

    print("")
    print(f"USUARIOS SECCION {seccion_objetivo or '(sin seccion)'} (grupoId={grupo_objetivo})")

    usuarios_rows: List[Dict[str, object]] = []
    for context in target_contexts:
        try:
            data = _fetch_alumnos_censo(
                token=token,
                colegio_id=int(args.colegio_id),
                empresa_id=int(args.empresa_id),
                ciclo_id=int(args.ciclo_id),
                nivel_id=int(context["nivel_id"]),
                grado_id=int(context["grado_id"]),
                grupo_id=int(context["grupo_id"]),
                timeout=int(args.timeout),
            )
        except Exception as exc:
            print(
                "Error alumnos "
                f"(nivelId={context['nivel_id']}, gradoId={context['grado_id']}, grupoId={context['grupo_id']}): {exc}",
                file=sys.stderr,
            )
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            usuarios_rows.append(_flatten_usuario(item, fallback=context))

    # Deduplicado por persona o alumno para evitar repetidos.
    dedup: List[Dict[str, object]] = []
    seen_users: Set[Tuple[Optional[int], Optional[int], int, str, str]] = set()
    for row in usuarios_rows:
        persona_id = _safe_int(row.get("persona_id"))
        alumno_id = _safe_int(row.get("alumno_id"))
        grupo_id = _safe_int(row.get("grupo_id")) or 0
        nivel = str(row.get("nivel") or "")
        grado = str(row.get("grado") or "")
        key = (persona_id, alumno_id, int(grupo_id), nivel, grado)
        if key in seen_users:
            continue
        seen_users.add(key)
        dedup.append(row)

    con_pago_rows = [row for row in dedup if bool(row.get("con_pago", False))]

    pendientes = [
        row
        for row in con_pago_rows
        if not _is_true(row.get("activo"))
        and _safe_int(row.get("alumno_id")) is not None
        and _safe_int(row.get("nivel_id")) is not None
        and _safe_int(row.get("grado_id")) is not None
        and _safe_int(row.get("grupo_id")) is not None
    ]
    print("")
    print(f"Activando inactivos pagados: {len(pendientes)}")
    ok_count = 0
    err_count = 0
    sim_count = 0
    activados_ok_ids: Set[int] = set()
    for row in pendientes:
        if not args.aplicar_cambios:
            sim_count += 1
            if _safe_int(row.get("alumno_id")) is not None:
                activados_ok_ids.add(int(row["alumno_id"]))
            print(
                "SIMULACION activar "
                f"alumnoId={row['alumno_id']} personaId={row.get('persona_id', '')}"
            )
        else:
            ok, err = _activar_alumno(
                token=token,
                colegio_id=int(args.colegio_id),
                empresa_id=int(args.empresa_id),
                ciclo_id=int(args.ciclo_id),
                nivel_id=int(row["nivel_id"]),
                grado_id=int(row["grado_id"]),
                grupo_id=int(row["grupo_id"]),
                alumno_id=int(row["alumno_id"]),
                timeout=int(args.timeout),
            )
            if ok:
                ok_count += 1
                if _safe_int(row.get("alumno_id")) is not None:
                    activados_ok_ids.add(int(row["alumno_id"]))
                print(
                    f"OK activar alumnoId={row['alumno_id']} personaId={row.get('persona_id', '')} via {err}"
                )
            else:
                err_count += 1
                print(
                    f"ERROR activar alumnoId={row['alumno_id']} personaId={row.get('persona_id', '')}: {err}",
                    file=sys.stderr,
                )
    if not args.aplicar_cambios:
        print(f"Resultado activacion (simulacion): SE HARIA={sim_count}")
    else:
        print(f"Resultado activacion: OK={ok_count} ERROR={err_count}")

    move_plan: List[Dict[str, object]] = []
    for plan in plan_mover_por_pagado.values():
        pagado_row = plan.get("pagado") if isinstance(plan.get("pagado"), dict) else {}
        ref_no_pagado = (
            plan.get("referencia_no_pagado")
            if isinstance(plan.get("referencia_no_pagado"), dict)
            else {}
        )
        alumno_id = _safe_int(pagado_row.get("alumno_id"))
        nivel_id = _safe_int(pagado_row.get("nivel_id"))
        grado_id = _safe_int(pagado_row.get("grado_id"))
        grupo_origen_id = _safe_int(pagado_row.get("grupo_id"))
        grupo_destino_id = _safe_int(plan.get("destino_grupo_id"))
        seccion_destino = str(plan.get("destino_seccion") or "").strip().upper()
        if (
            alumno_id is None
            or nivel_id is None
            or grado_id is None
            or grupo_origen_id is None
            or grupo_destino_id is None
        ):
            continue
        if not _is_true(pagado_row.get("con_pago")):
            continue
        esta_activo = _is_true(pagado_row.get("activo")) or int(alumno_id) in activados_ok_ids
        if not esta_activo:
            continue
        if int(grupo_origen_id) == int(grupo_destino_id):
            continue
        move_plan.append(
            {
                "alumno_mover": pagado_row,
                "alumno_parecido": pagado_row,
                "alumno_inactivar": ref_no_pagado,
                "nivel_id": int(nivel_id),
                "grado_id": int(grado_id),
                "grupo_origen_id": int(grupo_origen_id),
                "grupo_destino_id": int(grupo_destino_id),
                "seccion_destino": seccion_destino or "A",
                "motivo": "Por comparacion con no pagado (idOficial+apellidos).",
            }
        )

    if not move_plan:
        fallback_elegibles: List[Dict[str, object]] = []
        for row in con_pago_rows:
            alumno_id = _safe_int(row.get("alumno_id"))
            nivel_id = _safe_int(row.get("nivel_id"))
            grado_id = _safe_int(row.get("grado_id"))
            grupo_id = _safe_int(row.get("grupo_id"))
            if (
                alumno_id is None
                or nivel_id != MOVE_ONLY_NIVEL_ID
                or grado_id != MOVE_ONLY_GRADO_ID
                or grupo_id != MOVE_ONLY_GRUPO_ID
            ):
                continue
            esta_activo = _is_true(row.get("activo")) or int(alumno_id) in activados_ok_ids
            if not esta_activo:
                continue
            fallback_elegibles.append(row)

        destino = _resolver_grupo_destino_automatico(
            niveles=niveles,
            nivel_id=MOVE_ONLY_NIVEL_ID,
            grado_id=MOVE_ONLY_GRADO_ID,
            grupo_origen_id=MOVE_ONLY_GRUPO_ID,
        )
        if destino is not None:
            nuevo_grupo_id, nuevo_grupo_clave = destino
            for row in fallback_elegibles:
                alumno_id = _safe_int(row.get("alumno_id"))
                grupo_origen_id = _safe_int(row.get("grupo_id"))
                if alumno_id is None or grupo_origen_id is None:
                    continue
                if int(grupo_origen_id) == int(nuevo_grupo_id):
                    continue
                move_plan.append(
                    {
                        "alumno_mover": row,
                        "alumno_parecido": {},
                        "alumno_inactivar": {},
                        "nivel_id": MOVE_ONLY_NIVEL_ID,
                        "grado_id": MOVE_ONLY_GRADO_ID,
                        "grupo_origen_id": int(grupo_origen_id),
                        "grupo_destino_id": int(nuevo_grupo_id),
                        "seccion_destino": str(nuevo_grupo_clave or ""),
                        "motivo": "Fallback automatico desde seccion Y pagados.",
                    }
                )

    print("")
    print("PLAN MOVIMIENTO (ALUMNO PARECIDO | ALUMNO A INACTIVAR | ALUMNO A MOVER)")
    print(
        "AlumnoParecido\tAlumnoAInactivar\tAlumnoAMover\tOrigenGrupo\tDestinoSeccion\tDestinoGrupo\tMotivo"
    )
    for plan in sorted(
        move_plan,
        key=lambda it: str((it.get("alumno_mover") or {}).get("nombre_completo") or ""),
    ):
        parecido = plan.get("alumno_parecido") if isinstance(plan.get("alumno_parecido"), dict) else {}
        inactivar = plan.get("alumno_inactivar") if isinstance(plan.get("alumno_inactivar"), dict) else {}
        mover = plan.get("alumno_mover") if isinstance(plan.get("alumno_mover"), dict) else {}
        parecido_txt = (
            f"{parecido.get('nombre_completo', '')} ({parecido.get('alumno_id', '')})"
            if parecido
            else "-"
        )
        inactivar_txt = (
            f"{inactivar.get('nombre_completo', '')} ({inactivar.get('alumno_id', '')})"
            if inactivar
            else "-"
        )
        mover_txt = f"{mover.get('nombre_completo', '')} ({mover.get('alumno_id', '')})"
        print(
            f"{parecido_txt}\t"
            f"{inactivar_txt}\t"
            f"{mover_txt}\t"
            f"{plan.get('grupo_origen_id', '')}\t"
            f"{plan.get('seccion_destino', '')}\t"
            f"{plan.get('grupo_destino_id', '')}\t"
            f"{plan.get('motivo', '')}"
        )
    print(f"Total alumnos a mover: {len(move_plan)}")

    asignacion_plan: List[Dict[str, object]] = []
    for plan in move_plan:
        mover = plan.get("alumno_mover") if isinstance(plan.get("alumno_mover"), dict) else {}
        alumno_id = _safe_int(mover.get("alumno_id"))
        nivel_id = _safe_int(plan.get("nivel_id"))
        grado_id = _safe_int(plan.get("grado_id"))
        grupo_destino_id = _safe_int(plan.get("grupo_destino_id"))
        seccion_destino = _normalize_text(plan.get("seccion_destino"))
        if len(seccion_destino) > 1:
            seccion_destino = seccion_destino[-1]
        if (
            alumno_id is None
            or nivel_id is None
            or grado_id is None
            or grupo_destino_id is None
        ):
            continue
        clases_destino = []
        for clase in rows:
            clase_nivel = _safe_int(clase.get("nivel_id"))
            clase_grado = _safe_int(clase.get("grado_id"))
            clase_grupo = _safe_int(clase.get("grupo_id"))
            clase_seccion = _normalize_text(clase.get("seccion"))
            if len(clase_seccion) > 1:
                clase_seccion = clase_seccion[-1]
            if clase_nivel != int(nivel_id) or clase_grado != int(grado_id):
                continue
            if clase_grupo is not None:
                if int(clase_grupo) != int(grupo_destino_id):
                    continue
            elif seccion_destino and clase_seccion != seccion_destino:
                continue
            clase_id = _safe_int(clase.get("clase_id"))
            if clase_id is None:
                continue
            clases_destino.append({"clase_id": int(clase_id), "clase": str(clase.get("clase") or "")})
        clases_destino.sort(key=lambda c: (str(c.get("clase") or ""), int(c.get("clase_id") or 0)))
        asignacion_plan.append(
            {
                "alumno_id": int(alumno_id),
                "persona_id": mover.get("persona_id"),
                "nombre": mover.get("nombre_completo"),
                "nivel_id": int(nivel_id),
                "grado_id": int(grado_id),
                "grupo_destino_id": int(grupo_destino_id),
                "seccion_destino": seccion_destino or plan.get("seccion_destino"),
                "clases": clases_destino,
            }
        )

    print("")
    print("PLAN ASIGNACION A CLASES (POST /clases/{clase_id}/alumnos)")
    for item in asignacion_plan:
        print(
            f"alumnoId={item.get('alumno_id')} personaId={item.get('persona_id', '')} "
            f"nombre={item.get('nombre', '')} -> seccion {item.get('seccion_destino', '')} "
            f"(grupoId={item.get('grupo_destino_id', '')}) clases={len(item.get('clases', []))}"
        )
        for clase in item.get("clases", []):
            print(f"  claseId={clase.get('clase_id', '')}\t{clase.get('clase', '')}")

    if not (args.aplicar_cambios and args.mover_ahora):
        print("SIMULACION: no se ejecuto movimiento.")
        print("Usa --aplicar-cambios --mover-ahora para ejecutar el POST /mover.")
        return 0

    move_ok = 0
    move_err = 0
    if not move_plan:
        print("Sin alumnos para mover.")
        return 0
    moved_success_ids: Set[int] = set()
    vistos_mover: Set[int] = set()
    for plan in move_plan:
        row = plan.get("alumno_mover") if isinstance(plan.get("alumno_mover"), dict) else {}
        alumno_id = _safe_int(row.get("alumno_id"))
        nivel_id = _safe_int(plan.get("nivel_id"))
        grado_id = _safe_int(plan.get("grado_id"))
        grupo_origen_id = _safe_int(plan.get("grupo_origen_id"))
        grupo_destino_id = _safe_int(plan.get("grupo_destino_id"))
        if alumno_id is None or int(alumno_id) in vistos_mover:
            continue
        if (
            nivel_id is None
            or grado_id is None
            or grupo_origen_id is None
            or grupo_destino_id is None
        ):
            continue
        vistos_mover.add(int(alumno_id))
        ok, err = _mover_alumno(
            token=token,
            colegio_id=int(args.colegio_id),
            empresa_id=int(args.empresa_id),
            ciclo_id=int(args.ciclo_id),
            nivel_id=int(nivel_id),
            grado_id=int(grado_id),
            grupo_id=int(grupo_origen_id),
            alumno_id=int(alumno_id),
            nuevo_nivel_id=int(nivel_id),
            nuevo_grado_id=int(grado_id),
            nuevo_grupo_id=int(grupo_destino_id),
            timeout=int(args.timeout),
        )
        if ok:
            move_ok += 1
            moved_success_ids.add(int(alumno_id))
            print(f"OK mover alumnoId={alumno_id} -> grupoId={grupo_destino_id}")
        else:
            move_err += 1
            print(
                f"ERROR mover alumnoId={alumno_id} -> grupoId={grupo_destino_id}: {err}",
                file=sys.stderr,
            )
    print(f"Resultado mover: OK={move_ok} ERROR={move_err}")

    print("")
    print("ASIGNANDO A CLASES DESTINO...")
    assign_ok = 0
    assign_err = 0
    assign_skip = 0
    for item in asignacion_plan:
        alumno_id = _safe_int(item.get("alumno_id"))
        if alumno_id is None or int(alumno_id) not in moved_success_ids:
            assign_skip += len(item.get("clases", []))
            continue
        for clase in item.get("clases", []):
            clase_id = _safe_int(clase.get("clase_id"))
            if clase_id is None:
                continue
            ok, err = _asignar_alumno_a_clase(
                token=token,
                empresa_id=int(args.empresa_id),
                ciclo_id=int(args.ciclo_id),
                clase_id=int(clase_id),
                alumno_id=int(alumno_id),
                timeout=int(args.timeout),
            )
            if ok:
                assign_ok += 1
                print(f"OK asignar alumnoId={alumno_id} -> claseId={clase_id}")
            else:
                assign_err += 1
                print(
                    f"ERROR asignar alumnoId={alumno_id} -> claseId={clase_id}: {err}",
                    file=sys.stderr,
                )
    print(f"Resultado asignacion clases: OK={assign_ok} ERROR={assign_err} SKIP={assign_skip}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
