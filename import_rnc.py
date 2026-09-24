#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
import_rnc.py
=============

Descarga el archivo oficial de contribuyentes DGII República Dominicana
(RNC_CONTRIBUYENTES.zip), lo descomprime, lo carga a una tabla de staging
en PostgreSQL y hace upsert masivo en la tabla `res_partner` del modelo
Odoo `res.partner`.

Uso:
    ./import_rnc.sh
o
    python3 import_rnc.py [--skip-download] [--dry-run]

Variables de entorno:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    DATA_DIR (default: /opt/containers_files/odooapi/extra-addons/gs_master_data/data)
    RNC_URL  (default: https://dgii.gov.do/app/WebApps/Consultas/RNC/RNC_CONTRIBUYENTES.zip)

Notas de mapeo a res.partner:
    DGII razon_social        -> res_partner.name
    DGII rnc                 -> res_partner.vat
    DGII commercial_name (*) -> res_partner.company_name
    (importación)            -> res_partner.is_company = TRUE
    (importación)            -> res_partner.active = TRUE  (no se cargan archivados)
    (importación)            -> res_partner.{DGII_LOADED_FIELD} = TRUE  ("Cargado desde DGII")
    DGII actividad_economica      ─┐
    DGII fecha_inicio_operaciones ─┴──> res_partner.comment (Notas internas),
    DGII estado                       en bloque "[DGII]" con formato
    DGII regimen_pago                 "Etiqueta: Valor" (una línea por campo)

(*) `commercial_name` no viene en el CSV DGII, pero la columna se conserva
    en la tabla de staging para posibles extensiones futuras.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterable, Iterator, Optional

import psycopg2
import psycopg2.extras
import requests

# -----------------------------------------------------------------------------
# Configuración
# -----------------------------------------------------------------------------

DEFAULT_DATA_DIR = Path(
    "/opt/containers_files/odooapi/extra-addons/gs_master_data/data"
)
DEFAULT_URL = (
    "https://dgii.gov.do/app/WebApps/Consultas/RNC/RNC_CONTRIBUYENTES.zip"
)
ZIP_NAME = "RNC_CONTRIBUYENTES.zip"
STAGING_TABLE = "res_partner_rnc_staging"
TARGET_TABLE = "res_partner"
# Nombre técnico de la columna booleana en res_partner que marca los
# partners cargados por este script (etiqueta en Odoo: "Cargado desde DGII").
# En Odoo, los campos personalizados usan típicamente el prefijo `x_`.
DGII_LOADED_FIELD = "x_cargado_desde_dgii"

# Columnas DGII adicionales que se persisten en `res_partner.comment`
# (Notas internas) en formato "Etiqueta: Valor". Cada tupla es
# (clave_csv, etiqueta_legible). El orden aquí define el orden en el bloque.
DGII_COMMENT_FIELDS = (
    ("actividad_economica",      "Actividad Económica"),
    ("fecha_inicio_operaciones", "Fecha Inicio Operaciones"),
    ("estado",                   "Estado"),
    ("regimen_pago",             "Régimen de Pago"),
)
# Marcador del bloque DGII dentro de comment; permite identificar/reemplazar
# el bloque en re-ejecuciones sin duplicar ni pisar notas del usuario.
DGII_COMMENT_MARKER = "[DGII]"
# Columnas reales del CSV DGII (orden observado en documentación)
DGII_COLUMNS = (
    "rnc",
    "razon_social",
    "actividad_economica",
    "fecha_inicio_operaciones",
    "estado",
    "regimen_pago",
)
EXPECTED_HEADERS_VARIANTS = {
    "rnc": ("rnc", "rnc_cedula"),
    "razon_social": ("razon_social", "razón social"),
    "actividad_economica": ("actividad_economica", "actividad económica"),
    "fecha_inicio_operaciones": ("fecha_inicio_operaciones", "fecha de inicio operaciones"),
    "estado": ("estado",),
    "regimen_pago": ("regimen_pago", "régimen de pago"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("import_rnc")

# User-Agent de navegador. DGII rechaza el UA por defecto de python-requests
# (responde 403 Forbidden), por lo que se simula un navegador real.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Importador RNC DGII → Odoo res.partner")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="No descarga si el ZIP ya existe localmente.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Hace todo el proceso pero no aplica el upsert final.",
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("DATA_DIR", str(DEFAULT_DATA_DIR)),
        help="Directorio destino del ZIP y la extracción.",
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("RNC_URL", DEFAULT_URL),
        help="URL del ZIP DGII.",
    )
    parser.add_argument(
        "--user-agent",
        default=os.environ.get("RNC_USER_AGENT", DEFAULT_USER_AGENT),
        help="User-Agent HTTP para la descarga (DGII bloquea UA de Python).",
    )
    return parser.parse_args()


def get_pg_params() -> dict:
    """Lee los parámetros de conexión desde el entorno."""
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "odoo17"),
        "user": os.environ.get("PGUSER", "odoo"),
        "password": os.environ.get("PGPASSWORD", ""),
    }


# -----------------------------------------------------------------------------
# Paso 1-2: Descarga
# -----------------------------------------------------------------------------


def download_zip(url: str, dest: Path, skip_if_exists: bool, user_agent: str) -> Path:
    """Descarga el ZIP a `dest`. Si ya existe y skip_if_exists, no descarga.

    DGII y otros portales gubernamentales suelen responder 403 al User-Agent
    por defecto de `python-requests`, por lo que se envían cabeceras de
    navegador. Se reintenta hasta 3 veces con backoff exponencial.

    Además se valida que el `Content-Length` declarado por el servidor
    coincida con los bytes recibidos. Si el servidor cierra la conexión
    prematuramente (típico en archivos grandes detrás de proxies con
    límites de tiempo), la descarga queda truncada aunque `r.raise_for_status()`
    no detecte nada. En ese caso el ZIP es inválido aunque la petición sea 200.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if skip_if_exists and dest.exists() and dest.stat().st_size > 0:
        log.info("ZIP ya existe, se omite descarga: %s (%.2f MB)",
                 dest, dest.stat().st_size / (1024 * 1024))
        # Aun así, validamos que sea un ZIP real (no corrupto) antes de reusarlo.
        if looks_like_zip(dest):
            try:
                with zipfile.ZipFile(dest) as zf:
                    bad = zf.testzip()
                    if bad is None:
                        return dest
                    log.warning("ZIP en caché falla testzip(): %s. Se re-descarga.", bad)
            except zipfile.BadZipFile:
                log.warning("ZIP en caché está corrupto. Se re-descarga.")
        else:
            log.warning("ZIP en caché no tiene firma PK\\x03\\x04. Se re-descarga.")
        try:
            dest.unlink()
        except OSError:
            pass

    headers = {
        "User-Agent": user_agent,
        "Accept": "application/zip,application/octet-stream,*/*;q=0.8",
        "Accept-Language": "es-DO,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://dgii.gov.do/",
        "Connection": "keep-alive",
    }

    log.info("Descargando %s -> %s", url, dest)
    last_exc: Optional[Exception] = None
    for attempt in range(1, 4):
        start = time.time()
        try:
            with requests.get(
                url,
                stream=True,
                timeout=300,           # subido de 120 a 300 s
                allow_redirects=True,
                headers=headers,
            ) as r:
                r.raise_for_status()
                expected = r.headers.get("Content-Length")
                expected_bytes = int(expected) if expected and expected.isdigit() else None
                received = 0
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 64):  # chunk menor
                        if chunk:
                            f.write(chunk)
                            received += len(chunk)
                elapsed = time.time() - start
                size_mb = received / (1024 * 1024)
                # Validar tamaño contra Content-Length
                if expected_bytes is not None and received != expected_bytes:
                    raise RuntimeError(
                        f"Descarga truncada: Content-Length={expected_bytes} "
                        f"pero recibidos={received} bytes "
                        f"(diferencia {expected_bytes - received}). "
                        f"El servidor probablemente cerró la conexión antes de tiempo."
                    )
                # Validar magic bytes antes de confiar
                if not looks_like_zip(dest):
                    preview = sniff_downloaded_content(dest)
                    try:
                        dest.unlink()
                    except OSError:
                        pass
                    raise RuntimeError(
                        f"Lo descargado no es un ZIP (firma PK\\x03\\x04 ausente). {preview}"
                    )
                log.info("Descarga OK en %.1fs (%.2f MB, esperado %s bytes)",
                         elapsed, size_mb,
                         expected_bytes if expected_bytes is not None else "?")
                return dest
        except (requests.exceptions.RequestException, RuntimeError) as exc:
            last_exc = exc
            log.warning("Intento %d/3 falló: %s", attempt, exc)
            try:
                if dest.exists():
                    dest.unlink()
            except OSError:
                pass
            if attempt < 3:
                time.sleep(2 ** attempt)
    assert last_exc is not None
    raise last_exc


# -----------------------------------------------------------------------------
# Paso 3: Descompresión
# -----------------------------------------------------------------------------


ZIP_MAGIC = b"PK\x03\x04"


def looks_like_zip(path: Path) -> bool:
    """Comprueba que el archivo empieza por la firma ZIP (PK\\x03\\x04)."""
    try:
        with open(path, "rb") as f:
            sig = f.read(4)
    except OSError:
        return False
    return sig == ZIP_MAGIC


def sniff_downloaded_content(path: Path) -> str:
    """Devuelve una vista previa textual de lo que se descargó (para diagnóstico)."""
    try:
        with open(path, "rb") as f:
            data = f.read(512)
    except OSError as exc:
        return f"<no se pudo leer: {exc}>"
    if data.lstrip().startswith(b"<"):
        return f"parece HTML/portal ({len(data)} bytes): {data[:200]!r}"
    if data.lstrip().startswith(b"{") or data.lstrip().startswith(b"["):
        return f"parece JSON ({len(data)} bytes): {data[:200]!r}"
    return f"binario ({len(data)} bytes): {data[:64]!r}"


def extract_zip(zip_path: Path, out_dir: Path) -> list[Path]:
    """Extrae el ZIP en `out_dir`. Devuelve la lista de archivos extraídos."""
    out_dir.mkdir(parents=True, exist_ok=True)
    # Validación previa: el archivo debe comenzar con la firma PK\x03\x04.
    # Si DGII devuelve HTML/captcha con HTTP 200, ZipFile fallará con BadZipFile
    # sin contexto. Mejor detectar aquí y abortar con un mensaje claro.
    if not looks_like_zip(zip_path):
        preview = sniff_downloaded_content(zip_path)
        try:
            zip_path.unlink()
        except OSError:
            pass
        raise RuntimeError(
            "El archivo descargado NO es un ZIP válido. "
            "DGII probablemente devolvió HTML/captcha o bloqueó la petición. "
            f"Contenido detectado: {preview}"
        )
    log.info("Extrayendo %s -> %s", zip_path, out_dir)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(out_dir)
            files = [out_dir / n for n in zf.namelist()]
    except zipfile.BadZipFile as exc:
        raise RuntimeError(
            f"El archivo en {zip_path} no se pudo abrir como ZIP: {exc}"
        ) from exc
    log.info("Archivos extraídos: %d", len(files))
    for f in files:
        log.info("  - %s (%.2f KB)", f.name, f.stat().st_size / 1024)
    return files


def pick_data_file(files: Iterable[Path]) -> Path:
    """Elige el archivo más grande con extensión típica de datos."""
    candidates = [
        f
        for f in files
        if f.is_file()
        and f.suffix.lower() in {".txt", ".csv", ".dat"}
        and f.stat().st_size > 0
    ]
    if not candidates:
        raise RuntimeError(
            "No se encontró archivo de datos (.txt/.csv/.dat) dentro del ZIP."
        )
    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
    chosen = candidates[0]
    log.info("Archivo seleccionado: %s", chosen)
    return chosen


# -----------------------------------------------------------------------------
# Paso 4: Detección de formato
# -----------------------------------------------------------------------------


def detect_dialect(path: Path) -> tuple[csv.Dialect, str]:
    """Detecta delimitador y encoding. Devuelve (dialect, encoding)."""
    # Probamos encodings comunes
    encodings = ("utf-8-sig", "latin-1", "cp1252")
    sample: Optional[str] = None
    used_encoding: Optional[str] = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict", newline="") as f:
                sample = f.read(8192)
            used_encoding = enc
            break
        except UnicodeDecodeError:
            continue
    if sample is None:
        # Último fallback permisivo
        with open(path, "r", encoding="latin-1", errors="replace", newline="") as f:
            sample = f.read(8192)
        used_encoding = "latin-1"

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
    except csv.Error:
        class _D(csv.excel):
            delimiter = "|"
        dialect = _D()

    log.info("Encoding detectado: %s | delimitador: %r",
             used_encoding, dialect.delimiter)
    return dialect, used_encoding  # type: ignore[return-value]


def normalize_header(h: str) -> str:
    return (h or "").strip().lower().replace("  ", " ")


def map_columns(headers: list[str]) -> dict[str, int]:
    """Devuelve un dict {campo_staging: indice_columna_csv}."""
    norm = [normalize_header(h) for h in headers]
    mapping: dict[str, int] = {}
    for canonical, variants in EXPECTED_HEADERS_VARIANTS.items():
        for v in variants:
            v_norm = normalize_header(v)
            if v_norm in norm:
                mapping[canonical] = norm.index(v_norm)
                break
    missing = [c for c in DGII_COLUMNS if c not in mapping]
    if missing:
        log.warning("Columnas DGII no detectadas en header: %s. Headers vistos: %s",
                    missing, headers)
    else:
        log.info("Mapeo de columnas OK: %s", mapping)
    return mapping


# -----------------------------------------------------------------------------
# Paso 5-7: Staging + Upsert
# -----------------------------------------------------------------------------


def iter_rows(path: Path, mapping: dict[str, int], encoding: str) -> Iterator[tuple]:
    """Genera tuplas por cada fila con todos los campos DGII mapeados:
    (rnc, name, commercial_name, economic_activity, state,
     fecha_inicio_operaciones, regimen_pago).
    """
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return
        col_map = mapping or map_columns(headers)
        for row in reader:
            if not row or all((c or "").strip() == "" for c in row):
                continue
            rnc = (row[col_map["rnc"]] if "rnc" in col_map else "").strip()
            if not rnc:
                continue
            name = (
                row[col_map["razon_social"]] if "razon_social" in col_map else ""
            ).strip()
            activity = (
                row[col_map["actividad_economica"]]
                if "actividad_economica" in col_map else ""
            ).strip()
            state = (
                row[col_map["estado"]] if "estado" in col_map else ""
            ).strip()
            fecha_inicio = (
                row[col_map["fecha_inicio_operaciones"]]
                if "fecha_inicio_operaciones" in col_map
                else ""
            ).strip()
            regimen = (
                row[col_map["regimen_pago"]] if "regimen_pago" in col_map else ""
            ).strip()
            # commercial_name, address, phone, email no vienen en el CSV
            yield (
                rnc,
                name or None,
                None,                     # commercial_name placeholder
                activity or None,
                state or None,
                fecha_inicio or None,
                regimen or None,
            )


DDL_STAGING = f"""
DROP TABLE IF EXISTS {STAGING_TABLE};
CREATE TABLE {STAGING_TABLE} (
    rnc                      TEXT,
    name                     TEXT,
    commercial_name          TEXT,
    economic_activity        TEXT,
    state                    TEXT,
    fecha_inicio_operaciones TEXT,
    regimen_pago             TEXT
);
CREATE INDEX ON {STAGING_TABLE} (rnc);
"""


def _dgii_comment_sql(staging_alias: str = "s") -> str:
    """Devuelve una expresión SQL que construye el bloque [DGII] para
    `res_partner.comment` con formato "Etiqueta: Valor" y un salto de
    línea (`E'\\n'`) después de cada campo.

    El bloque tiene esta forma:
        [DGII]\\n
        Actividad Económica: <valor>\\n
        Fecha Inicio Operaciones: <valor>\\n
        Estado: <valor>\\n
        Régimen de Pago: <valor>\\n

    Las líneas con valor vacío o NULL se omiten automáticamente
    (NULLIF sobre 'Etiqueta: '). Si todos los valores están vacíos,
    el bloque se reduce al marcador solo.

    `staging_alias` es el alias SQL de la tabla staging en el FROM
    (por defecto 's').
    """
    parts = [f"  '{DGII_COMMENT_MARKER}\\n'"]
    # Mapeo columna_staging → etiqueta. Usamos los nombres canónicos
    # del CSV (que coinciden con los nuevos nombres de columna staging).
    staging_col_for = {
        "actividad_economica":      "economic_activity",
        "fecha_inicio_operaciones": "fecha_inicio_operaciones",
        "estado":                   "state",
        "regimen_pago":             "regimen_pago",
    }
    for csv_key, label in DGII_COMMENT_FIELDS:
        col = staging_col_for[csv_key]
        # NULLIF sobre la concatenación descarta la línea si el valor está vacío.
        # Se concatena E'\n' al final para dejar un salto de línea tras cada campo.
        parts.append(
            f"  (NULLIF('{label}: ' || NULLIF({staging_alias}.{col}, ''),"
            f" '{label}: ') || E'\\n')"
        )
    return "CONCAT(\n" + ",\n".join(parts) + "\n)"


# res.partner NO se trunca: contiene datos de usuarios. Se hace
# upsert por coincidencia de vat (= rnc del CSV).
DGII_COMMENT_EXPR = _dgii_comment_sql("s")

UPDATE_SQL = f"""
UPDATE {TARGET_TABLE} rp
SET name              = s.name,
    company_name      = COALESCE(NULLIF(s.commercial_name, ''), rp.company_name),
    comment           = CASE
        WHEN rp.{DGII_LOADED_FIELD} = TRUE THEN rp.comment
        WHEN rp.comment IS NULL OR rp.comment = '' THEN {DGII_COMMENT_EXPR}
        ELSE rp.comment || E'\\n\\n' || {DGII_COMMENT_EXPR}
    END,
    {DGII_LOADED_FIELD} = TRUE
FROM {STAGING_TABLE} s
WHERE rp.vat = s.rnc
  AND s.rnc IS NOT NULL
  AND length(trim(s.rnc)) > 0;
"""

INSERT_SQL = f"""
INSERT INTO {TARGET_TABLE}
    (vat, name, company_name, is_company, active, comment, {DGII_LOADED_FIELD})
SELECT s.rnc,
       s.name,
       NULLIF(s.commercial_name, ''),
       TRUE,
       TRUE,
       {DGII_COMMENT_EXPR},
       TRUE
FROM {STAGING_TABLE} s
WHERE s.rnc IS NOT NULL
  AND length(trim(s.rnc)) > 0
  AND NOT EXISTS (
      SELECT 1 FROM {TARGET_TABLE} rp WHERE rp.vat = s.rnc
  )
RETURNING id;
"""


def ensure_staging_table(cur) -> None:
    log.info("Creando/limpiando tabla staging %s", STAGING_TABLE)
    cur.execute(DDL_STAGING)


def ensure_dgii_loaded_column(cur) -> None:
    """Asegura que la columna booleana del flag 'Cargado desde DGII' exista
    en `res_partner`. La crea como `BOOLEAN DEFAULT FALSE` si no existe.

    Esto permite ejecutar el script contra bases que aún no tienen el
    campo personalizado sin necesidad de hacerlo manualmente. El default
    FALSE garantiza que los partners existentes (no cargados por el script)
    no queden marcados.
    """
    cur.execute(
        f"ALTER TABLE {TARGET_TABLE} "
        f"ADD COLUMN IF NOT EXISTS {DGII_LOADED_FIELD} BOOLEAN DEFAULT FALSE"
    )
    log.info("Columna %s.%s garantizada (BOOLEAN DEFAULT FALSE)",
             TARGET_TABLE, DGII_LOADED_FIELD)


def copy_rows_to_staging(cur, rows: Iterable[tuple]) -> int:
    """Carga filas vía COPY FROM STDIN. Devuelve total cargado."""
    buf = io.StringIO()
    count = 0
    for row in rows:
        # Escape de saltos de línea y NULs para COPY TEXT
        safe = tuple(
            "" if v is None else str(v).replace("\\", "\\\\")
                                .replace("\n", " ").replace("\r", " ")
                                .replace("\t", " ")
            for v in row
        )
        buf.write("\t".join(safe) + "\n")
        count += 1
    buf.seek(0)
    log.info("COPY %d filas a %s", count, STAGING_TABLE)
    if count > 0:
        cur.copy_expert(
            f"COPY {STAGING_TABLE} "
            "(rnc, name, commercial_name, economic_activity, state, "
            "fecha_inicio_operaciones, regimen_pago) "
            "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '')",
            buf,
        )
    return count


def run_upsert(cur) -> tuple[int, int]:
    """Ejecuta el upsert. Devuelve (inserts, updates).

    Estrategia:
      1. UPDATE de partners existentes cuyo `vat` coincide con un rnc del CSV.
      2. INSERT de los partners nuevos (los que no existen por `vat`).
    En ambos casos se respeta la información que el usuario haya podido
    cargar manualmente (no se sobreescriben campos personalizados).
    """
    log.info("Ejecutando UPSERT a %s ...", TARGET_TABLE)
    cur.execute(f"SELECT count(*) FROM {STAGING_TABLE}")
    staging_count = cur.fetchone()[0]

    # 1) UPDATE existentes
    cur.execute(UPDATE_SQL)
    updated = cur.rowcount
    log.info("UPDATE %s: %d filas actualizadas", TARGET_TABLE, updated)

    # 2) INSERT nuevos
    cur.execute(INSERT_SQL)
    inserted_rows = cur.fetchall()
    inserted = len(inserted_rows)
    log.info("INSERT %s: %d filas nuevas", TARGET_TABLE, inserted)

    cur.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
    final_count = cur.fetchone()[0]
    log.info("UPSERT OK. staging=%d, inserted=%d, updated=%d, total_final=%d",
             staging_count, inserted, updated, final_count)
    return inserted, updated


# -----------------------------------------------------------------------------
# Orquestación
# -----------------------------------------------------------------------------


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    zip_path = data_dir / ZIP_NAME
    extract_dir = data_dir / "rnc_contribuyentes"

    total_start = time.time()

    # 1) Descarga
    download_zip(args.url, zip_path, skip_if_exists=args.skip_download,
                 user_agent=args.user_agent)

    # 2) Extracción
    files = extract_zip(zip_path, extract_dir)
    data_file = pick_data_file(files)

    # 3) Detección de formato
    dialect, encoding = detect_dialect(data_file)
    with open(data_file, "r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f, dialect=dialect)
        try:
            headers = next(reader)
        except StopIteration:
            log.error("El archivo de datos está vacío")
            return 2
    mapping = map_columns(headers)

    # 4) Conexión PG
    pg_params = get_pg_params()
    log.info("Conectando a PostgreSQL %s:%s/%s como %s",
             pg_params["host"], pg_params["port"], pg_params["dbname"], pg_params["user"])
    conn = psycopg2.connect(**pg_params)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            ensure_staging_table(cur)
            # Garantiza que el campo "Cargado desde DGII" exista en
            # res_partner antes del upsert (idempotente).
            ensure_dgii_loaded_column(cur)

            # 5) COPY staging
            total_rows = copy_rows_to_staging(
                cur,
                iter_rows(data_file, mapping, encoding),
            )
            log.info("Filas leídas del archivo: %d", total_rows)

            if args.dry_run:
                log.info("--dry-run activo: no se ejecuta el upsert final.")
                conn.commit()
                return 0

            # 6) Upsert a res.partner (sin truncate: la tabla contiene
            # datos de usuario; se hace UPDATE + INSERT por vat)
            inserted, updated = run_upsert(cur)

        conn.commit()
        log.info("Commit OK.")
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        log.exception("Error durante la carga: %s", exc)
        return 1
    finally:
        conn.close()

    elapsed = time.time() - total_start
    log.info("=" * 60)
    log.info("RESUMEN")
    log.info("  ZIP:           %s", zip_path)
    log.info("  Archivo data:  %s", data_file)
    log.info("  Encoding:      %s", encoding)
    log.info("  Filas leídas:  %d", total_rows)
    log.info("  Insertadas:    %d", inserted)
    log.info("  Actualizadas:  %d", updated)
    log.info("  Duración:      %.2fs", elapsed)
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
