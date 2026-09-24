#!/usr/bin/env bash
# =============================================================================
# import_rnc.sh
# Wrapper bash que prepara variables de entorno y ejecuta el importador
# Python de contribuyentes DGII hacia Odoo (res.partner).
# =============================================================================
#   Pasos:
# Variables de entorno (o exportadas en el shell)
export PGHOST=192.168.16.80
export PGPORT=5432
export PGDATABASE=db_test_imp_rnc
export PGUSER=odoo18
export PGPASSWORD='dbprd01'
#   ./scripts/import_rnc.sh
#   Reutilizando ZIP ya descargado
#   ./scripts/import_rnc.sh --skip-download
#



set -euo pipefail

# ---------- Configuración por defecto ---------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ADDONS_DIR="$( dirname "$SCRIPT_DIR" )"

: "${DATA_DIR:=/tmp/data}"
: "${RNC_URL:=https://dgii.gov.do/app/WebApps/Consultas/RNC/RNC_CONTRIBUYENTES.zip}"
: "${PYTHON_BIN:=python3}"

# Conexión PostgreSQL (exportadas para que import_rnc.py las consuma)
: "${PGHOST:=192.168.16.80}"
: "${PGPORT:=5432}"
: "${PGDATABASE:=db_test_imp_rnc}"
: "${PGUSER:=odoo18}"
# PGPASSWORD puede no estar definida. Si lo está, se exporta.
if [[ -n "${PGPASSWORD:-}" ]]; then
    export PGPASSWORD
fi

#Ejecutar con password PostgreSQL
#PGPASSWORD='MiPassword123' ./script.sh

export DATA_DIR RNC_URL PGHOST PGPORT PGDATABASE PGUSER

echo "[import_rnc.sh] DATA_DIR  = ${DATA_DIR}"
echo "[import_rnc.sh] RNC_URL   = ${RNC_URL}"
echo "[import_rnc.sh] PGHOST    = ${PGHOST}:${PGPORT}/${PGDATABASE} (user=${PGUSER})"

# ---------- Verificaciones básicas -----------------------------------------
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "[import_rnc.sh] ERROR: $PYTHON_BIN no encontrado en PATH" >&2
    exit 1
fi

if ! "$PYTHON_BIN" -c "import psycopg2" >/dev/null 2>&1; then
    echo "[import_rnc.sh] ERROR: falta psycopg2. Instalar con: pip install psycopg2-binary" >&2
    exit 1
fi

if ! "$PYTHON_BIN" -c "import requests" >/dev/null 2>&1; then
    echo "[import_rnc.sh] ERROR: falta requests. Instalar con: pip install requests" >&2
    exit 1
fi

# ---------- Ejecución -------------------------------------------------------
echo "[import_rnc.sh] Lanzando importador Python..."
exec "$PYTHON_BIN" "$SCRIPT_DIR/import_rnc.py" "$@"
