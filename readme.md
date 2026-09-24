# Importador RNC DGII → Odoo `res.partner`

Importa el archivo oficial de contribuyentes de la **DGII República Dominicana**
(`RNC_CONTRIBUYENTES.zip`) directamente a la tabla `res_partner` del modelo
estándar de Odoo **`res.partner`**.

## Archivos

- `import_rnc.sh` — wrapper bash que exporta variables de entorno y lanza el script Python.
- `import_rnc.py` — importador (descarga → extracción → staging → upsert en PostgreSQL).

## Flujo

1. **Descarga** el ZIP desde el portal DGII (con reintentos y validación de integridad).
2. **Extrae** el archivo y selecciona el CSV/TXT más grande.
3. **Carga** los datos en una tabla de staging `res_partner_rnc_staging` vía `COPY FROM STDIN`.
4. **Upsert** sobre `res_partner` con la siguiente lógica:
   - `UPDATE` de partners existentes cuando `res_partner.vat = staging.rnc`.
   - `INSERT` de partners nuevos (con `is_company = TRUE`).

> **Importante:** la tabla `res_partner` **NO** se trunca — contiene datos de
> usuario. La importación es **no destructiva**.

## Mapeo de campos

| DGII (CSV)           | `res.partner`     |
| -------------------- | ----------------- |
| `rnc`                | `vat`             |
| `razon_social`       | `name`            |
| `commercial_name` *  | `company_name`    |
| _(implícito)_        | `is_company=TRUE` |

\* `commercial_name` no viene en el CSV DGII; la columna se conserva en staging
por compatibilidad/extensión futura.

## Uso rápido

```bash
# Variables de entorno (o exportadas en el shell)
export PGHOST=15.204.246.110
export PGPORT=6475
export PGDATABASE=api.coolify.gestionsimple.com
export PGUSER=odooapi
export PGPASSWORD='***'

# Ejecutar
./import_rnc.sh

# Reutilizar el ZIP ya descargado
./import_rnc.sh --skip-download

# Simular sin aplicar el upsert final
./import_rnc.sh --dry-run
```

## Variables de entorno

| Variable      | Default                                                                            |
| ------------- | ---------------------------------------------------------------------------------- |
| `PGHOST`      | `15.204.246.110`                                                                   |
| `PGPORT`      | `6475`                                                                             |
| `PGDATABASE`  | `api.coolify.gestionsimple.com`                                                    |
| `PGUSER`      | `odooapi`                                                                          |
| `PGPASSWORD`  | _(vacía)_                                                                          |
| `DATA_DIR`    | `/opt/containers_files/odooapi/extra-addons/gs_master_data/data`                   |
| `RNC_URL`     | `https://dgii.gov.do/app/WebApps/Consultas/RNC/RNC_CONTRIBUYENTES.zip`             |
| `RNC_USER_AGENT` | UA de navegador (DGII bloquea el UA por defecto de `python-requests`)           |

## Notas

- El script usa un `User-Agent` de navegador real porque la DGII devuelve
  `403 Forbidden` al UA por defecto de `python-requests`.
- Se valida la firma ZIP (`PK\x03\x04`) y el `Content-Length` para detectar
  descargas truncadas.
- Encoding detectado automáticamente entre `utf-8-sig`, `latin-1` y `cp1252`.
- Dependencias Python: `psycopg2-binary`, `requests`.