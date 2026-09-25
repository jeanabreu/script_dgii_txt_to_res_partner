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

## Mapeo de campos

| DGII (CSV)           | `res.partner`     |
| -------------------- | ----------------- |
| `RNC`                | `vat`             |
| `Razon Social`       | `name`            |
| `Actividad comercial`| `comment`         |
| `Inicio Operaciones` | `comment`         |

\* 'Actividad comercial', 'Inicio Operaciones' y 'Estado' se agregan en al campo 'comment' del modelo res.partner

## Uso rápido

```bash
# Variables de entorno (o exportadas en el shell)
export PGHOST=192.168.16.82
export PGPORT=5432
export PGDATABASE=db_test_imp_rnc
export PGUSER=odoo18
export PGPASSWORD='dbprd01'


# Instalar depedencia
pip install psycopg2-binary --break-system-packages

# Dar permisos de ejecución a los archivos
chmod +x import_rnc.sh import_rnc.py

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
| `PGHOST`      | `192.168.16.82`                                                                    |
| `PGPORT`      | `5432`                                                                             |
| `PGDATABASE`  | `db_test_imp_rnc`                                                    |
| `PGUSER`      | `odoo18`                                                                          |
| `PGPASSWORD`  | `dbprd01`                                                                          |
| `DATA_DIR`    | `/tmp/data`                   |
| `RNC_URL`     | `https://dgii.gov.do/app/WebApps/Consultas/RNC/RNC_CONTRIBUYENTES.zip`             |
| `RNC_USER_AGENT` | UA de navegador (DGII bloquea el UA por defecto de `python-requests`)           |

## Notas

- Dependencias Python: `psycopg2-binary`, `requests`.
- pip install psycopg2-binary --break-system-packages
