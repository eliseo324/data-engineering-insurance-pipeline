# data-engineering-insurance-pipeline

Solución end-to-end de ingeniería de datos para integrar **clientes, pólizas, recaudos, siniestros y
movimientos** en una arquitectura **Medallion (Bronze, Silver, Gold)** sobre **Databricks Free Edition**,
**Delta Lake** y **Unity Catalog**, con calidad de datos, cuarentena, reconciliación, observabilidad,
gobierno y .

## Resumen de la Arquitectura

```
CSV (/volume1/raw/<dataset>)
   │  ingesta parametrizada, idempotente por hash de archivo, se evita rutas, fechas y nombres rígidos
   
BRONZE  bronze_<dataset>      copia fiel (STRING) + metadatos de ingesta
   │  tipado, dedup, reglas de calidad, cuarentena, conformación de claves, SCD2
   
SILVER  silver_<dataset>      + silver_cuarentena
   │  modelado orientado a casos de uso
   
GOLD    gold_poliza_360 · gold_siniestralidad_mensual · gold_discrepancias · gold_calidad_pipeline
CONTROL ctl_ejecuciones · ctl_archivos_procesados · ctl_esquemas · ctl_watermarks · ctl_calidad_resultados · ctl_reconciliacion
```

Todo esta en `workspace.default` y lo gobierna Unity Catalog.

## Estructura del repositorio
De acuerdo a los requerimientos se propone lo siguiente:

| Ruta | Contenido |
|---|---|
| `src/common` | Configuración parametrizada, logging, fechas, hashing, contratos y tablas de control |
| `src/ingestion` | Descubrimiento de archivos e ingesta Bronze |
| `src/transformations` | Transformaciones Silver y Gold |
| `src/quality` | Perfilamiento, motor de reglas, cuarentena y reconciliación |
| `conf/contracts` | Data contracts YAML por dataset (esquema, tipos, protección de información sensible(PII), claves, SLA) |
| `notebooks` | Notebooks Databricks (formato fuente `.py`) que orquestan los módulos |
| `tests` | Pruebas unitarias (pytest + Spark local) |
| `resources` | Definición del workflow (Databricks Asset Bundles) |
| `docs` | Arquitectura, decisiones, evidencias |
| `sql` | Consultas de observabilidad, gobierno y trazabilidad |

## Convención de nombres

| Objeto | Patrón | Ejemplo |
|---|---|---|
| Tabla Bronze | `bronze_<dataset>` | `bronze_polizas` |
| Tabla Silver | `silver_<dataset>` | `silver_siniestros` |
| Producto Gold | `gold_<producto>` | `gold_poliza_360` |
| Tabla de control | `ctl_<tema>` | `ctl_ejecuciones` |
| Vista segura | `vw_<objeto>_protegida` | `vw_clientes_protegida` |
| Función UC | `fn_<accion>_<objeto>` | `fn_mask_documento` |
| Columna técnica | prefijo `_` | `_run_id`, `_source_file` |


## Desarrollo local y pruebas
Para ejecuciones locales tener en cuenta que se requiere Java 17 para Spark local
```bash
python -m venv .venv && source .venv/bin/activate
```
```bash
pip install -e ".[dev]"
```
Para checkear tipado
```bash
ruff check .
```
testeo
```bash
pytest -v
```

La integración continua (GitHub Actions, `.github/workflows/ci.yml`) ejecuta lint y pruebas en cada PR hacia `dll`.

## Documentación
- [Supuestos, decisiones y deuda técnica](docs/decisiones.md)
- [Productos de datos Gold: granulidad, métricas y reglas](docs/productos_gold.md)