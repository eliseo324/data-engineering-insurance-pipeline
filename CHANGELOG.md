# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y
[versionado semántico](https://semver.org/lang/es/). Cada versión publicada corresponde a un tag
`vMAYOR.MENOR.PARCHE`, que es el artefacto que se promueve de QA a PROD.

<!-- ## [Sin publicar] -->

### Agregado
- Ingesta Bronze idempotente por hash de archivo, con metadatos de ingesta y control de esquema.
- Data contracts YAML por dataset (esquema, tipos, dominios, PII, claves y SLA).
- Silver incremental por watermark: tipado, reglas de calidad declarativas, cuarentena, deduplicación
  determinística, SCD2 de clientes, MERGE idempotente y reconciliación.
- Productos Gold por snapshot de fecha de corte: `gold_poliza_360`, `gold_siniestralidad_mensual`,
  `gold_discrepancias` y `gold_calidad_pipeline`, con reconciliación Silver → Gold.
- Workflow como código (Asset Bundles) programado el día 5 a las 06:00 (America/Bogota), con
  postcondiciones, publicación con SLA y simulación de fallas.
- Observabilidad: vistas `vw_obs_*` y alertas con severidad y responsable.
- Gobierno: documentación y etiquetas PII en Unity Catalog, enmascaramiento, permisos por rol,
  retención y derecho de supresión.
- CI/CD: cobertura mínima, compatibilidad de contratos, despliegue DEV → QA → PROD con aprobaciones
  y rollback de código y de datos.
- Documento de arquitectura con diagramas como código (objetivo en Azure e implementación en Free
  Edition) y diseño de las extensiones de tiempo casi real e IA generativa.
