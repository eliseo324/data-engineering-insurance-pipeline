## Qué cambia y por qué

<!-- Resumen del cambio y su motivación (incidente, requerimiento, deuda técnica). -->

## Tipo de cambio

- [ ] Funcionalidad nueva
- [ ] Corrección
- [ ] Cambio de data contract (versión semántica actualizada)
- [ ] Reglas de calidad / umbrales
- [ ] Permisos o gobierno (`conf/gobierno.yml`)
- [ ] Orquestación (job) o infraestructura (bundle)
- [ ] Documentación

## Controles previos

- [ ] La CI pasa: lint, pruebas, cobertura ≥ 70 %, compatibilidad de contratos y secretos
- [ ] Si cambia un contrato: la versión sube según el nivel del cambio y el dueño del dato aprobó
- [ ] Si cambian permisos: revisé el plan (`80_gobierno` con `modo_gobierno = plan`)
- [ ] Si cambia el job: `databricks bundle validate` y la validación del DAG pasan
- [ ] Ejecuté el pipeline en DEV con `fecha_corte` de prueba y las postcondiciones pasaron
- [ ] Actualicé `CHANGELOG.md` y la documentación (`docs/decisiones.md` si hubo una decisión)

## Impacto y rollback

<!-- Tablas o productos afectados, si requiere reproceso (modo=reproceso) o backfill, y cómo revertir:
     código -> redesplegar el tag anterior; datos -> notebooks/92_rollback_gold. -->

## Evidencia

<!-- Capturas de la ejecución, consultas de validación, resultados de calidad. -->
