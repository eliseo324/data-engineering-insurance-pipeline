# Observabilidad técnica y de datos

La observabilidad se apoya en tres piezas:

1. **Tablas de control** (`ctl_*`). Las escribe cada tarea del pipeline: ejecuciones, archivos, esquemas, calidad, reconciliación, publicaciones y alertas.
2. **Vistas `vw_obs_*` en Unity Catalog.** Son una capa semántica estable y gobernada sobre las tablas de control. Las publica `notebooks/60_observabilidad`.
3. **Tablero y alertas** que consultan esas vistas.

La tarea `observabilidad` es la última del job y usa `run_if: ALL_DONE`: **se ejecuta aunque falle una tarea anterior**. Así el tablero y las alertas reflejan también las ejecuciones fallidas.

## 1. Métricas y dónde se consultan

| Métrica requerida | Vista | Columnas clave |
|---|---|---|
| Estado de la ejecución | `vw_obs_ejecuciones` | `estado_ejecucion` (PUBLICADO / FALLIDO / PARCIAL), `tareas_fallidas`, `ultimo_error` |
| Duración | `vw_obs_ejecuciones`, `vw_obs_tareas` | `duracion_seg` por ejecución y por tarea |
| Volumen de entrada y salida | `vw_obs_volumenes` | `entrada`, `salida` por capa y dataset |
| Registros aceptados y rechazados | `vw_obs_volumenes`, `vw_obs_cuarentena` | `salida` (aceptados), `rechazados`, `pct_rechazo`; cuarentena por regla y estado de gestión |
| Duplicados | `vw_obs_volumenes` | `duplicados` (exactos + por clave), `pct_duplicados` |
| Frescura | `vw_obs_frescura` | `rezago_dias_vs_corte`, `frescura_max_dias` (del contrato), `dias_desde_ultima_ingesta`, `estado_frescura` |
| Evolución del esquema | `vw_obs_esquemas` | `columnas_nuevas`, `columnas_faltantes`, `cambio_detectado`, `esquema_hash` vs el anterior |
| Calidad | `vw_obs_calidad` | `ok`, `advertencias`, `fallas` por capa (silver, postcondición) y dataset |
| Costo o consumo | `vw_obs_consumo` (estimado); `vw_obs_consumo_real` si hay *system tables* | `horas_computo`, `dbu_estimados`, `usd_estimados` |
| Cumplimiento del SLA | `vw_obs_sla` | `cumple_sla`, `minutos_de_margen`, `aplica_sla` (solo ejecuciones programadas) |

**Sobre el consumo:**
- Free Edition no expone `system.billing`, y su costo es cero. Por eso `vw_obs_consumo` lo **estima** con duración × DBU/hora × USD/DBU. Los supuestos están en `conf/observabilidad.yml`.
- Si las *system tables* están disponibles, el notebook crea además `vw_obs_consumo_real` (`system.billing.usage` + `list_prices`, por `job_run_id`). Esa es la fuente que se usa en el entorno corporativo.

## 2. Tablero AI/BI

**Fila 1, indicadores (counters):**
- última ejecución: `estado_ejecucion` del registro más reciente;
- duración de la última ejecución;
- cumplimiento del SLA en los últimos 12 cortes (% de `cumple_sla` donde `aplica_sla`);
- alertas abiertas por severidad.

**Fila 2, tendencias:**
- duración por ejecución (línea; eje X `inicio`, Y `duracion_seg`);
- duración por tarea en la última ejecución (barras horizontales);
- volumen por dataset y capa (barras agrupadas; `entrada`, `salida`, `rechazados`, `duplicados`).

**Fila 3, calidad y datos:**
- calidad por dataset (barras apiladas `ok` / `advertencias` / `fallas`);
- frescura (tabla con formato condicional en `estado_frescura`);
- evolución del esquema (tabla);
- consumo estimado por ejecución (barras);
- alertas abiertas (tabla).

4. **Filtros:** agrega filtros globales por `fecha_corte` y `dataset`.
5. **Publicación:** publica el tablero y compártelo con los evaluadores con permiso *Can view*.
6. **Versionado (recomendado):** usa *Export* para obtener el `.lvdash.json` y guárdalo en `resources/dashboards/observabilidad.lvdash.json`. Para desplegarlo por ambiente con Asset Bundles, se agrega como recurso:

```yaml
resources:
  dashboards:
    observabilidad:
      display_name: Observabilidad · insurance-medallion
      file_path: ../dashboards/observabilidad.lvdash.json
      warehouse_id: ${var.warehouse_id}
```

Si el tablero AI/BI no estuviera disponible, `notebooks/60_observabilidad` muestra las mismas vistas como tablero de control dentro del notebook.

## 3. Alertas

Las alertas se evalúan al final de cada ejecución y las activas quedan en `ctl_alertas`. Las fallas del job notifican además por correo desde el propio workflow (`on_failure`, `on_duration_warning`).

| ID | Alerta | Severidad | Responsable | Condición |
|---|---|---|---|---|
| OBS-01 | Ejecución fallida | P2 | Ingeniería de datos | La última ejecución tiene tareas fallidas |
| OBS-02 | SLA incumplido | P1 | Ingeniería de datos | La última publicación programada salió después del día 5 a las 08:00 |
| OBS-03 | Publicación mensual pendiente | P1 | Ingeniería de datos | Venció el SLA del mes y no hay publicación para el corte (solo en `prod`) |
| OBS-04 | Frescura vencida | P3 | Dueño del dato | El dato más reciente supera la frescura máxima del contrato |
| OBS-05 | Advertencias de calidad | P3 | Dueño del dato | Reglas en advertencia en la última ejecución |
| OBS-06 | Cambio de esquema | P2 | Dueño del dato | Columnas nuevas o faltantes en archivos de los últimos 35 días |
| OBS-07 | Duración anómala | P3 | Plataforma | Más del doble de la mediana de las ejecuciones anteriores (mínimo 3) |
| OBS-08 | Cuarentena pendiente | P3 | Dueño del dato | Más de 50 registros en cuarentena sin gestionar |

**Notificación en tiempo real (opcional):** para recibir correo por las alertas que no hacen fallar al job, crea una *SQL Alert* en **Alerts → Create alert** con:
- consulta: `SELECT count(*) AS alertas_p1_p2 FROM workspace.default.ctl_alertas WHERE estado = 'ABIERTA' AND severidad IN ('P1','P2') AND fecha_evaluacion >= current_timestamp() - INTERVAL 1 DAY`;
- condición: `alertas_p1_p2 > 0`;
- programación: diaria;
- destino: el correo del equipo.

En el entorno corporativo el destino sería un webhook a Teams o Slack y a la herramienta de incidentes.

## 4. Procedimiento de atención

1. **Recepción.** La alerta llega por correo (job o SQL Alert) o se ve en el tablero, con su severidad y responsable.
2. **Triage** con el tablero:

| Pregunta | Dónde mirar |
|---|---|
| ¿Qué falló? | `vw_obs_ejecuciones.ultimo_error` y `vw_obs_tareas` |
| ¿Es un problema de datos o de infraestructura? | `vw_obs_calidad`, `vw_obs_cuarentena`, `vw_obs_esquemas` |
| ¿Hay riesgo para el SLA? | `vw_obs_sla` y la hora actual frente al día 5 a las 08:00 |

3. **Acción** según `docs/operacion.md`, sección 5:
   - reintento o *Repair run*;
   - corrección en origen por el dueño del dato;
   - ajuste versionado de contrato o reglas;
   - rollback de un producto con `RESTORE TABLE`.
4. **Cierre.** Se actualiza `ctl_alertas.estado` (`EN_ATENCION`, `RESUELTA`) y se registra la causa raíz. Un incidente P1 o P2 lleva un postmortem breve con la acción preventiva: nueva regla, nueva postcondición o ajuste de umbral.

Los tiempos de respuesta por severidad (P1: 30 min, P2: 2 h hábiles, P3: 1 día hábil) y el RACI están en `docs/operacion.md`.
