# Productos de datos Gold

Definición funcional de cada producto: granularidad, claves, dimensiones conformadas, métricas, reglas de
negocio, tratamiento de vigencias e historico. Todos viven en `workspace.default` y se publican
por snapshot de `fecha_corte` (ver *Historico*).

Parámetros que afectan los resultados:
- **`fecha_corte`:** el pipeline la recibe como parámetro. Con los datos entregados es 2026-08-31.
- **`dias_proximo_vencer`:** 30 días por defecto.

---

## Dimensiones conformadas

Gold usa las mismas dimensiones con la misma definicion en todos los productos:

| Dimensión | Origen | Regla |
|---|---|---|
| Cliente | `silver_clientes` (SCD2) | Se toma la versión **vigente en la fecha de corte** (`vigente_desde <= corte < vigente_hasta`). Si una póliza no tiene cliente identificado, segmento y ciudad quedan como `NO_IDENTIFICADO` |
| Producto, sucursal, canal | `silver_polizas` | Atributos de la póliza; definen el grano de la siniestralidad mensual |
| Tiempo | Fecha de corte y mes calendario (`periodo` = primer día del mes) | Pagos por `fecha_pago`, siniestros por `fecha_ocurrencia` y emisión por `fecha_inicio_vigencia` |

## Tratamiento de vigencias

- **Vigencia inclusiva:** una póliza está vigente en la fecha D si `fecha_inicio_vigencia <= D <= fecha_fin_vigencia`.
- **Fecha de cancelación:** el core no la informa. Para pólizas `Cancelada` se toma:
  1. el primer evento `CANCELACION` dentro de la vigencia;
  2. si no hay, la `fecha_actualizacion`, siempre que caiga dentro de la vigencia;
  3. si tampoco, el fin de vigencia.

  La cancelación corta la exposición: `fecha_fin_efectiva = fecha_cancelacion`.
- **Estado al corte:** se deriva de fechas, no del texto del core. Se evalúa en este orden:
  1. `Cancelada`: estado `Cancelada` y `fecha_cancelacion <= corte`.
  2. `No iniciada`: inicio posterior al corte.
  3. `Renovada` o `Vencida`: fin anterior al corte. Es `Renovada` si el core la marcó así.
  4. `Proxima a vencer`: le quedan `dias_proximo_vencer` días o menos.
  5. `Vigente`: en cualquier otro caso.

  Cuando el estado del core contradice las fechas, `estado_inconsistente = true`. Con los datos entregados hay 1 caso: `POL0001173` dice `Vencida` en el core pero su vigencia termina justo el día de corte.

---

## gold_poliza_360

**Granularidad:** 1 fila por póliza y `fecha_corte`. **Clave:** (`fecha_corte`, `poliza_id`).
**Alcance:** pólizas aceptadas en Silver (las que están en cuarentena no se publican; aparecen en `gold_discrepancias`).

| Métrica | Definición |
|---|---|
| `prima_emitida` | Prima de la vigencia contratada |
| `prima_recaudada` | Suma de recaudos con estado `Aplicado` y `fecha_pago <= corte` |
| `prima_pendiente` | `prima_emitida - prima_recaudada`. Un valor negativo indica saldo a favor del cliente |
| `recaudo_pendiente_confirmar`, `recaudo_reversado` | Recaudos en estado `Pendiente` o `Reversado` (no suman a lo recaudado) |
| `num_siniestros`, `num_siniestros_abiertos` | Siniestros con ocurrencia anterior o igual al corte. Abiertos: estados `Abierto`, `En investigacion` y `Reactivado` |
| `costo_incurrido` | `valor_pagado + valor_reserva` de esos siniestros (última foto disponible) |
| `siniestralidad_poliza` | `costo_incurrido / prima_emitida` |
| `fecha_ultima_actividad`, `tipo_ultima_actividad` | La más reciente, anterior o igual al corte, entre actualización de póliza, pago, actualización de siniestro y movimiento |
| `num_pagos_fuera_vigencia`, `num_siniestros_fuera_vigencia` | Conteos según las reglas de consistencia de Silver |
| `advertencias_calidad` | Reglas que marcaron la póliza en Silver (p. ej. `polizas.RI.cliente`) |

## gold_siniestralidad_mensual

**Granularidad:** 1 fila por `periodo` (mes), `producto` y `sucursal`, en cada `fecha_corte`.
**Clave:** (`fecha_corte`, `periodo`, `producto`, `sucursal`).

| Métrica | Definición |
|---|---|
| `polizas_expuestas` | Pólizas con al menos un día de vigencia efectiva en el mes, hasta el corte |
| `exposicion_anios_poliza` | Suma de días expuestos / 365,25 |
| `prima_devengada` | Por póliza: `prima x días expuestos en el mes / días de vigencia contratada`, sumado al grano |
| `polizas_emitidas`, `prima_emitida` | Pólizas y prima según el mes de inicio de vigencia |
| `prima_recaudada` | Recaudos `Aplicado` según el mes de `fecha_pago` |
| `prima_pendiente_acumulada` | Suma acumulada de (emitida - recaudada) por producto y sucursal hasta el mes |
| `siniestros`, `costo_incurrido` | Siniestros según el mes de ocurrencia; incurrido = pagado + reserva |
| `razon_siniestralidad` | `costo_incurrido / prima_devengada` (vacío si no hay prima devengada) |

Con los datos entregados, la razón de siniestralidad está entre 4 y 8 (400 % a 800 %). Los montos
de siniestros (10 a 40 millones) superan con creces las primas (0,1 a 15 millones). Es una
característica de los datos sintéticos, no un error de cálculo: la reconciliación cuadra al centavo.

## gold_discrepancias

**Granularidad:** 1 fila por hallazgo y registro, en cada `fecha_corte`. Responde a la pregunta *"¿qué pólizas presentan pagos sin correspondencia, siniestros sin póliza válida o movimientos fuera de vigencia?"*.

| tipo_discrepancia | Severidad | Origen |
|---|---|---|
| `PAGO_SIN_POLIZA` | alta | Cuarentena: regla `recaudos.RI.poliza` |
| `SINIESTRO_SIN_POLIZA` | alta | Cuarentena: regla `siniestros.RI.poliza` |
| `SINIESTRO_FUERA_DE_VIGENCIA` | alta | Silver: posible siniestro no cubierto |
| `PAGO_A_POLIZA_EN_CUARENTENA`, `SINIESTRO_DE_POLIZA_EN_CUARENTENA` | media | Silver: el padre existe, pero está retenido |
| `MOVIMIENTO_FUERA_DE_VIGENCIA` | media | Cambios de estado y cancelaciones fuera de vigencia. `CREACION` y `RENOVACION` se excluyen porque es normal que ocurran en los bordes |
| `POLIZA_SIN_CLIENTE` | media | Silver: regla `polizas.RI.cliente` |
| `RECAUDO_EXCEDE_PRIMA` | media | `gold_poliza_360`: lo recaudado supera la prima emitida |
| `PAGO_FUERA_DE_VIGENCIA` | baja | Silver: puede ser anticipo o mora |

## gold_calidad_pipeline

**Granularidad:** 1 fila por control (regla de calidad o reconciliación), dataset y ejecución.
- Une `ctl_calidad_resultados` con `ctl_reconciliacion` (Bronze→Silver y Silver→Gold).
- `es_ultima_ejecucion` marca la ejecución más reciente de cada dataset.
- Se reconstruye completo en cada ejecución, porque es un consolidado de las tablas de control.

---

## Historico
- **Silver:** guarda el estado actual, salvo clientes, que usa SCD2.
- **Gold:** se publica por **snapshot mensual**. Cada corte se escribe con `replaceWhere fecha_corte = <corte>`.
    - Reprocesar un corte lo reemplaza exactamente, sin duplicar.
    - Los cortes anteriores se conservan, lo que permite comparar la cartera mes a mes.
- **Backfill de un corte pasado:** usa la foto actual de Silver filtrada por fechas, y la versión del cliente vigente en ese corte (deuda técnica DT-01).

## Reconciliación Silver → Gold
En cada publicación se verifican, con el mismo alcance (corte y pólizas válidas):
- el conteo de pólizas;
- la prima emitida;
- la prima recaudada;
- el costo incurrido (en `gold_poliza_360` y en `gold_siniestralidad_mensual`).

El resultado queda en `ctl_reconciliacion` y en `gold_calidad_pipeline`. Si hay diferencias, la tarea falla.