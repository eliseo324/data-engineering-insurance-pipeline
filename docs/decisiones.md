# Supuestos, decisiones y deuda técnica

Registro de hallazgos y decisiones tomadas, respaldadas con cifras del notebook `01_perfilamiento` sobre los archivos entregados. 

---

## 1. Hallazgos del perfilamiento

Fecha de corte de referencia: **2026-08-31**. Los datos llegan hasta el 2026-08-31 y, con esa fecha,
el estado `Vigente` del core coincide con la vigencia en el 100 % de los casos. Ver D-03.

| Dataset | Registros | Únicos por clave | Hallazgos principales |
|---|---:|---:|---|
| clientes | 502 | 501 | 1 duplicado exacto (`CLI000021`); 6 `numero_documento` vacíos; 3 formatos de fecha |
| polizas | 1.502 | 1.501 | 1 duplicado exacto (`POL0000101`); 5 `producto` vacíos; 5 primas negativas; 8 pólizas con cliente inexistente (`CLI_ORF*`); 5 con `fin_vigencia` < `inicio_vigencia` (todas con fin = 2022-01-01) |
| recaudos | 5.002 | 5.001 | 1 duplicado exacto (`REC00000201`); 6 `fecha_pago` vacías; 8 valores negativos con estado `Aplicado`; 12 pagos a pólizas inexistentes (`POL_ORF*`); ~49 % de pagos fuera de la vigencia de la póliza |
| siniestros | 2.502 | 2.501 | 1 duplicado exacto (`SIN00000301`); 5 `estado` vacíos; 6 `valor_pagado` negativos; 10 siniestros sin póliza (`POL_ORF_S*`); 5 con aviso < ocurrencia; 24 con aviso posterior al corte; ~80 % con ocurrencia fuera de vigencia |
| movimientos | 8.002 | 8.001 | 1 duplicado exacto (`EVT000000501`); 1 **reenvío con distinto `evento_id`** (`EVT999999991` = `EVT000001201`); 8 `fecha_evento` vacías; 3 con ingesta < evento; 2.471 eventos tardíos (> 30 días); 20 huérfanos; 8 grupos con la misma fecha para la misma entidad |

Hallazgos transversales:
- **Formato de archivo:** separador `;`, codificación UTF-8. El `payload` es JSON con comillas escapadas como `""`.
- **Fechas:** `yyyy-MM-dd` (alrededor del 95 %), `dd/MM/yyyy` (alrededor del 3 %) y `yyyy/MM/dd` (alrededor del 2 %). Las fechas con `/` son **día/mes**: el primer componente llega a 25 y el segundo nunca pasa de 12.
- **Volumen:** muy inferior al orientativo del enunciado (500 a 8.000 filas frente a 100 mil a 3 millones). El diseño se mantiene escalable y no depende de que los datos quepan en memoria.
- **Esquema:** cada dataset llega en un solo archivo, por lo que no se observa evolución entre archivos. Se simula con un segundo archivo para evidenciar el manejo (ver D-07).
- **Nombres de columnas:** difieren del enunciado (`fecha_inicio_vigencia` en lugar de `inicio_vigencia`, `valor_pagado`/`valor_reserva` en lugar de `pagado`/`reserva`) y hay columnas adicionales (`tipo_documento`, `correo`, `telefono`, `moneda`, `canal`, `referencia_bancaria`, `fecha_ingesta`, `secuencia`, `origen`). El contrato sigue a la fuente real.
- **Datos sintéticos aleatorios:** el ~80 % de siniestros y el ~49 % de pagos caen fuera de vigencia, y el `estado_destino` del payload no siempre corresponde a la entidad (p. ej. `Aplicado` en una póliza). En un entorno real estas tasas dispararían una alerta de bloqueo; aquí se tratan como *informativas* para no detener el pipeline por una característica del generador (ver D-11).

---

## 2. Decisiones

### D-01 · Bronze almacena todo como STRING
Bronze es una copia fiel y no interpreta tipos. Así se evita perder valores como `22/11/2024` o
montos mal formados por un casteo temprano, y el reproceso no depende de reglas que pueden cambiar.
El casteo y su validación ocurren en Silver.

### D-02 · Idempotencia en Bronze por hash del contenido del archivo
- Cada archivo se identifica por el SHA-256 de su contenido, registrado en `ctl_archivos_procesados`.
- **Modo `normal`:** se omite un archivo ya ingerido, aunque llegue con otro nombre. Esto cubre "registros que llegan nuevamente en una reejecución".
- **Modo `reproceso`:** se reescriben las filas del archivo con `replaceWhere _source_file_hash IN (...)`. La tabla queda en el mismo estado, sin duplicados.
- Un archivo con el mismo nombre pero distinto contenido se trata como una versión nueva y se ingiere; Silver resuelve cuál versión prevalece.

### D-03 · Fecha de corte y calendario
- `fecha_corte` es un parámetro con formato `YYYY-MM-DD`. Si se deja vacío, toma el último día del mes anterior en hora de Colombia, porque el job corre el día 5 de cada mes a las 6:00 a. m. y procesa el mes cerrado.
- Con los datos entregados el corte natural es **2026-08-31**: es la fecha máxima de `fecha_actualizacion`, `fecha_pago` y `fecha_evento`, y con ella no hay pólizas `Vigente` con vigencia terminado.

### D-04 · Parseo de fechas multiformato
Se usa `colaesce(try_to_timestamp(x, fmt) ...)` en el orden `yyyy-MM-dd`, `dd/MM/yyyy`, `yyyy/MM/dd`.
`try_to_timestamp` devuelve NULL en lugar de fallar, lo cual es necesario porque Databricks serverless
tiene ANSI activo. Un valor no nulo en origen que no se pueda parsear se trata como error de validez
y va a cuarentena.

### D-05 · Data contracts en YAML (`conf/contracts/`)
Hay un contrato por dataset con esquema, tipos, obligatoriedad, dominios, patrones, protección de información sensible, clave de
negocio, orden de versiones, referencias y SLA. De ese contrato se derivan:
1. la validación del encabezado en Bronze;
2. el casteo en Silver;
3. las reglas automáticas de completitud, dominio y formato;
4. los comentarios y etiquetas de protección de información sensible(PII) en Unity Catalog.

Un cambio en el contrato se versiona (`version`) y se revisa por Pull Request.

### D-06 · Duplicados
- **Exactos:** mismo `_row_hash`. Se descartan y se cuentan en la reconciliación.
- **Por clave de negocio:** se conserva la versión más reciente según el `orden_version` del contrato. El desempate es determinístico: `_ingestion_ts` desc y luego `_row_hash`, lo que resuelve las actualizaciones con la misma marca temporal.
- **Movimientos:** la clave de negocio es `(entidad, id_entidad, tipo_evento, fecha_evento, secuencia, payload)`. Un reenvío con otro `evento_id` es el mismo hecho y no se duplica.

### D-07 · Evolución de esquema
- Una columna nueva se conserva en Bronze (`mergeSchema`), se registra en `ctl_esquemas` y genera una advertencia.
- Si falta una columna requerida, el archivo se rechaza (`RECHAZADO_ESQUEMA`) y la tarea falla con severidad de bloqueo. Los demás archivos válidos del lote sí se ingieren.

### D-08 · Tablas Delta administradas (managed)
- Free Edition no permite external locations sobre ADLS.
- Las tablas administradas delegan a Unity Catalog el ciclo de vida, la optimización predictiva y el `VACUUM`.
- En el entorno corporativo se usarían tablas administradas sobre un catálogo con *managed location* propia por ambiente en ADLS. Las tablas externas quedarían solo para datos compartidos con otros motores.

### D-09 · Particionamiento y archivos pequeños
- No se particiona: con menos de 10 mil filas por tabla (y aun con los volúmenes orientativos de millones), particionar por fecha crearía muchos archivos pequeños.
- Se usa *liquid clustering* y `OPTIMIZE` sobre las columnas de filtro (`fecha_corte`, `poliza_id`).
- En Bronze, cada ejecución escribe un único commit por dataset (unión de los archivos del lote) para no fragmentar.

---