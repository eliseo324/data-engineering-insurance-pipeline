from __future__ import annotations

COLUMNAS_TECNICAS = {
    "_source_file": "Ruta del archivo fuente en el volumen (linaje a la fuente)",
    "_source_file_name": "Nombre del archivo fuente",
    "_source_file_hash": "SHA-256 del contenido del archivo fuente (idempotencia de ingesta)",
    "_source_file_mtime": "Fecha de modificación del archivo fuente",
    "_ingestion_ts": "Momento de la ingesta a Bronze (UTC); base del watermark de Silver",
    "_run_id": "Ejecución que ingirió el registro a Bronze",
    "_bronze_run_id": "Ejecución que ingirió el registro a Bronze",
    "_silver_run_id": "Última ejecución que escribió el registro en Silver",
    "_silver_ts": "Momento de la última escritura en Silver (UTC)",
    "_fecha_corte": "Fecha de corte de la ejecución que ingirió el registro",
    "_schema_hash": "Huella del encabezado del archivo (evolución de esquema)",
    "_row_hash": "SHA-256 del contenido del registro (duplicados exactos y cambios)",
    "_dq_advertencias": "Reglas de calidad de acción 'marcar' que el registro incumple",
    "_attr_hash": "Huella de los atributos historizados (detección de cambios SCD2)",
    "_event_key": "Huella de la clave de negocio del evento (absorbe reenvíos con otro evento_id)",
    "vigente_desde": "Inicio de vigencia de la versión del cliente (SCD2, inclusiva)",
    "vigente_hasta": "Fin de vigencia de la versión del cliente (SCD2, exclusiva; NULL = vigente)",
    "es_actual": "Indica la versión vigente del cliente (SCD2)",
    "estado_origen": "Estado anterior informado en el payload del evento",
    "estado_destino": "Estado nuevo informado en el payload del evento",
    "usuario_proceso": "Usuario o proceso que originó el evento",
}

PREFIJO_REF = {
    "_ref_": "La clave foránea existe en la tabla Silver padre",
    "_refcol_": "Columna traída de la tabla padre para validar reglas",
}

GOLD = {
    "poliza_360": {
        "comentario": "Vista integral de la póliza por fecha de corte: cliente, vigencia, estado, prima, recaudo, "
                      "siniestros y última actividad. Grano: póliza x fecha_corte. Ver docs/productos_gold.md",
        "columnas": {
            "fecha_corte": "Fecha de corte del snapshot",
            "poliza_id": "Identificador de la póliza (clave junto con fecha_corte)",
            "estado_al_corte": "Estado derivado de vigencia y cancelación: Vigente, Proxima a vencer, Vencida, "
                               "Renovada, Cancelada, No iniciada",
            "prima_emitida": "Prima de la vigencia contratada (COP)",
            "prima_recaudada": "Recaudos Aplicado con fecha de pago <= corte (COP)",
            "prima_pendiente": "Prima emitida menos recaudada; negativo = saldo a favor (COP)",
            "costo_incurrido": "Valor pagado + reserva de siniestros ocurridos <= corte (COP)",
            "fecha_ultima_actividad": "Fecha más reciente entre póliza, pagos, siniestros y movimientos",
        },
    },
    "siniestralidad_mensual": {
        "comentario": "Exposición, primas, costo incurrido y razón de siniestralidad por mes, producto y sucursal. "
                      "Grano: periodo x producto x sucursal x fecha_corte",
        "columnas": {
            "periodo": "Primer día del mes analizado",
            "polizas_expuestas": "Pólizas con al menos un día de vigencia efectiva en el mes",
            "prima_devengada": "Prima x días expuestos en el mes / días de vigencia (COP)",
            "costo_incurrido": "Pagado + reserva de siniestros ocurridos en el mes (COP)",
            "razon_siniestralidad": "Costo incurrido / prima devengada",
            "prima_pendiente_acumulada": "Acumulado de prima emitida menos recaudada hasta el mes (COP)",
        },
    },
    "discrepancias": {
        "comentario": "Hallazgos entre recaudos, pólizas, siniestros y movimientos por fecha de corte "
                      "(pagos sin póliza, siniestros sin póliza válida, movimientos fuera de vigencia, etc.)",
        "columnas": {"tipo_discrepancia": "Tipo de hallazgo", "severidad": "alta, media o baja",
                     "origen": "Tabla de la que proviene el hallazgo"},
    },
    "calidad_pipeline": {
        "comentario": "Resultados de reglas de calidad, postcondiciones y reconciliaciones por dataset y ejecución",
        "columnas": {"tipo_control": "REGLA o RECONCILIACION", "resultado": "OK, OK_DENTRO_UMBRAL, INFORMATIVA, "
                     "ADVERTENCIA, FALLA o DIFERENCIA", "es_ultima_ejecucion": "Última ejecución del dataset"},
    },
}

CLASIFICACION_PII = {"alta": "confidencial", "media": "interna-sensible"}
