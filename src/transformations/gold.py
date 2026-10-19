from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ESTADOS_SINIESTRO_ABIERTO = ("Abierto", "En investigacion", "Reactivado")
DEC = "decimal(18,2)"


def _corte(c: date):
    return F.lit(c).cast("date")


def _dec(col):
    return F.coalesce(col, F.lit(0)).cast(DEC)


def clientes_al_corte(clientes: DataFrame, corte: date) -> DataFrame:
    """Versión SCD2 del cliente vigente en la fecha de corte (si no hay, la versión más antigua)."""
    c = _corte(corte)
    vigente = (F.col("vigente_desde") <= c) & (F.col("vigente_hasta").isNull() | (F.col("vigente_hasta") > c))
    w = Window.partitionBy("cliente_id").orderBy(F.when(vigente, 0).otherwise(1), F.col("vigente_desde").asc())
    return clientes.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")


def polizas_con_fin_efectivo(polizas: DataFrame, movimientos: DataFrame) -> DataFrame:
    """Agrega `fecha_cancelacion` y `fecha_fin_efectiva`.

    Supuesto (docs/productos_gold.md): el core no informa la fecha de cancelación. Para pólizas en
    estado Cancelada se toma el primer evento CANCELACION dentro de la vigencia; si no hay, la
    `fecha_actualizacion` si cae dentro de la vigencia; si tampoco, el fin de vigencia.
    """
    canc = (
        movimientos.where((F.col("entidad") == "POLIZA") & (F.col("tipo_evento") == "CANCELACION"))
        .select(F.col("id_entidad").alias("poliza_id"), F.col("fecha_evento").alias("__f_canc"))
    )
    primera = (
        polizas.select("poliza_id", "fecha_inicio_vigencia", "fecha_fin_vigencia")
        .join(canc, "poliza_id")
        .where(F.col("__f_canc").between(F.col("fecha_inicio_vigencia"), F.col("fecha_fin_vigencia")))
        .groupBy("poliza_id").agg(F.min("__f_canc").alias("__f_canc_evento"))
    )
    act_en_vigencia = F.when(
        F.col("fecha_actualizacion").between(F.col("fecha_inicio_vigencia"), F.col("fecha_fin_vigencia")),
        F.col("fecha_actualizacion"),
    )
    return (
        polizas.join(primera, "poliza_id", "left")
        .withColumn("fecha_cancelacion", F.when(
            F.col("estado") == "Cancelada",
            F.coalesce(F.col("__f_canc_evento"), act_en_vigencia, F.col("fecha_fin_vigencia"))))
        .withColumn("fecha_fin_efectiva", F.coalesce(F.col("fecha_cancelacion"), F.col("fecha_fin_vigencia")))
        .drop("__f_canc_evento")
    )


def estado_al_corte(corte: date, dias_proximo: int):
    """Estado de la póliza en la fecha de corte, derivado de vigencia y cancelación."""
    c = _corte(corte)
    return (
        F.when((F.col("estado") == "Cancelada") & (F.col("fecha_cancelacion") <= c), F.lit("Cancelada"))
        .when(F.col("fecha_inicio_vigencia") > c, F.lit("No iniciada"))
        .when(F.col("fecha_fin_vigencia") < c,
              F.when(F.col("estado") == "Renovada", F.lit("Renovada")).otherwise(F.lit("Vencida")))
        .when(F.datediff(F.col("fecha_fin_vigencia"), c) <= dias_proximo, F.lit("Proxima a vencer"))
        .otherwise(F.lit("Vigente"))
    )


def build_poliza_360(
    polizas: DataFrame, clientes: DataFrame, recaudos: DataFrame, siniestros: DataFrame,
    movimientos: DataFrame, corte: date, dias_proximo: int = 30,
) -> DataFrame:
    c = _corte(corte)
    mov = movimientos.where(F.col("fecha_evento") <= c)
    pol = polizas_con_fin_efectivo(polizas, mov)

    cli = clientes_al_corte(clientes, corte).select(
        "cliente_id", F.col("segmento").alias("segmento_cliente"), F.col("ciudad").alias("ciudad_cliente"),
        F.col("tipo_documento").alias("tipo_documento_cliente"),
    )
    rec = recaudos.where(F.col("fecha_pago") <= c).groupBy("poliza_id").agg(
        F.sum(F.when(F.col("estado_transaccion") == "Aplicado", F.col("valor"))).alias("prima_recaudada"),
        F.sum(F.when(F.col("estado_transaccion") == "Pendiente", F.col("valor"))).alias("recaudo_pendiente_confirmar"),
        F.sum(F.when(F.col("estado_transaccion") == "Reversado", F.col("valor"))).alias("recaudo_reversado"),
        F.count(F.lit(1)).alias("num_recaudos"),
        F.max(F.when(F.col("estado_transaccion") == "Aplicado", F.col("fecha_pago"))).alias("fecha_ultimo_pago"),
        F.max("fecha_pago").alias("__f_rec"),
        F.sum(F.array_contains("_dq_advertencias", "recaudos.CON.pago_en_vigencia").cast("int"))
        .alias("num_pagos_fuera_vigencia"),
    )
    sin = siniestros.where(F.col("fecha_ocurrencia") <= c).groupBy("poliza_id").agg(
        F.count(F.lit(1)).alias("num_siniestros"),
        F.sum(F.col("estado").isin(list(ESTADOS_SINIESTRO_ABIERTO)).cast("int")).alias("num_siniestros_abiertos"),
        F.sum("valor_pagado").alias("valor_pagado_siniestros"),
        F.sum("valor_reserva").alias("valor_reserva_siniestros"),
        F.max("fecha_actualizacion").alias("__f_sin"),
        F.sum(F.array_contains("_dq_advertencias", "siniestros.CON.ocurrencia_en_vigencia").cast("int"))
        .alias("num_siniestros_fuera_vigencia"),
    )
    mv = mov.where(F.col("entidad") == "POLIZA").groupBy(F.col("id_entidad").alias("poliza_id")).agg(
        F.count(F.lit(1)).alias("num_movimientos"),
        F.max("fecha_evento").alias("__f_mov"),
        F.max_by("tipo_evento", F.struct("fecha_evento", "secuencia")).alias("ultimo_evento"),
    )

    df = (pol.join(cli, "cliente_id", "left").join(rec, "poliza_id", "left")
          .join(sin, "poliza_id", "left").join(mv, "poliza_id", "left"))

    f_pol = F.when(F.col("fecha_actualizacion") <= c, F.col("fecha_actualizacion"))
    ultima = F.greatest(f_pol, F.col("__f_rec"), F.col("__f_sin"), F.col("__f_mov"))
    tipo_ultima = (
        F.when(ultima.isNull(), F.lit(None))
        .when(ultima == F.col("__f_mov"), F.concat(F.lit("MOVIMIENTO "), F.col("ultimo_evento")))
        .when(ultima == F.col("__f_sin"), F.lit("SINIESTRO"))
        .when(ultima == F.col("__f_rec"), F.lit("RECAUDO"))
        .otherwise(F.lit("ACTUALIZACION POLIZA"))
    )
    est = estado_al_corte(corte, dias_proximo)
    incurrido = _dec(F.col("valor_pagado_siniestros")) + _dec(F.col("valor_reserva_siniestros"))

    return df.select(
        F.lit(corte).cast("date").alias("fecha_corte"),
        "poliza_id", "cliente_id",
        F.coalesce(F.col("_ref_cliente"), F.lit(False)).alias("cliente_identificado"),
        F.coalesce(F.col("segmento_cliente"), F.lit("NO_IDENTIFICADO")).alias("segmento_cliente"),
        F.coalesce(F.col("ciudad_cliente"), F.lit("NO_IDENTIFICADO")).alias("ciudad_cliente"),
        "tipo_documento_cliente", "producto", "sucursal", "canal",
        "fecha_inicio_vigencia", "fecha_fin_vigencia", "fecha_cancelacion",
        (F.datediff("fecha_fin_vigencia", "fecha_inicio_vigencia") + 1).alias("dias_vigencia"),
        F.col("estado").alias("estado_core"),
        est.alias("estado_al_corte"),
        F.when(F.col("fecha_fin_vigencia") >= c, F.datediff("fecha_fin_vigencia", c)).alias("dias_para_vencer"),
        (((F.col("estado") == "Vigente") & est.isin("Vencida", "Renovada"))
         | ((F.col("estado") == "Vencida") & est.isin("Vigente", "Proxima a vencer"))).alias("estado_inconsistente"),
        _dec(F.col("prima")).alias("prima_emitida"),
        _dec(F.col("prima_recaudada")).alias("prima_recaudada"),
        (_dec(F.col("prima")) - _dec(F.col("prima_recaudada"))).cast(DEC).alias("prima_pendiente"),
        _dec(F.col("recaudo_pendiente_confirmar")).alias("recaudo_pendiente_confirmar"),
        _dec(F.col("recaudo_reversado")).alias("recaudo_reversado"),
        F.coalesce(F.col("num_recaudos"), F.lit(0)).alias("num_recaudos"),
        "fecha_ultimo_pago",
        F.coalesce(F.col("num_pagos_fuera_vigencia"), F.lit(0)).alias("num_pagos_fuera_vigencia"),
        F.coalesce(F.col("num_siniestros"), F.lit(0)).alias("num_siniestros"),
        F.coalesce(F.col("num_siniestros_abiertos"), F.lit(0)).alias("num_siniestros_abiertos"),
        _dec(F.col("valor_pagado_siniestros")).alias("valor_pagado_siniestros"),
        _dec(F.col("valor_reserva_siniestros")).alias("valor_reserva_siniestros"),
        incurrido.cast(DEC).alias("costo_incurrido"),
        F.when(F.col("prima") > 0, F.round(incurrido / F.col("prima"), 4).cast("double")).alias("siniestralidad_poliza"),
        F.coalesce(F.col("num_siniestros_fuera_vigencia"), F.lit(0)).alias("num_siniestros_fuera_vigencia"),
        F.coalesce(F.col("num_movimientos"), F.lit(0)).alias("num_movimientos"),
        "ultimo_evento",
        ultima.alias("fecha_ultima_actividad"),
        tipo_ultima.alias("tipo_ultima_actividad"),
        F.col("_dq_advertencias").alias("advertencias_calidad"),
        F.col("_silver_run_id").alias("_silver_run_id_poliza"),
    )


def exposicion_mensual(polizas_fin: DataFrame, corte: date) -> DataFrame:
    """Explota cada póliza en sus meses de exposición hasta la fecha de corte (o su fin efectivo).

    prima_devengada_mes = prima x días expuestos en el mes / días de vigencia contratada.
    """
    c = _corte(corte)
    p = (
        polizas_fin.where(F.col("fecha_inicio_vigencia") <= c)
        .withColumn("__fin_exp", F.least(F.col("fecha_fin_efectiva"), c))
        .where(F.col("__fin_exp") >= F.col("fecha_inicio_vigencia"))
        .withColumn("__dias_vig", F.datediff("fecha_fin_vigencia", "fecha_inicio_vigencia") + 1)
        .withColumn("periodo", F.explode(F.sequence(
            F.trunc("fecha_inicio_vigencia", "month"), F.trunc("__fin_exp", "month"), F.expr("INTERVAL 1 MONTH"))))
    )
    dias = F.datediff(F.least(F.col("__fin_exp"), F.last_day("periodo")),
                      F.greatest(F.col("fecha_inicio_vigencia"), F.col("periodo"))) + 1
    return p.select(
        "poliza_id", "producto", "sucursal", F.col("periodo").cast("date").alias("periodo"),
        dias.alias("dias_expuestos"),
        (F.col("prima") * dias / F.col("__dias_vig")).alias("prima_devengada"),
    )


def build_siniestralidad_mensual(
    polizas: DataFrame, siniestros: DataFrame, recaudos: DataFrame, movimientos: DataFrame, corte: date
) -> DataFrame:
    c = _corte(corte)
    pol = polizas_con_fin_efectivo(polizas, movimientos.where(F.col("fecha_evento") <= c))
    keys = ["periodo", "producto", "sucursal"]
    dims = pol.select("poliza_id", "producto", "sucursal")

    expo = exposicion_mensual(pol, corte).groupBy(*keys).agg(
        F.countDistinct("poliza_id").alias("polizas_expuestas"),
        F.round(F.sum("dias_expuestos") / F.lit(365.25), 4).alias("exposicion_anios_poliza"),
        F.sum("prima_devengada").alias("prima_devengada"),
    )
    emit = pol.where(F.col("fecha_inicio_vigencia") <= c).groupBy(
        F.trunc("fecha_inicio_vigencia", "month").cast("date").alias("periodo"), "producto", "sucursal"
    ).agg(F.count(F.lit(1)).alias("polizas_emitidas"), F.sum("prima").alias("prima_emitida"))
    recm = (
        recaudos.where((F.col("estado_transaccion") == "Aplicado") & (F.col("fecha_pago") <= c))
        .join(dims, "poliza_id")
        .groupBy(F.trunc("fecha_pago", "month").cast("date").alias("periodo"), "producto", "sucursal")
        .agg(F.sum("valor").alias("prima_recaudada"))
    )
    sinm = (
        siniestros.where(F.col("fecha_ocurrencia") <= c).join(dims, "poliza_id")
        .groupBy(F.trunc("fecha_ocurrencia", "month").cast("date").alias("periodo"), "producto", "sucursal")
        .agg(F.count(F.lit(1)).alias("siniestros"), F.sum("valor_pagado").alias("valor_pagado"),
             F.sum("valor_reserva").alias("valor_reserva"))
    )
    base = (expo.select(*keys).unionByName(emit.select(*keys)).unionByName(recm.select(*keys))
            .unionByName(sinm.select(*keys)).distinct())
    df = (base.join(expo, keys, "left").join(emit, keys, "left")
          .join(recm, keys, "left").join(sinm, keys, "left"))

    incurrido = _dec(F.col("valor_pagado")) + _dec(F.col("valor_reserva"))
    devengada = _dec(F.col("prima_devengada"))
    w_acum = Window.partitionBy("producto", "sucursal").orderBy("periodo") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        df.select(
            F.lit(corte).cast("date").alias("fecha_corte"), *keys,
            F.coalesce(F.col("polizas_expuestas"), F.lit(0)).alias("polizas_expuestas"),
            F.coalesce(F.col("exposicion_anios_poliza"), F.lit(0.0)).alias("exposicion_anios_poliza"),
            F.coalesce(F.col("polizas_emitidas"), F.lit(0)).alias("polizas_emitidas"),
            _dec(F.col("prima_emitida")).alias("prima_emitida"),
            devengada.alias("prima_devengada"),
            _dec(F.col("prima_recaudada")).alias("prima_recaudada"),
            F.coalesce(F.col("siniestros"), F.lit(0)).alias("siniestros"),
            _dec(F.col("valor_pagado")).alias("valor_pagado"),
            _dec(F.col("valor_reserva")).alias("valor_reserva"),
            incurrido.cast(DEC).alias("costo_incurrido"),
            F.when(devengada > 0, F.round(incurrido / devengada, 4).cast("double")).alias("razon_siniestralidad"),
        )
        .withColumn("prima_pendiente_acumulada",
                    F.sum(F.col("prima_emitida") - F.col("prima_recaudada")).over(w_acum).cast(DEC))
    )


def build_discrepancias(
    polizas: DataFrame, recaudos: DataFrame, siniestros: DataFrame, movimientos: DataFrame,
    cuarentena: DataFrame, poliza_360: DataFrame, corte: date,
) -> DataFrame:
    """Hallazgos que responden "¿qué pólizas presentan pagos sin correspondencia, siniestros sin
    póliza válida o movimientos fuera de vigencia?". Una fila por hallazgo y registro."""
    c = _corte(corte)

    def fila(df, tipo, severidad, entidad, id_col, poliza_col, fecha_col, valor_col, detalle, regla, origen):
        return df.select(
            F.lit(tipo).alias("tipo_discrepancia"), F.lit(severidad).alias("severidad"),
            F.lit(entidad).alias("entidad"), F.col(id_col).cast("string").alias("id_registro"),
            F.col(poliza_col).cast("string").alias("poliza_id"), F.col(fecha_col).cast("date").alias("fecha_referencia"),
            (F.col(valor_col).cast(DEC) if valor_col else F.lit(None).cast(DEC)).alias("valor"),
            detalle.alias("detalle"), F.lit(regla).alias("regla_id"), F.lit(origen).alias("origen"),
        )

    def desde_cuarentena(ds, regla, tipo, id_json, valor_json, fecha_json, detalle):
        q = cuarentena.where((F.col("dataset") == ds) & F.array_contains("reglas_incumplidas", regla))
        q = q.select(
            F.get_json_object("registro", f"$.{id_json}").alias("__id"),
            F.get_json_object("registro", "$.poliza_id").alias("__pol"),
            F.get_json_object("registro", f"$.{fecha_json}").alias("__f_txt"),
            F.expr(f"try_cast(get_json_object(registro, '$.{valor_json}') AS {DEC})").alias("__v"),
        ).withColumn("__f", F.coalesce(*[F.try_to_timestamp(F.col("__f_txt"), F.lit(f)).cast("date")
                                         for f in ("yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd")]))
        return fila(q, tipo, "alta", ds.upper().rstrip("S"), "__id", "__pol", "__f", "__v", F.lit(detalle), regla,
                    "silver_cuarentena")

    adv = "_dq_advertencias"
    partes = [
        desde_cuarentena("recaudos", "recaudos.RI.poliza", "PAGO_SIN_POLIZA", "recaudo_id", "valor", "fecha_pago",
                         "Pago a una póliza inexistente (en cuarentena)"),
        desde_cuarentena("siniestros", "siniestros.RI.poliza", "SINIESTRO_SIN_POLIZA", "siniestro_id",
                         "valor_pagado", "fecha_ocurrencia", "Siniestro sin póliza válida (en cuarentena)"),
        fila(recaudos.where(F.col("_ref_poliza_cuarentena") & (F.col("fecha_pago") <= c)),
             "PAGO_A_POLIZA_EN_CUARENTENA", "media", "RECAUDO", "recaudo_id", "poliza_id", "fecha_pago", "valor",
             F.lit("La póliza existe pero fue retenida en cuarentena"), "recaudos.RI.poliza_valida", "silver_recaudos"),
        fila(siniestros.where(F.col("_ref_poliza_cuarentena") & (F.col("fecha_ocurrencia") <= c)),
             "SINIESTRO_DE_POLIZA_EN_CUARENTENA", "media", "SINIESTRO", "siniestro_id", "poliza_id",
             "fecha_ocurrencia", "valor_pagado", F.lit("La póliza existe pero fue retenida en cuarentena"),
             "siniestros.RI.poliza_valida", "silver_siniestros"),
        fila(recaudos.where(F.array_contains(adv, "recaudos.CON.pago_en_vigencia") & (F.col("fecha_pago") <= c)),
             "PAGO_FUERA_DE_VIGENCIA", "baja", "RECAUDO", "recaudo_id", "poliza_id", "fecha_pago", "valor",
             F.lit("Fecha de pago fuera de la vigencia de la póliza"), "recaudos.CON.pago_en_vigencia",
             "silver_recaudos"),
        fila(siniestros.where(F.array_contains(adv, "siniestros.CON.ocurrencia_en_vigencia")
                              & (F.col("fecha_ocurrencia") <= c)),
             "SINIESTRO_FUERA_DE_VIGENCIA", "alta", "SINIESTRO", "siniestro_id", "poliza_id", "fecha_ocurrencia",
             "valor_pagado", F.lit("Ocurrencia fuera de la vigencia: posible siniestro no cubierto"),
             "siniestros.CON.ocurrencia_en_vigencia", "silver_siniestros"),
        fila(movimientos.where((F.col("entidad") == "POLIZA") & (F.col("fecha_evento") <= c))
             .join(polizas.select(F.col("poliza_id").alias("id_entidad"), "fecha_inicio_vigencia",
                                  "fecha_fin_vigencia"), "id_entidad")
             .where(~F.col("fecha_evento").between(F.col("fecha_inicio_vigencia"), F.col("fecha_fin_vigencia"))
                    & F.col("tipo_evento").isin("CAMBIO_ESTADO", "CANCELACION")),
             "MOVIMIENTO_FUERA_DE_VIGENCIA", "media", "MOVIMIENTO", "evento_id", "id_entidad", "fecha_evento", None,
             F.concat(F.lit("Evento "), F.col("tipo_evento"), F.lit(" fuera de la vigencia")), None,
             "silver_movimientos"),
        fila(polizas.where(~F.col("_ref_cliente")), "POLIZA_SIN_CLIENTE", "media", "POLIZA", "poliza_id",
             "poliza_id", "fecha_inicio_vigencia", "prima", F.lit("El tomador no existe en clientes"),
             "polizas.RI.cliente", "silver_polizas"),
        fila(poliza_360.where(F.col("prima_recaudada") > F.col("prima_emitida")),
             "RECAUDO_EXCEDE_PRIMA", "media", "POLIZA", "poliza_id", "poliza_id", "fecha_ultimo_pago",
             "prima_recaudada", F.concat(F.lit("Recaudado supera la prima emitida en "),
                                         (F.col("prima_recaudada") - F.col("prima_emitida")).cast("string")),
             None, "gold_poliza_360"),
    ]
    out = partes[0]
    for p in partes[1:]:
        out = out.unionByName(p)
    return out.select(F.lit(corte).cast("date").alias("fecha_corte"), *out.columns)


def build_calidad_pipeline(calidad: DataFrame, reconciliacion: DataFrame) -> DataFrame:
    """Une reglas de calidad y reconciliaciones en un solo producto consultable por ejecución."""
    reglas = calidad.select(
        "run_id", "fecha_corte", "pipeline", "capa", "dataset", F.lit("REGLA").alias("tipo_control"),
        F.col("regla_id").alias("control_id"), "dimension", "severidad", "accion", "descripcion",
        "registros_evaluados", "registros_fallidos", "pct_fallo", "umbral_pct",
        F.lit(None).cast("decimal(24,2)").alias("valor_origen"), F.lit(None).cast("decimal(24,2)").alias("valor_destino"),
        F.lit(None).cast("decimal(24,2)").alias("diferencia"), "resultado", "fecha_evaluacion",
    )
    recon = reconciliacion.select(
        "run_id", "fecha_corte", "pipeline",
        F.concat_ws("->", "etapa_origen", "etapa_destino").alias("capa"), "dataset",
        F.lit("RECONCILIACION").alias("tipo_control"),
        F.concat_ws(".", "dataset", F.lit("REC"), "metrica").alias("control_id"),
        F.lit("consistencia").alias("dimension"),
        F.lit("bloqueo").alias("severidad"), F.lit(None).cast("string").alias("accion"),
        F.coalesce(F.col("detalle"), F.concat(F.lit("Reconciliación de "), F.col("metrica"))).alias("descripcion"),
        F.lit(None).cast("bigint").alias("registros_evaluados"), F.lit(None).cast("bigint").alias("registros_fallidos"),
        F.lit(None).cast("double").alias("pct_fallo"), F.lit(None).cast("double").alias("umbral_pct"),
        "valor_origen", "valor_destino", "diferencia", "resultado", "fecha_evaluacion",
    )
    unido = reglas.unionByName(recon)
    ultima = (unido.groupBy("dataset").agg(F.max_by("run_id", "fecha_evaluacion").alias("__ultimo_run")))
    return (unido.join(ultima, "dataset", "left")
            .withColumn("es_ultima_ejecucion", F.col("run_id") == F.col("__ultimo_run")).drop("__ultimo_run"))
