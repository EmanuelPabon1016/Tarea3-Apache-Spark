#!/usr/bin/env python3
"""Tarea 3: análisis batch de consumo eléctrico con PySpark."""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, concat_ws, to_timestamp, to_date, hour,
    avg, count, max as spark_max, min as spark_min, sum as spark_sum,
    round as spark_round
)
 
MEASURES = [
    "Global_active_power", "Global_reactive_power", "Voltage",
    "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"
]
 
 
def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/household_power_consumption.txt"
    output_dir = "resultados_batch"
    spark = (SparkSession.builder.appName("Tarea3_Batch_Consumo_Electrico")
             .master("local[*]").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
 
    print("\n=== 1. CARGA DEL CONJUNTO DE DATOS ===")
    print("Archivo:", input_path)
    raw = (spark.read.option("header", True).option("sep", ";")
           .option("nullValue", "?").option("mode", "PERMISSIVE").csv(input_path))
    raw.printSchema()
    total_origen = raw.count()
    print("Filas de origen:", total_origen)
    expected = ["Date", "Time"] + MEASURES
    missing_cols = [name for name in expected if name not in raw.columns]
    if missing_cols:
        raise ValueError("Faltan columnas esperadas: " + str(missing_cols)
                         + ". Compruebe separador y cabecera del archivo.")
 
    print("\n=== 2. CONVERSIÓN Y CONTROL DE FALTANTES ===")
    df = raw.select(*expected)
    for name in MEASURES:
        cleaned = when(trim(col(name)).isin("", "?"), None).otherwise(trim(col(name)))
        df = df.withColumn(name, cleaned.cast("double"))
    df = (df.withColumn("timestamp", to_timestamp(concat_ws(" ", col("Date"), col("Time")), "d/M/yyyy H:mm:ss"))
            .withColumn("fecha", to_date(col("timestamp")))
            .withColumn("hora", hour(col("timestamp"))))
 
    missing_exprs = [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
                     for c in MEASURES]
    df.select(*missing_exprs).show(truncate=False)
 
    # Solo se exige timestamp y potencia activa válidos; otros campos ausentes
    # permanecen como nulos y avg() los omite, evitando descartar filas de más.
    valid = df.filter(
        col("timestamp").isNotNull()
        & col("Global_active_power").isNotNull()
        & (col("Global_active_power") >= 0)
    ).cache()
    total_validos = valid.count()
    descartados = total_origen - total_validos
    print("Filas válidas después del filtro:", total_validos)
    print("Filas excluidas por fecha/potencia activa inválidas:", descartados)
    if total_validos == 0:
        raise ValueError("No quedaron registros válidos; revise el formato de fecha y las reglas.")
 
    print("\n=== 3. MÉTRICAS GLOBALES ===")
    valid.agg(
        count("*").alias("mediciones_validas"),
        spark_round(avg("Global_active_power"), 4).alias("potencia_activa_media_kw"),
        spark_round(spark_max("Global_active_power"), 4).alias("potencia_activa_max_kw"),
        spark_round(spark_min("Global_active_power"), 4).alias("potencia_activa_min_kw"),
        spark_round(avg("Voltage"), 2).alias("voltaje_medio_v"),
        spark_round(avg("Global_intensity"), 3).alias("intensidad_media_a"),
        spark_round(avg("Sub_metering_1"), 3).alias("submedidor_1_medio_wh_min"),
        spark_round(avg("Sub_metering_2"), 3).alias("submedidor_2_medio_wh_min"),
        spark_round(avg("Sub_metering_3"), 3).alias("submedidor_3_medio_wh_min")
    ).show(truncate=False)
 
    print("\n=== 4. AGREGACIÓN POR HORA DEL DÍA ===")
    por_hora = (valid.groupBy("hora").agg(
        count("*").alias("mediciones"),
        spark_round(avg("Global_active_power"), 4).alias("potencia_activa_media_kw"),
        spark_round(avg("Voltage"), 2).alias("voltaje_medio_v"),
        spark_round(avg("Global_intensity"), 3).alias("intensidad_media_a")
    ).orderBy("hora").cache())
    por_hora.show(24, truncate=False)
 
    print("\n=== 5. AGREGACIÓN POR FECHA (PRIMERAS 15 FECHAS) ===")
    por_dia = (valid.groupBy("fecha").agg(
        count("*").alias("mediciones"),
        spark_round(avg("Global_active_power"), 4).alias("potencia_activa_media_kw"),
        spark_round(avg("Voltage"), 2).alias("voltaje_medio_v")
    ).orderBy("fecha").cache())
    por_dia.show(15, truncate=False)
 
    print("\n=== 6. BARRAS DE TEXTO: POTENCIA ACTIVA MEDIA POR HORA ===")
    filas = por_hora.select("hora", "potencia_activa_media_kw").collect()  # máximo 24 filas
    max_val = max((float(r["potencia_activa_media_kw"] or 0) for r in filas), default=1.0) or 1.0
    for r in filas:
        value = float(r["potencia_activa_media_kw"] or 0)
        bars = "#" * max(1, int(40 * value / max_val))
        print(f"{int(r['hora']):02d}:00 | {value:>7.3f} kW | {bars}")
 
    print("\n=== 7. EXPORTACIÓN DE RESULTADOS ===")
    os.makedirs(output_dir, exist_ok=True)
    por_hora.write.mode("overwrite").option("header", True).csv(os.path.join(output_dir, "por_hora"))
    por_dia.write.mode("overwrite").option("header", True).csv(os.path.join(output_dir, "por_dia"))
    print("Resultados horarios:", os.path.join(output_dir, "por_hora"))
    print("Resultados diarios:", os.path.join(output_dir, "por_dia"))
    print("Nota: cada ruta CSV es una carpeta Spark con archivos part-*.csv.")
    valid.unpersist(); por_hora.unpersist(); por_dia.unpersist()
    spark.stop()
 
 
if __name__ == "__main__":
    main()
