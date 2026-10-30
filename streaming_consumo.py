#!/usr/bin/env python3
"""Consume mediciones sintéticas desde Kafka y resume consumo con Spark Structured Streaming."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, round as spark_round
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
 
TOPIC = os.environ.get("KAFKA_TOPIC", "consumo_electrico_tiempo_real")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("global_active_power", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("consumption_level", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("source", StringType(), True),
])
 
 
def main():
    spark = (SparkSession.builder.appName("Tarea3_Kafka_SparkStreaming_Consumo")
             .master("local[*]").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print("Consumiendo topic:", TOPIC, "desde", BOOTSTRAP, flush=True)
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())
    parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("event"))
              .select("event.*")
              .filter(col("event_id").isNotNull() & col("consumption_level").isNotNull())
              .filter(col("global_active_power").isNotNull() & (col("global_active_power") >= 0))
              .filter(col("voltage").isNotNull() & (col("voltage") > 0)))
    resumen = parsed.groupBy("consumption_level").agg(
        count("*").alias("eventos"),
        spark_round(avg("global_active_power"), 3).alias("potencia_activa_promedio_kw"),
        spark_round(avg("voltage"), 2).alias("voltaje_promedio_v")
    )
    query = (resumen.writeStream.outputMode("complete").format("console")
             .option("truncate", "false").option("numRows", 20)
             .option("checkpointLocation", "/tmp/checkpoint_tarea3_consumo")
             .start())
    query.awaitTermination()
 
 
if __name__ == "__main__":
    main()
