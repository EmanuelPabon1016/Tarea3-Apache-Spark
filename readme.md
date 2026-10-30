# Tarea 3 - Procesamiento de datos con Apache Spark
 
## Objetivo
Procesar mediciones públicas de consumo eléctrico doméstico con PySpark y demostrar procesamiento de eventos simulados con Apache Kafka y Spark Structured Streaming.
procesamiento de eventos simulados con Apache Kafka y Spark Structured Streaming.
 
## Fuente de datos
UCI Machine Learning Repository — Individual Household Electric Power Consumption:
https://archive.ics.uci.edu/dataset/235/individual%2Bhousehold%2Belectric%2Bpower%2Bconsumption
Archivo utilizado: household_power_consumption.txt (extraído del ZIP oficial de UCI).
 
## Requisitos
- Python compatible con la versión de Spark instalada.
- Apache Spark / PySpark instalados y operativos.
- Apache Kafka operativo según la configuración del laboratorio.
- Conector Spark-Kafka compatible con la versión de Spark y Scala.
 
## Ejecución batch
mkdir -p data resultados_batch
wget -O data/household_power_consumption.zip "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"
unzip -o data/household_power_consumption.zip -d data
spark-submit batch_consumo.py data/household_power_consumption.txt
## Ejecución en tiempo real
1. Iniciar Kafka según la configuración del laboratorio.
2. Crear el topic `consumo_electrico_tiempo_real`.
3. Iniciar `streaming_consumo.py` con `spark-submit` y el conector compatible con Spark/Scala locales.
4. En otra terminal, ejecutar `python3 generador_eventos.py` para publicar mediciones sintéticas.
## Limitaciones
Los eventos de streaming son sintéticos.
