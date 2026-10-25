from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, round as sround
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

spark = SparkSession.builder \
    .appName("CovidKafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de los eventos que envía el productor
schema = StructType([
    StructField("id_caso", IntegerType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType()),
    StructField("edad", IntegerType()),
    StructField("sexo", StringType()),
    StructField("estado", StringType()),
    StructField("recuperado", StringType()),
    StructField("tipo_contagio", StringType()),
    StructField("timestamp", LongType())
])

# Lectura del stream desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "covid_data") \
    .option("startingOffsets", "latest") \
    .load()

# Parseo del JSON y conversión del timestamp (epoch) a tipo fecha
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp"))

# Análisis 1: casos y edad promedio por departamento en ventanas de 1 minuto
stats_departamento = parsed_df \
    .groupBy(window(col("event_time"), "1 minute"), col("departamento")) \
    .agg(count("*").alias("casos"),
         sround(avg("edad"), 2).alias("edad_promedio")) \
    .orderBy(col("casos").desc())

# Análisis 2: casos por sexo y estado (Leve, Moderado, Grave, Fallecido, etc.)
stats_sexo_estado = parsed_df \
    .groupBy(col("sexo"), col("estado")) \
    .agg(count("*").alias("casos")) \
    .orderBy(col("casos").desc())

q1 = stats_departamento.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

q2 = stats_sexo_estado.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

spark.streams.awaitAnyTermination()
