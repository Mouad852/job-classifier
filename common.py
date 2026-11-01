"""Configuration partagée par tous les jobs Spark."""
import os
import sys
from pyspark.sql import SparkSession

# Les workers Spark doivent utiliser le m\u00eame Python que le driver. Cela \u00e9vite
# qu'un Python syst\u00e8me incompatible soit choisi lorsque le projet tourne dans un venv.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "job_offers")
# Doit correspondre à la version de pyspark installée (3.5.1) et à Scala 2.12
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
MODEL_PATH = os.getenv("MODEL_PATH", "models/job_classifier")


def get_spark(app_name, kafka=True):
    builder = (SparkSession.builder.appName(app_name).master("local[*]")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.pyspark.python", os.environ["PYSPARK_PYTHON"]))
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark):
    """Flux Kafka -> DataFrame (text, timestamp)."""
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           # "latest" : lancer Spark AVANT le producer. Mettre "earliest" pour tout relire.
           .option("startingOffsets", os.getenv("STARTING_OFFSETS", "latest"))
           .option("failOnDataLoss", "false")
           .load())
    return (raw.selectExpr("CAST(value AS STRING) AS text", "timestamp")
               .filter("length(trim(text)) > 0"))
