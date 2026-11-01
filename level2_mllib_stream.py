"""LEVEL 2 : Kafka -> Spark Structured Streaming -> modèle MLlib -> console."""
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from common import MODEL_PATH, get_spark, read_kafka

spark = get_spark("Level2-MLlib")
model = PipelineModel.load(MODEL_PATH)

df = read_kafka(spark)
scored = (model.transform(df)
          .select("text", "domaine",
                  F.round(F.array_max(vector_to_array("probability")), 3).alias("confiance")))

query = (scored.writeStream.outputMode("append").format("console")
         .option("truncate", "false").option("numRows", 80)
         .option("checkpointLocation", "checkpoints/level2")
         .start())
query.awaitTermination()
