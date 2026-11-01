"""LEVEL 3 : Kafka -> Spark (MLlib) -> Elasticsearch (-> Kibana)."""
from datetime import datetime, timezone

from elasticsearch import Elasticsearch, helpers
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from common import MODEL_PATH, get_spark, read_kafka

ES_URL, INDEX = "http://localhost:9200", "job_offers"
es = Elasticsearch(ES_URL)
if not es.indices.exists(index=INDEX):
    es.indices.create(index=INDEX, mappings={"properties": {
        "@timestamp": {"type": "date"},
        "text": {"type": "text"},
        "domaine": {"type": "keyword"},
        "confiance": {"type": "float"},
        "source": {"type": "keyword"},
    }})

spark = get_spark("Level3-Elasticsearch")
model = PipelineModel.load(MODEL_PATH)

scored = (model.transform(read_kafka(spark))
          .select("text", "domaine", F.array_max(vector_to_array("probability")).alias("confiance")))


def write_batch(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    actions = [{"_index": INDEX,
                "_source": {"@timestamp": now, "text": r["text"], "domaine": r["domaine"],
                            "confiance": float(r["confiance"]), "source": "kafka"}}
               for r in rows]
    helpers.bulk(es, actions)
    print(f"batch {batch_id} : {len(rows)} offres indexées")
    batch_df.select("text", "domaine").show(truncate=70)


query = (scored.writeStream.foreachBatch(write_batch)
         .option("checkpointLocation", "checkpoints/level3").start())
query.awaitTermination()
