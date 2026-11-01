"""LEVEL 1 : Kafka -> Spark Structured Streaming -> classification par mots-clés -> console."""
import re
from pyspark.sql import functions as F
from common import get_spark, read_kafka

KEYWORDS = {
    "Data": ["data", "sql", "spark", "hadoop", "etl", "bi", "power bi", "tableau", "big data",
             "machine learning", "pandas", "airflow", "data lake", "data warehouse", "hive", "snowflake"],
    "Développement": ["développeur", "developer", "java", "spring", "react", "angular", "vue.js",
                      "javascript", "typescript", "node.js", "php", "symfony", "laravel", "api rest",
                      "front-end", "back-end", "full stack", "flutter", "html", "css"],
    "Sécurité": ["sécurité", "security", "cybersécurité", "soc", "siem", "pentest", "firewall",
                 "iso 27001", "vulnérabilités", "owasp", "edr", "cryptographie", "rssi", "intrusion"],
    "Cloud": ["cloud", "aws", "azure", "gcp", "kubernetes", "docker", "terraform", "ansible",
              "devops", "ci/cd", "jenkins", "helm", "sre", "infrastructure as code"],
}


def count_expr(words):
    # Mot entier, insensible à la casse, compatible accents (évite que "soc" matche "social")
    alt = "|".join(re.escape(w) for w in words)
    pattern = r"(?iu)(?<![\p{L}\p{N}])(?:" + alt + r")(?![\p{L}\p{N}])"
    return F.regexp_count(F.col("text"), F.lit(pattern))


spark = get_spark("Level1-Keywords")
df = read_kafka(spark)

names = list(KEYWORDS)
for i, dom in enumerate(names):
    df = df.withColumn(f"s{i}", count_expr(KEYWORDS[dom]))

best = F.greatest(*[F.col(f"s{i}") for i in range(len(names))])
label = F.lit("Autre")
for i, dom in reversed(list(enumerate(names))):     # en cas d'égalité, le 1er domaine gagne
    label = F.when(F.col(f"s{i}") == best, F.lit(dom)).otherwise(label)
label = F.when(best == 0, F.lit("Autre")).otherwise(label)

result = df.select(F.col("text"), label.alias("domaine"),
                   *[F.col(f"s{i}").alias(f"score_{i}") for i in range(len(names))])

query = (result.writeStream.outputMode("append").format("console")
         .option("truncate", "false").option("numRows", 80)
         .option("checkpointLocation", "checkpoints/level1")
         .start())
print("Score_0..3 =", names)
query.awaitTermination()
