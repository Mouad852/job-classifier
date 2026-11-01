"""LEVEL 2 (étape batch) : entraîne un pipeline Spark MLlib et le sauvegarde."""
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (HashingTF, IDF, IndexToString, RegexTokenizer,
                                StopWordsRemover, StringIndexer)
from common import MODEL_PATH, get_spark

spark = get_spark("TrainJobClassifier", kafka=False)
data = spark.read.csv("data/train_jobs.csv", header=True, inferSchema=True, multiLine=False)
data = data.dropna()

indexer = StringIndexer(inputCol="label", outputCol="label_idx").fit(data)
data = indexer.transform(data)
train, test = data.randomSplit([0.8, 0.2], seed=42)

stop_words = (StopWordsRemover.loadDefaultStopWords("french")
              + StopWordsRemover.loadDefaultStopWords("english"))

pipeline = Pipeline(stages=[
    # garde les lettres accentuées, chiffres, # et + (c++, c#)
    RegexTokenizer(inputCol="text", outputCol="tokens", pattern=r"[^\p{L}\p{N}#+]+"),
    StopWordsRemover(inputCol="tokens", outputCol="filtered", stopWords=stop_words),
    HashingTF(inputCol="filtered", outputCol="tf", numFeatures=1 << 14),
    IDF(inputCol="tf", outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label_idx", maxIter=50, regParam=0.01),
    IndexToString(inputCol="prediction", outputCol="domaine", labels=indexer.labels),
])

model = pipeline.fit(train)
pred = model.transform(test)

for metric in ("accuracy", "f1"):
    score = MulticlassClassificationEvaluator(labelCol="label_idx", predictionCol="prediction",
                                              metricName=metric).evaluate(pred)
    print(f"{metric} = {score:.4f}")
print("Matrice de confusion (label x domaine prédit) :")
pred.groupBy("label").pivot("domaine").count().na.fill(0).show()

model.write().overwrite().save(MODEL_PATH)
print("Modèle sauvegardé dans", MODEL_PATH)
