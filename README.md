# Classification d'offres d'emploi : Kafka + Spark Structured Streaming

Domaines : **Data, Développement, Sécurité, Cloud**

```
fichier / Mastodon -> Producer (Python) -> Kafka (topic job_offers) -> Spark Structured Streaming
                                                                        |-> console (Level 1, 2)
                                                                        |-> Elasticsearch -> Kibana (Level 3)
```

## Prérequis
- Docker + Docker Compose
- Python 3.9 à 3.11 et **Java 11 ou 17** (indispensable pour PySpark)
- Windows : utilisez WSL2 (PySpark en natif demande winutils, source d'ennuis)

## Installation
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
docker compose up -d                 # Kafka
python generate_dataset.py           # crée data/train_jobs.csv et data/jobs_stream.txt
```

## Level 1 : mots-clés
Terminal A (Spark d'abord, car startingOffsets = latest) :
```bash
python level1_keywords_stream.py
```
Terminal B :
```bash
python producer.py --delay 1
```
Les offres s'affichent dans le terminal A avec leur domaine et les scores (score_0 = Data, 1 = Développement, 2 = Sécurité, 3 = Cloud).

## Level 2 : MLlib
```bash
python train_model.py                # affiche accuracy, f1, matrice de confusion
python level2_mllib_stream.py        # puis, dans un autre terminal : python producer.py
```
Pipeline : RegexTokenizer -> StopWordsRemover (FR+EN) -> HashingTF -> IDF -> LogisticRegression -> IndexToString.

## Level 3 : Elasticsearch + Kibana
```bash
docker compose --profile viz up -d   # attendre ~1 min
python level3_elasticsearch_stream.py
python producer.py --loop --delay 0.5
```
Kibana : http://localhost:5601
1. Stack Management -> Data Views -> créer `job_offers`, champ temps `@timestamp`.
2. Dashboard : camembert (terms de `domaine`), courbe (count par minute avec date histogram), tableau des dernières offres, métrique de confiance moyenne.

## Level 4 : réseau social (Mastodon)
```bash
python producer_mastodon.py          # remplace producer.py, le job Spark reste identique
```
Attention : ce sont des posts généraux, pas des offres calibrées. Le modèle classera ce qu'il reçoit, et la confiance sera plus faible.

## Level 5 : plan de la vidéo (5 min)
1. Architecture (30 s) : schéma ci-dessus.
2. `docker compose up -d` + producer qui envoie (45 s).
3. Level 1 : console avec classification par mots-clés (45 s).
4. Level 2 : accuracy de l'entraînement + console MLlib (1 min).
5. Level 3 : dashboard Kibana qui se met à jour en direct (1 min 30).
6. Level 4 : flux Mastodon (45 s) et conclusion (15 s).

## Dépannage
- `Failed to find data source: kafka` : la version du package dans `common.py` doit correspondre à celle de pyspark (3.5.1, Scala 2.12).
- Relancer un job : `rm -rf checkpoints/<level>` si le checkpoint pose problème.
- Rien ne s'affiche : lancer Spark avant le producer, ou `STARTING_OFFSETS=earliest python level2_mllib_stream.py`.
- Elasticsearch ne démarre pas sous Linux : `sudo sysctl -w vm.max_map_count=262144`.
- Port 9092 : le broker est annoncé en `localhost`, donc Spark et producer doivent tourner sur la même machine que Docker.
