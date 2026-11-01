# CONTEXT_IA.md : brief complet du projet (à lire en entier avant toute action)

## 0. Comment utiliser ce fichier

**Pour l'humain :** ouvrez une conversation avec votre IA, joignez ce fichier (ou collez son contenu) et le dossier du projet, puis collez ce prompt :

> Lis intégralement CONTEXT_IA.md. Tu es mon assistant pour terminer ce projet universitaire. Respecte les versions et décisions de la section 2, ne réécris pas le projet de zéro. Commence par la section 6 (installation) et guide-moi pas à pas, une étape à la fois. Quand une commande échoue, demande-moi l'erreur complète avant de proposer une correction. Signale-moi tout ce que tu supposes sans l'avoir vérifié.

**Pour l'IA :** ce fichier décrit la consigne, les choix faits, l'architecture, le rôle de chaque fichier, ce qui a été vérifié et ce qui ne l'a PAS été, et ce qu'il reste à faire. Si quelque chose contredit ce fichier, signale-le à l'utilisateur au lieu de deviner.

---

## 1. Consigne académique (module « Traitement distribué et Big Data »)

Objectif principal : **comprendre l'intégration Kafka + Spark Streaming**. Le temps est limité. Au choix : (1) analyse de sentiments sur des commentaires, (2) **classification d'offres d'emploi par domaine : Data, Développement, Sécurité, Cloud**. Le choix retenu est le **cas 2**.

| Niveau | Exigence officielle |
|---|---|
| 1 | Producer qui lit un fichier texte ligne par ligne et envoie chaque ligne dans un topic Kafka. Kafka stocke. Spark Structured Streaming lit en temps réel, applique un modèle **simple basé sur des mots-clés** et affiche le résultat. |
| 2 | Utiliser **Spark MLlib** pour la classification. |
| 3 | Visualisations, de préférence **Elasticsearch / Kibana**. |
| 4 | Lire en temps réel depuis un **réseau social**. |
| 5 | **Capsule vidéo** déposée sur **Moodle**. |

---

## 2. Décisions déjà prises (ne pas changer sans raison)

- **Cas d'usage :** offres d'emploi, 4 classes : `Data`, `Développement`, `Sécurité`, `Cloud` (+ `Autre` en Level 1 si aucun mot-clé).
- **Langage :** Python (PySpark). Version **pyspark==3.5.1** (Scala 2.12). Le package Kafka doit correspondre : `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` (défini dans `common.py`). Si la version de pyspark change, changer ce package aussi.
- **Python supporté : 3.9 à 3.11** (3.10 ou 3.11 recommandé). **Python 3.12+ et 3.14 ne sont pas supportés par PySpark 3.5.1** et provoquent des erreurs de workers.
- **Java : 17 (ou 11)** obligatoire.
- **Kafka :** image `apache/kafka:3.7.0` en mode KRaft (sans Zookeeper), broker annoncé sur `localhost:9092`, topic `job_offers` (créé automatiquement).
- **Dataset :** généré (synthétique) par `generate_dataset.py`. 1200 offres étiquetées pour l'entraînement, 80 non étiquetées pour le flux. Il est volontairement bruité (15 % de compétences d'un autre domaine). **Limite assumée :** l'accuracy sera probablement très élevée parce que les données sont fabriquées par gabarits. Si un vrai dataset est trouvé (Kaggle, offres scrapées), le remplacer en gardant les colonnes `label,text` dans `data/train_jobs.csv`.
- **Level 4 :** Mastodon (API publique par hashtag, sans clé), car Reddit restreint l'accès à son API et X est payant.
- **Level 3 :** écriture dans Elasticsearch avec le client Python `elasticsearch==8.13.0` dans `foreachBatch` (pas le connecteur Spark-ES, pour éviter les conflits de versions).
- **Langue des livrables :** français.

---

## 3. Architecture

```
data/jobs_stream.txt ─┐
                      ├─> Producer (Python) ─> Kafka (topic "job_offers") ─> Spark Structured Streaming ─┬─> console (Level 1 : mots-clés, Level 2 : MLlib)
Mastodon (Level 4) ───┘                                                                                  └─> Elasticsearch (index "job_offers") ─> Kibana (Level 3)

Hors flux, en batch :  data/train_jobs.csv ─> train_model.py (Pipeline MLlib) ─> models/job_classifier ─> chargé par les jobs streaming Level 2 et 3
```

Principe clé à comprendre : **on entraîne le modèle en batch, une fois, puis on l'applique en continu sur le flux**. Le flux Kafka n'est jamais utilisé pour entraîner.

---

## 4. Arborescence et rôle de chaque fichier

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` | Kafka (par défaut). Elasticsearch 8.13.4 + Kibana 8.13.4 avec `--profile viz` (sécurité ES désactivée, mémoire 512 Mo). |
| `requirements.txt` | pyspark==3.5.1, kafka-python-ng, elasticsearch==8.13.0, requests. |
| `common.py` | Constantes (`KAFKA_BOOTSTRAP`, `TOPIC`, `MODEL_PATH`), `get_spark()` (session locale `local[*]` + package Kafka) et `read_kafka()` (flux Kafka -> colonnes `text`, `timestamp`). `startingOffsets` vaut `latest` par défaut, modifiable avec la variable d'environnement `STARTING_OFFSETS=earliest`. |
| `generate_dataset.py` | Génère `data/train_jobs.csv` (label,text) et `data/jobs_stream.txt` (une offre par ligne). |
| `producer.py` | Lit un fichier ligne par ligne et envoie dans Kafka. Options : `--file`, `--delay` (secondes), `--loop`. |
| `level1_keywords_stream.py` | **Level 1.** Dictionnaire de mots-clés par domaine, compte les occurrences avec `regexp_count` (mots entiers, insensible à la casse et aux accents), prend le domaine au score maximal (« Autre » si tous les scores valent 0), affiche en console. |
| `train_model.py` | **Level 2 (batch).** Pipeline : RegexTokenizer -> StopWordsRemover (FR+EN) -> HashingTF (16384) -> IDF -> LogisticRegression -> IndexToString. Split 80/20, affiche **accuracy, F1 et matrice de confusion**, sauvegarde dans `models/job_classifier`. |
| `level2_mllib_stream.py` | **Level 2 (streaming).** Charge le modèle, l'applique au flux, affiche `text`, `domaine`, `confiance` en console. |
| `level3_elasticsearch_stream.py` | **Level 3.** Même chose que Level 2 puis, dans `foreachBatch`, indexe chaque offre dans Elasticsearch (`@timestamp`, `text`, `domaine`, `confiance`, `source`). Crée l'index avec son mapping s'il n'existe pas. |
| `producer_mastodon.py` | **Level 4.** Scrute toutes les 15 s les hashtags (`hiring`, `jobs`, `devops`, `dataengineer`, `cybersecurity`, `cloud`) d'une instance Mastodon, nettoie le HTML, déduplique, envoie dans Kafka. |
| `README.md` | Commandes de base. |
| `CONTEXT_IA.md` | Ce fichier. |

Dossiers créés à l'exécution : `checkpoints/` (état des streams) et `models/` (modèle MLlib).

---

## 5. État d'avancement : ce qui est vérifié et ce qui ne l'est PAS

**Vérifié :** `generate_dataset.py` s'exécute et produit les deux fichiers ; tous les `.py` compilent sans erreur de syntaxe ; `pip install -r requirements.txt` réussit.

**NON vérifié (jamais exécuté de bout en bout) :** le broker Kafka, les jobs Spark (Level 1, 2, 3), l'entraînement, l'indexation Elasticsearch, le producer Mastodon. Le code a été écrit avec soin mais **aucune exécution réelle n'a eu lieu**. Il peut y avoir des erreurs d'exécution à corriger : c'est normal, les traiter une par une.

**Historique de l'auteur initial :** sa machine (Windows 10, build 26300 qui semble un Insider Preview, Python 3.14) n'a pas permis de faire tourner Docker Desktop (« Docker Engine stopped », erreur WSL 1603). Le projet n'a donc jamais été lancé là-bas. **Le binôme repart de zéro sur une machine propre.**

---

## 6. Installation sur la machine du binôme

**Environnement recommandé :** Linux, macOS, ou Windows avec **WSL2 (Ubuntu)**. Éviter Windows natif pour Spark (il faut `winutils.exe` et `HADOOP_HOME`, source de nombreux problèmes).

1. Installer **Java 17** : `java -version` doit répondre.
2. Installer **Python 3.10 ou 3.11** : `python --version`.
3. Dans le dossier du projet :
   ```bash
   python -m venv venv
   source venv/bin/activate        # Windows : venv\Scripts\activate
   pip install -r requirements.txt
   python generate_dataset.py
   ```
4. Lancer Kafka, au choix :
   - **Avec Docker :** `docker compose up -d` puis `docker ps` doit montrer le conteneur `kafka` en `Up`.
   - **Sans Docker (si Docker pose problème) :**
     ```bash
     wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz
     tar -xzf kafka_2.13-3.7.0.tgz && cd kafka_2.13-3.7.0
     KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
     bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
     bin/kafka-server-start.sh config/kraft/server.properties   # laisser ce terminal ouvert
     ```
     (Sous Windows natif : mêmes commandes avec les `.bat` du dossier `bin\windows`.)

---

## 7. Exécution niveau par niveau

**Règle d'or : démarrer le job Spark AVANT le producer** (avec `startingOffsets=latest`, Spark ignore les messages envoyés avant son démarrage).

### Level 1 : mots-clés
```bash
python level1_keywords_stream.py     # terminal A, attendre "Batch: 0"
python producer.py --delay 1         # terminal B
```
Attendu : un tableau par micro-batch dans le terminal A avec `text`, `domaine`, `score_0..3` (0=Data, 1=Développement, 2=Sécurité, 3=Cloud).

### Level 2 : MLlib
```bash
python train_model.py                # une seule fois, avant le streaming
python level2_mllib_stream.py        # terminal A
python producer.py                   # terminal B
```
Attendu à l'entraînement : lignes `accuracy = ...` et `f1 = ...`, la matrice de confusion, puis « Modèle sauvegardé ». Attendu en streaming : colonnes `text`, `domaine`, `confiance`. **Noter l'accuracy pour le rapport et la vidéo.**

### Level 3 : Elasticsearch + Kibana
- Avec Docker : `docker compose --profile viz up -d` (attendre ~1 min, puis http://localhost:9200 doit répondre en JSON).
- Sans Docker : télécharger Elasticsearch et Kibana 8.13.4 (archives `.tar.gz`/`.zip` sur elastic.co). Dans `config/elasticsearch.yml`, mettre `xpack.security.enabled: false` et `discovery.type: single-node`, puis lancer Elasticsearch, puis Kibana.
- Sous Linux/WSL, si ES ne démarre pas : `sudo sysctl -w vm.max_map_count=262144`.

```bash
python level3_elasticsearch_stream.py   # terminal A
python producer.py --loop --delay 0.5   # terminal B (en boucle pour alimenter le dashboard)
```
Kibana (http://localhost:5601) :
1. Stack Management -> Data Views -> créer `job_offers`, champ temporel `@timestamp`.
2. Analytics -> Dashboard -> Create -> avec Lens :
   - **Camembert** : Slice by `domaine` (Top values), taille = Count.
   - **Courbe** : axe X `@timestamp` (Date histogram), axe Y Count, **Break down by** `domaine`.
   - **Métrique** : Count, et moyenne de `confiance`.
   - **Tableau** : colonnes `@timestamp`, `domaine`, `confiance`, `text`.
3. Sauvegarder le dashboard, régler l'actualisation automatique sur 5 secondes (haut à droite) pour la vidéo.

### Level 4 : réseau social
```bash
python producer_mastodon.py          # remplace producer.py ; le job Spark reste le même
```
Ce sont des posts généraux (pas de vraies offres) : la confiance sera plus basse, et c'est normal. Si l'API répond 401/403/429 ou rien : essayer une autre instance (`--instance https://mastodon.social` est le défaut), changer `--tags`, augmenter `--interval`. **Plan B si l'API publique est bloquée :** utiliser `producer.py` sur un fichier de posts réels copiés à la main et l'indiquer honnêtement dans la vidéo.

---

## 8. Pièges connus et dépannage

| Symptôme | Cause / solution |
|---|---|
| `Failed to find data source: kafka` | Le package Kafka ne correspond pas à pyspark (3.5.1 / Scala 2.12), ou pas d'accès Internet au premier lancement (il télécharge le JAR). |
| Spark ne montre rien | Producer lancé avant Spark. Relancer dans le bon ordre, ou `STARTING_OFFSETS=earliest python ...`. |
| Erreur de checkpoint au redémarrage | `rm -rf checkpoints/` (surtout après modification du code ou changement de topic). |
| `JAVA_HOME` / Java introuvable | Installer Java 17 et rouvrir le terminal. |
| `HADOOP_HOME and hadoop.home.dir are unset` ou `NativeIO$Windows` | Spark sous Windows natif. Utiliser WSL2, ou installer winutils. |
| Erreurs bizarres de worker / pickle | Python trop récent (3.12+). Utiliser 3.10 ou 3.11. |
| `NoBrokersAvailable` (producer) | Kafka pas démarré, ou port 9092 occupé. |
| `FileNotFoundError` / modèle absent au Level 2 | Lancer `python train_model.py` avant. |
| Elasticsearch `Connection refused` | ES pas encore prêt (attendre), ou conteneur arrêté faute de mémoire (augmenter la RAM de Docker ou d'ES). |
| Kibana vide | Le data view doit utiliser `@timestamp` ; élargir la plage de temps (Last 15 minutes) et vérifier `curl localhost:9200/job_offers/_count`. |
| Docker Desktop « Engine stopped » sous Windows | Problème de WSL/Windows. Contournement : Kafka sans Docker (section 6). |

---

## 9. Ce qu'il reste à faire (checklist)

- [ ] Environnement installé (Java 17, Python 3.10/3.11, venv, `pip install`).
- [ ] Kafka démarré (Docker ou natif).
- [ ] **Level 1** exécuté et fonctionnel (capture d'écran de la console).
- [ ] **Level 2** : `train_model.py` exécuté (noter accuracy, F1, matrice de confusion) + streaming fonctionnel.
- [ ] Corriger toute erreur d'exécution trouvée et **la signaler à l'équipe**.
- [ ] (Recommandé) Remplacer le dataset synthétique par un vrai dataset, ou au moins ajouter des offres réelles dans `data/jobs_stream.txt` pour une démo plus crédible.
- [ ] **Level 3** : ES + Kibana, dashboard (camembert, courbe, métrique, tableau).
- [ ] **Level 4** : Mastodon fonctionnel (ou plan B documenté).
- [ ] **Level 5** : vidéo enregistrée et déposée sur Moodle.

---

## 10. Plan de la capsule vidéo (≈ 5 min)

| Temps | Contenu |
|---|---|
| 0:00 – 0:30 | Cas d'usage et schéma d'architecture (section 3). |
| 0:30 – 1:15 | Kafka lancé, producer qui envoie des offres (terminal visible). |
| 1:15 – 2:00 | **Level 1** : console Spark, classification par mots-clés, montrer un cas réussi et un cas ambigu. |
| 2:00 – 3:00 | **Level 2** : accuracy/F1/matrice de confusion, puis prédictions MLlib avec la confiance ; comparer avec Level 1. |
| 3:00 – 4:15 | **Level 3** : dashboard Kibana qui se met à jour en direct pendant que le producer tourne. |
| 4:15 – 4:50 | **Level 4** : flux Mastodon réel, classé en direct. |
| 4:50 – 5:00 | Conclusion : limites (dataset synthétique, bruit des posts) et améliorations. |

Conseil : tester le déroulé une fois sans enregistrer. Avoir tous les terminaux et Kibana déjà ouverts.

---

## 11. Concepts à savoir expliquer à l'oral

- **Kafka :** un *producer* écrit des messages dans un *topic* (journal ordonné et persistant, découpé en partitions) ; un *consumer* les lit à son rythme grâce aux *offsets*. Il découple celui qui envoie de celui qui traite.
- **Spark Structured Streaming :** le flux est vu comme une table sans fin ; Spark traite des **micro-batches** et applique les mêmes opérations DataFrame que le batch. `readStream` / `writeStream`, mode de sortie `append`, **checkpoint** pour reprendre sans perte, `foreachBatch` pour écrire vers un système externe (ES).
- **Level 1 vs Level 2 :** règles manuelles (rapide, transparent, fragile) contre modèle appris (généralise, mesurable).
- **Pipeline MLlib :** tokenisation -> suppression des mots vides -> TF (fréquence des mots, par hachage) -> IDF (pondère les mots rares) -> régression logistique multinomiale -> reconversion de l'indice en nom de domaine.
- **Évaluation :** accuracy, F1, matrice de confusion sur 20 % de données non vues. Attention : sur des données synthétiques le score est optimiste (à dire honnêtement).
- **Pourquoi `latest` :** on veut le « temps réel », donc uniquement les nouveaux messages.

---

## 12. Règles pour l'IA qui reprend le projet

1. Ne pas réécrire le projet ; modifier le minimum nécessaire et expliquer chaque changement.
2. Ne pas changer les versions (pyspark 3.5.1, package Kafka, Elasticsearch 8.13.x) sans l'annoncer.
3. Ne jamais affirmer qu'une commande « marche » sans que l'utilisateur ait montré sa sortie.
4. Toujours demander l'erreur **complète** avant de proposer un correctif.
5. Avancer niveau par niveau : ne pas passer au Level N+1 tant que le Level N ne tourne pas.
6. Garder les réponses en français et les étapes courtes (une action à la fois).
