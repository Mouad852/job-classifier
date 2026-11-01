"""LEVEL 4 : lit en temps réel des posts publics Mastodon (hashtags emploi/IT) -> Kafka.
API publique, sans clé ni compte. Le reste du pipeline ne change pas."""
import argparse
import html
import re
import time

import requests
from kafka import KafkaProducer
from common import KAFKA_BOOTSTRAP, TOPIC

parser = argparse.ArgumentParser()
parser.add_argument("--instance", default="https://mastodon.social")
parser.add_argument("--tags", nargs="+", default=["hiring", "jobs", "devops", "dataengineer", "cybersecurity", "cloud"])
parser.add_argument("--interval", type=int, default=15, help="secondes entre deux scrutations")
args = parser.parse_args()

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
seen = set()
print(f"Lecture de {args.instance} pour les tags {args.tags} (Ctrl+C pour arrêter)")
try:
    while True:
        for tag in args.tags:
            try:
                r = requests.get(f"{args.instance}/api/v1/timelines/tag/{tag}", params={"limit": 20}, timeout=15)
                r.raise_for_status()
            except requests.RequestException as e:
                print("erreur API :", e)
                continue
            for post in r.json():
                if post["id"] in seen or post.get("language") not in (None, "en", "fr"):
                    continue
                seen.add(post["id"])
                text = html.unescape(re.sub(r"<[^>]+>", " ", post["content"]))
                text = re.sub(r"\s+", " ", text).strip()
                if len(text) < 30:
                    continue
                producer.send(TOPIC, text.encode("utf-8"))
                print(f"[#{tag}]", text[:100])
        producer.flush()
        time.sleep(args.interval)
except KeyboardInterrupt:
    pass
finally:
    producer.close()
