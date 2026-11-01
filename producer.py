"""Producer : lit un fichier texte ligne par ligne et envoie chaque ligne dans Kafka."""
import argparse
import time
from kafka import KafkaProducer
from common import KAFKA_BOOTSTRAP, TOPIC

parser = argparse.ArgumentParser()
parser.add_argument("--file", default="data/jobs_stream.txt")
parser.add_argument("--delay", type=float, default=1.0, help="secondes entre deux messages")
parser.add_argument("--loop", action="store_true", help="relire le fichier en boucle")
args = parser.parse_args()

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
print(f"Envoi de {args.file} vers le topic '{TOPIC}' (Ctrl+C pour arrêter)")
try:
    while True:
        with open(args.file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                producer.send(TOPIC, line.encode("utf-8"))
                print("->", line[:90])
                time.sleep(args.delay)
        producer.flush()
        if not args.loop:
            break
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    producer.close()
