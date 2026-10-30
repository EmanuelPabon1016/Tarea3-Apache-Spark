#!/usr/bin/env python3
"""Publica eventos sintéticos de medición eléctrica en un topic Kafka."""
import itertools
import json
import os
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime
 
TOPIC = os.environ.get("KAFKA_TOPIC", "consumo_electrico_tiempo_real")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
 
 
def main():
    producer = shutil.which("kafka-console-producer.sh")
    if producer is None:
        kafka_home = os.environ.get("KAFKA_HOME")
        if kafka_home:
            candidate = os.path.join(kafka_home, "bin", "kafka-console-producer.sh")
            if os.path.isfile(candidate):
                producer = candidate
    if producer is None:
        sys.exit("No encuentro kafka-console-producer.sh. Agregue Kafka a PATH o configure KAFKA_HOME.")
 
    cmd = [producer, "--bootstrap-server", BOOTSTRAP, "--topic", TOPIC]
    print("Productor:", " ".join(cmd), flush=True)
    print("Publicando una medición sintética cada 2 segundos. Ctrl+C para terminar.", flush=True)
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True, bufsize=1)
    try:
        for i in itertools.count(1):
            power = round(random.uniform(0.1, 6.0), 3)
            voltage = round(random.uniform(220.0, 245.0), 2)
            level = "alto" if power >= 3.5 else ("medio" if power >= 1.5 else "bajo")
            event = {
                "event_id": i,
                "global_active_power": power,
                "voltage": voltage,
                "consumption_level": level,
                "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "simulado"
            }
            proc.stdin.write(json.dumps(event, ensure_ascii=False) + "\n")
            proc.stdin.flush()
            print("Enviado:", json.dumps(event, ensure_ascii=False), flush=True)
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nDeteniendo el generador...", flush=True)
    finally:
        try:
            if proc.stdin: proc.stdin.close()
        except Exception:
            pass
        if proc.poll() is None:
            proc.terminate()
            try: proc.wait(timeout=3)
            except subprocess.TimeoutExpired: proc.kill()
        print("Generador detenido.", flush=True)
 
 
if __name__ == "__main__":
    main()
