from kafka import KafkaConsumer
import json, os, signal
import datetime as dt
from config import *

date_str = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
OUTPUT_FILE = f"data/data_{date_str}.ndjson"
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

LOG_FILE = f"log/{date_str}_log/log_{date_str}.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)


_shutdown = False
def sigint(signum, frame):
    global _shutdown
    _shutdown = True
    print("CTRL+C detected. Stopping.....")

def safe_json_deserializer(x):
    try:
        if not x:
            return None
        return json.loads(x.decode('utf-8'))
    except Exception:
        return None


def main():
    signal.signal(signal.SIGINT, sigint)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers= KAFKA_BROKER,
        value_deserializer=safe_json_deserializer,
        auto_offset_reset='earliest',
        group_id='bank-consumer-group',
        consumer_timeout_ms=1000
    )

    print(f"Writing consumed message into {OUTPUT_FILE}.. (CTRL+C to stop)")

    try:
         with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
            with open(LOG_FILE, 'a', encoding='utf-8') as log_f:   # ✅ open once

                while not _shutdown:
                    for msg in consumer:
                        if _shutdown:
                            break

                        if msg.value is None:
                            continue

                        try:
                            line = json.dumps(msg.value)
                            f.write(line + '\n')
                            f.flush()

                        except Exception as e:
                            # ✅ NEVER re-process the same data again here
                            log_f.write(
                                f"{dt.datetime.now().isoformat()} | ERROR: {e} | RAW: {msg.value}\n"
                            )
                            log_f.flush()
                            continue

    finally:
        try:
            consumer.close()
        except:
            pass

        print('CLOSED')

if __name__ == '__main__':
    main()