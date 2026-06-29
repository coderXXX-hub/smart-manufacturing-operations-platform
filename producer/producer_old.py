import json
import time
import logging
import pandas as pd

from confluent_kafka import Producer
from config import *
from event_generator import generate_event


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks="all"
)


df = pd.read_csv(CSV_FILE)

logger.info(f"Loaded {len(df)} manufacturing records")


try:

    for _, row in df.iterrows():

        event = generate_event(row)

        future = producer.send(TOPIC, event)

        metadata = future.get(timeout=10)

        logger.info(
            f"Sent event "
            f"partition={metadata.partition} "
            f"offset={metadata.offset} "
            f"machine={event['machine_id']} "
            f"failure={event['failure_type']}"
        )

        time.sleep(SEND_INTERVAL_SECONDS)

except KeyboardInterrupt:

    logger.info("Stopping producer...")

finally:

    producer.flush()
    producer.close()
