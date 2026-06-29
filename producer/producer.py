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


##########################################################
# Delivery callback
##########################################################

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(
            f"Delivered "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


##########################################################
# Kafka Producer
##########################################################

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
    "client.id": "manufacturing-producer"
})


##########################################################
# Load CSV
##########################################################

df = pd.read_csv(CSV_FILE)

logger.info(f"Loaded {len(df)} manufacturing records")


##########################################################
# Publish Events
##########################################################

try:

    while True:

        for _, row in df.iterrows():

            event = generate_event(row)

            producer.produce(
                topic=TOPIC,
                value=json.dumps(event),
                callback=delivery_report
            )

            producer.poll(0)

            logger.info(
                f"Sent Machine={event['machine_id']} "
                f"Failure={event['failure_type']} "
                f"Plant={event['plant']}"
            )

            time.sleep(SEND_INTERVAL_SECONDS)

except KeyboardInterrupt:

    logger.info("Stopping producer...")

finally:

    producer.flush()

    logger.info("Producer closed.")
