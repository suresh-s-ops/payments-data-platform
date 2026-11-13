import json
import os

import pandas as pd
import snowflake.connector
from confluent_kafka import Consumer, Producer
from jsonschema import Draft202012Validator
from snowflake.connector.pandas_tools import write_pandas

BATCH_SIZE = 5000
SOURCE_TOPIC = "payments.payment_attempts"
DLQ_TOPIC = "payments.payment_attempts.dlq"

with open("contracts/payment_attempt.v2.json") as f:
    VALIDATOR = Draft202012Validator(json.load(f))

consumer = Consumer({
    "bootstrap.servers": os.environ["KAFKA_BROKERS"],
    "group.id": "payments-snowflake-loader",
    "enable.auto.commit": False,          # commit only after Snowflake write succeeds
    "auto.offset.reset": "earliest",
})
dlq = Producer({"bootstrap.servers": os.environ["KAFKA_BROKERS"]})
sf = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
    private_key_file=os.environ["SNOWFLAKE_KEY_PATH"],
    warehouse="LOAD_WH", database="RAW", schema="PAYMENTS",
)


def run():
    """Validate payment events against the contract and load them to Snowflake.

    Business rule: a payment event is never silently dropped. Every attempt
    represents real customer money, so finance, risk and audit must be able to
    trace it, including the ones that failed validation (sent to the DLQ).
    """
    consumer.subscribe([SOURCE_TOPIC])
    good = []
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        payload = json.loads(msg.value())
        errors = [e.message for e in VALIDATOR.iter_errors(payload)]
        if errors:
            dlq.produce(DLQ_TOPIC, json.dumps(
                {"payload": payload, "errors": errors, "contract": "v2"}))
        else:
            good.append({"PAYLOAD": json.dumps(payload),
                         "KAFKA_PARTITION": msg.partition(),
                         "KAFKA_OFFSET": msg.offset()})

        if len(good) >= BATCH_SIZE:
            write_pandas(sf, pd.DataFrame(good), "PAYMENT_ATTEMPTS_RAW")
            dlq.flush()
            consumer.commit(asynchronous=False)   # at-least-once; MERGE dedupes later
            good.clear()


if __name__ == "__main__":
    run()
