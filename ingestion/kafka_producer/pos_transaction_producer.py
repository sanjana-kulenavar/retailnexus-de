# ingestion/kafka_producer/pos_transaction_producer.py

# ─────────────────────────────────────────────────────────────────────
# IMPORTS — loading the Python libraries we need
# ─────────────────────────────────────────────────────────────────────

import json      # converts Python dicts → JSON text for Kafka
import random    # generates random values for fake transactions
import time      # controls how fast we send (10 per second)
import uuid      # generates unique transaction IDs
import os        # reads environment variables from .env file

from datetime import datetime
from kafka import KafkaProducer           # from kafka-python-ng package
from transaction_schema import POSTransaction
from dotenv import load_dotenv

# load_dotenv() reads your .env file and makes all variables available
# via os.getenv() — this is how scripts read configuration safely
load_dotenv()


# ─────────────────────────────────────────────────────────────────────
# SETUP — data used to generate realistic transactions
# ─────────────────────────────────────────────────────────────────────

# 50 Dutch retail stores numbered NL-001 to NL-050
# str(i).zfill(3) pads the number to 3 digits: 1 → "001", 10 → "010"
STORE_IDS = [f"NL-{str(i).zfill(3)}" for i in range(1, 51)]
# Result: ["NL-001", "NL-002", ..., "NL-050"]

# 5,000 products in the catalog
PRODUCT_IDS = [f"PROD-{str(i).zfill(5)}" for i in range(1, 5001)]
# Result: ["PROD-00001", "PROD-00002", ..., "PROD-05000"]

# Assign a fixed price to each product
# random.uniform(0.99, 499.99) = random decimal between 0.99 and 499.99
# round(..., 2) = round to 2 decimal places (cents)
PRODUCT_PRICES = {
    product: round(random.uniform(0.99, 499.99), 2)
    for product in PRODUCT_IDS
}

# Payment method weights — Dutch market proportions
# In the Netherlands ~60% of transactions are contactless card
PAYMENT_METHODS = ["CONTACTLESS", "CARD", "CASH", "VOUCHER"]
PAYMENT_WEIGHTS = [0.60, 0.25, 0.12, 0.03]
# weights must sum to 1.0

# Each store has between 2 and 10 checkout terminals
TERMINALS_PER_STORE = {store: random.randint(2, 10) for store in STORE_IDS}


# ─────────────────────────────────────────────────────────────────────
# TRANSACTION GENERATOR
# ─────────────────────────────────────────────────────────────────────

def generate_transaction() -> POSTransaction:
    """
    Generates one realistic POS transaction.

    Steps:
    1. Picks random values for each field
    2. Calculates total_amount from quantity × unit_price
    3. Determines audit_flag based on business rules
    4. Creates and returns a validated POSTransaction object
       (Pydantic validates automatically when you call POSTransaction(...))

    Returns:
        POSTransaction: a validated, ready-to-send transaction object
    """

    # Pick a random store
    store_id = random.choice(STORE_IDS)

    # Pick a random terminal in that store
    num_terminals = TERMINALS_PER_STORE[store_id]
    terminal_num = str(random.randint(1, num_terminals)).zfill(2)
    terminal_id = f"POS-{terminal_num}"

    # Pick a product and its price
    product_id = random.choice(PRODUCT_IDS)
    unit_price = PRODUCT_PRICES[product_id]

    # Pick a quantity — lower quantities are more common in retail
    # random.choices() lets you assign probabilities to each option
    quantity = random.choices(
        population=[1, 2, 3, 4, 5],
        weights=[0.55, 0.25, 0.12, 0.05, 0.03]
    )[0]  # [0] because random.choices returns a list, we want the first item

    # Calculate total
    total_amount = round(unit_price * quantity, 2)

    # 2% of transactions are voided
    # random.random() returns a decimal between 0.0 and 1.0
    # 0.02 = 2% chance of being True
    is_voided = random.random() < 0.02

    # Apply ReSA-equivalent audit flags
    if is_voided:
        audit_flag = "VOIDED"
    elif total_amount > 5000:
        # High-value transactions need manager approval in retail
        # This mirrors the exception flagging logic in Oracle ReSA
        audit_flag = "EXCEPTION_HIGH_VALUE"
    else:
        audit_flag = "VALID"

    # Create the transaction object — Pydantic validates all fields here
    # If any field fails validation, an error is raised immediately
    return POSTransaction(
        transaction_id=str(uuid.uuid4()),  # uuid4 = random, globally unique ID
        store_id=store_id,
        terminal_id=terminal_id,
        cashier_id=f"EMP-{random.randint(1000, 9999)}",
        product_id=product_id,
        quantity=quantity,
        unit_price=unit_price,
        total_amount=total_amount,
        payment_method=random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
        transaction_ts=datetime.utcnow().isoformat(),
        is_voided=is_voided,
        audit_flag=audit_flag
    )


# ─────────────────────────────────────────────────────────────────────
# KAFKA PRODUCER SETUP
# ─────────────────────────────────────────────────────────────────────

def create_kafka_producer() -> KafkaProducer:
    """
    Creates and configures a KafkaProducer.

    Kafka stores all data as bytes (raw binary).
    Serializers convert Python objects → bytes automatically:

    key_serializer:
        The message key is the store_id (a Python string)
        .encode("utf-8") converts it to bytes
        WHY USE A KEY? Messages with the same key always go to the
        same Kafka partition. So all NL-001 transactions stay together.

    value_serializer:
        The message value is the transaction dict
        json.dumps() converts dict → JSON string
        .encode("utf-8") converts string → bytes

    Returns:
        KafkaProducer: ready to send messages
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=3,           # retry up to 3 times if sending fails
        acks="all",          # wait for Kafka to confirm message was saved
        request_timeout_ms=5000,
    )


# ─────────────────────────────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────────────────────────────

def run_producer(transactions_per_second: int = 10):
    """
    Runs the producer continuously until you press Ctrl+C.

    Every second:
    1. Generates `transactions_per_second` transactions
    2. Sends them all to Kafka
    3. Calls flush() to make sure they're delivered
    4. Waits 1 second before the next batch
    5. Prints progress every 100 transactions

    Args:
        transactions_per_second: messages to send per second (default: 10)
    """
    topic = os.getenv("KAFKA_TOPIC_POS", "pos-transactions")

    print("=" * 60)
    print("  ReSA POS Transaction Producer — RetailNexus DE")
    print("=" * 60)
    print(f"  Kafka topic : {topic}")
    print(f"  Send rate   : {transactions_per_second} transactions/second")
    print(f"  Stores      : {len(STORE_IDS)} Dutch retail stores")
    print(f"  Products    : {len(PRODUCT_IDS)} items in catalog")
    print(f"  Press Ctrl+C to stop")
    print("=" * 60)

    producer = create_kafka_producer()
    total_sent = 0

    try:
        while True:  # run forever until Ctrl+C

            batch_transactions = []

            for _ in range(transactions_per_second):
                txn = generate_transaction()

                # .model_dump() converts the Pydantic object → Python dict
                # (In Pydantic v2, this replaced the old .dict() method)
                txn_dict = txn.model_dump()

                # Send to Kafka
                # topic: which Kafka topic to write to
                # key: store_id — ensures ordering per store
                # value: the full transaction as a dict (serializer handles JSON)
                producer.send(
                    topic=topic,
                    key=txn.store_id,
                    value=txn_dict
                )

                batch_transactions.append(txn)
                total_sent += 1

            # flush() forces immediate delivery of all buffered messages
            # Without this, messages might wait in an internal buffer
            producer.flush()

            # Print progress every 100 transactions
            if total_sent % 100 == 0:
                last = batch_transactions[-1]
                print(
                    f"  ✅ Sent: {total_sent:>6,} | "
                    f"Store: {last.store_id} | "
                    f"Product: {last.product_id} | "
                    f"€{last.total_amount:>8.2f} | "
                    f"{last.payment_method:<12} | "
                    f"Voided: {str(last.is_voided):<5} | "
                    f"Flag: {last.audit_flag}"
                )

            # Wait 1 second → this gives us exactly N transactions/second
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n  ⏹  Stopped. Total transactions sent: {total_sent:,}")
        producer.close()


if __name__ == "__main__":
    run_producer(transactions_per_second=10)