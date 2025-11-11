
import os
import json
import logging
import time
import threading
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from .sentiment_analyzer import analyze_sentiment

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv(dotenv_path='./config/.env')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
KAFKA_REVIEW_INPUT_TOPIC = os.getenv('KAFKA_REVIEW_INPUT_TOPIC', 'product_reviews')
KAFKA_POSITIVE_TOPIC = os.getenv('KAFKA_POSITIVE_TOPIC', 'product_reviews_positive')
KAFKA_NEGATIVE_TOPIC = os.getenv('KAFKA_NEGATIVE_TOPIC', 'product_reviews_negative')
KAFKA_DEAD_LETTER_TOPIC = os.getenv('KAFKA_DEAD_LETTER_TOPIC', 'review_dead_letter')

# SASL configuration
SASL_MECHANISM = 'SCRAM-SHA-512'
SECURITY_PROTOCOL = 'SASL_PLAINTEXT' # Use SASL_SSL if TLS is enabled

# Consumer settings
CONSUMER_GROUP_ID = 'product_sentiment_analysis_group'
AUTO_OFFSET_RESET = 'earliest'
VALUE_DESERIALIZER = lambda m: json.loads(m.decode('utf-8'))
VALUE_SERIALIZER = lambda m: json.dumps(m).encode('utf-8')

# Retry settings
MAX_RETRIES = 5
RETRY_BACKOFF_MS = 1000  # 1 second

def create_kafka_consumer():
    """Creates and returns a KafkaConsumer instance."""
    try:
        consumer = KafkaConsumer(
            KAFKA_REVIEW_INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset=AUTO_OFFSET_RESET,
            enable_auto_commit=True,
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_deserializer=VALUE_DESERIALIZER
        )
        logging.info(f"Kafka Consumer connected to {KAFKA_BOOTSTRAP_SERVERS} for topic {KAFKA_REVIEW_INPUT_TOPIC}")
        return consumer
    except KafkaError as e:
        logging.error(f"Error creating Kafka Consumer: {e}")
        raise

def create_kafka_producer():
    """Creates and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=VALUE_SERIALIZER
        )
        logging.info(f"Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except KafkaError as e:
        logging.error(f"Error creating Kafka Producer: {e}")
        raise

def send_to_dead_letter_topic(producer, original_message, error_details):
    """Sends a message to the dead-letter topic using a provided producer."""
    dead_letter_message = {
        'original_message': original_message,
        'error': str(error_details),
        'timestamp': time.time()
    }
    try:
        producer.send(KAFKA_DEAD_LETTER_TOPIC, dead_letter_message)
        producer.flush()
        logging.warning(f"Sent message to dead-letter topic: {original_message}")
    except KafkaError as e:
        logging.error(f"Failed to send to dead-letter topic: {e}")

def process_message(dead_letter_producer, sentiment_producer, message, shared_sentiment_data, lock):
    """Processes a single Kafka message, aggregates sentiment, and updates shared data."""
    review_data = message.value
    
    # Check for the expected keys from the input data (product and tweetContents)
    if not isinstance(review_data, dict) or 'product' not in review_data or 'tweetContents' not in review_data:
        logging.error(f"Invalid message format received: {review_data}. Skipping.")
        send_to_dead_letter_topic(dead_letter_producer, review_data, "Invalid message format: Missing product or tweetContents")
        return

    product_name = review_data['product'] # Extract from 'product' field
    review_content = review_data['tweetContents'] # Extract from 'tweetContents' field
    sentiment = analyze_sentiment(review_content)
    review_data['sentiment'] = sentiment # Add sentiment to the review data

    with lock:
        if product_name not in shared_sentiment_data:
            shared_sentiment_data[product_name] = {'positive': 0, 'negative': 0}
        if sentiment == 'positive':
            shared_sentiment_data[product_name]['positive'] += 1
            try:
                sentiment_producer.send(KAFKA_POSITIVE_TOPIC, review_data)
                logging.info(f"Sent positive review to {KAFKA_POSITIVE_TOPIC}: {review_data.get('userId', 'N/A')}")
            except KafkaError as e:
                logging.error(f"Failed to send positive review to topic: {e}")
                send_to_dead_letter_topic(dead_letter_producer, review_data, f"Failed to send positive review: {e}")
        elif sentiment == 'negative':
            shared_sentiment_data[product_name]['negative'] += 1
            try:
                sentiment_producer.send(KAFKA_NEGATIVE_TOPIC, review_data)
                logging.info(f"Sent negative review to {KAFKA_NEGATIVE_TOPIC}: {review_data.get('userId', 'N/A')}")
            except KafkaError as e:
                logging.error(f"Failed to send negative review to topic: {e}")
                send_to_dead_letter_topic(dead_letter_producer, review_data, f"Failed to send negative review: {e}")
        else:
            logging.info(f"Neutral review received (not sent to specific sentiment topic): {review_data.get('userId', 'N/A')}")
        sentiment_producer.flush()
        logging.info(f"Aggregated sentiment for {product_name}: {sentiment}. Current counts: {shared_sentiment_data[product_name]}")

def consumer_task(shared_sentiment_data, lock, consumer_config):
    """
    Task for the consumer thread.
    Consumes messages, performs sentiment analysis, and updates shared sentiment data.
    """
    consumer = None
    dead_letter_producer = None # Producer specifically for dead-letter messages from consumer
    sentiment_producer = None # Producer for positive/negative sentiment topics
    try:
        consumer = create_kafka_consumer()
        dead_letter_producer = create_kafka_producer()
        sentiment_producer = create_kafka_producer()

        for message in consumer:
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    logging.info(f"Received message: Topic={message.topic}, Partition={message.partition}, Offset={message.offset}")
                    process_message(dead_letter_producer, sentiment_producer, message, shared_sentiment_data, lock)
                    break # Success, break out of retry loop
                except Exception as e:
                    retries += 1
                    logging.error(f"Error processing message (attempt {retries}/{MAX_RETRIES}): {e}. Message: {message.value}")
                    if retries < MAX_RETRIES:
                        time.sleep(RETRY_BACKOFF_MS / 1000) # Convert ms to seconds
                    else:
                        logging.error(f"Max retries reached for message: {message.value}. Sending to dead-letter topic.")
                        send_to_dead_letter_topic(dead_letter_producer, message.value, e)
                        break # Max retries reached, move to next message

    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.critical(f"Unhandled error in consumer loop: {e}")
    finally:
        if consumer:
            consumer.close()
            logging.info("Kafka Consumer closed.")
        if dead_letter_producer:
            dead_letter_producer.close()
            logging.info("Kafka Dead-Letter Producer closed for dead-letter topic.")
        if sentiment_producer:
            sentiment_producer.close()
            logging.info("Kafka Sentiment Producer closed for sentiment topics.")
