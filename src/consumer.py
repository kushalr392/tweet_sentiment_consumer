
import os
import json
import logging
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from textblob import TextBlob
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv(dotenv_path='./config/.env')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'tweets_input')
KAFKA_POSITIVE_TOPIC = os.getenv('KAFKA_POSITIVE_TOPIC', 'tweets_positive')
KAFKA_NEGATIVE_TOPIC = os.getenv('KAFKA_NEGATIVE_TOPIC', 'tweets_negative')
KAFKA_DEAD_LETTER_TOPIC = os.getenv('KAFKA_DEAD_LETTER_TOPIC', 'tweets_dead_letter')

# SASL configuration
SASL_MECHANISM = 'SCRAM-SHA-512'
SECURITY_PROTOCOL = 'SASL_PLAINTEXT' # Use SASL_SSL if TLS is enabled

# Consumer and Producer settings
CONSUMER_GROUP_ID = 'sentiment_analysis_group'
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
            KAFKA_INPUT_TOPIC,
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
        logging.info(f"Kafka Consumer connected to {KAFKA_BOOTSTRAP_SERVERS} for topic {KAFKA_INPUT_TOPIC}")
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

def analyze_sentiment(text):
    """Performs sentiment analysis on the given text."""
    analysis = TextBlob(text)
    # Return 'positive', 'negative', or 'neutral'
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity < 0:
        return 'negative'
    else:
        return 'neutral'

def send_to_dead_letter_topic(producer, original_message, error_details):
    """Sends a message to the dead-letter topic."""
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

def process_message(producer, message):
    """Processes a single Kafka message."""
    tweet_data = message.value
    if not isinstance(tweet_data, dict) or 'tweetContents' not in tweet_data:
        logging.error(f"Invalid message format received: {tweet_data}. Skipping.")
        send_to_dead_letter_topic(producer, tweet_data, "Invalid message format")
        return

    tweet_content = tweet_data['tweetContents']
    sentiment = analyze_sentiment(tweet_content)
    tweet_data['sentiment'] = sentiment

    try:
        if sentiment == 'positive':
            producer.send(KAFKA_POSITIVE_TOPIC, tweet_data)
            logging.info(f"Sent positive tweet to {KAFKA_POSITIVE_TOPIC}: {tweet_data['userId']}")
        elif sentiment == 'negative':
            producer.send(KAFKA_NEGATIVE_TOPIC, tweet_data)
            logging.info(f"Sent negative tweet to {KAFKA_NEGATIVE_TOPIC}: {tweet_data['userId']}")
        else:
            # Optionally send neutral tweets to a specific topic or log them
            logging.info(f"Neutral tweet received (not sent to specific topic): {tweet_data['userId']}")
        producer.flush()
    except KafkaError as e:
        logging.error(f"Failed to send message to output topic for tweet {tweet_data.get('userId', 'N/A')}: {e}")
        send_to_dead_letter_topic(producer, tweet_data, e)

def run_consumer():
    """Runs the Kafka consumer to process messages."""
    consumer = None
    producer = None
    try:
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()

        for message in consumer:
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    logging.info(f"Received message: Topic={message.topic}, Partition={message.partition}, Offset={message.offset}")
                    process_message(producer, message)
                    break # Success, break out of retry loop
                except Exception as e:
                    retries += 1
                    logging.error(f"Error processing message (attempt {retries}/{MAX_RETRIES}): {e}. Message: {message.value}")
                    if retries < MAX_RETRIES:
                        time.sleep(RETRY_BACKOFF_MS / 1000) # Convert ms to seconds
                    else:
                        logging.error(f"Max retries reached for message: {message.value}. Sending to dead-letter topic.")
                        send_to_dead_letter_topic(producer, message.value, e)
                        break # Max retries reached, move to next message

    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.critical(f"Unhandled error in consumer loop: {e}")
    finally:
        if consumer:
            consumer.close()
            logging.info("Kafka Consumer closed.")
        if producer:
            producer.close()
            logging.info("Kafka Producer closed.")

if __name__ == "__main__":
    run_consumer()
