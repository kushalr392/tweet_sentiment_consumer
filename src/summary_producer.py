
import os
import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_kafka_producer(bootstrap_servers, username, password):
    """Creates and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-512',
            sasl_plain_username=username,
            sasl_plain_password=password,
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        logging.info(f"Kafka Producer connected to {bootstrap_servers}")
        return producer
    except KafkaError as e:
        logging.error(f"Error creating Kafka Producer: {e}")
        raise

def calculate_5_star_rating(positive_count, negative_count):
    """Calculates a 5-star rating based on positive and negative counts."""
    total_reviews = positive_count + negative_count
    if total_reviews == 0:
        return 0.0
    # A simple linear mapping: 1 star for all negative, 5 stars for all positive.
    # (positive_ratio * 4) + 1
    positive_ratio = positive_count / total_reviews
    rating = (positive_ratio * 4) + 1
    return round(rating, 2)

def send_summary_report(producer, summary_topic, product_sentiment_data):
    """Generates and sends a summary report to the summary topic."""
    for product_name, sentiment_counts in product_sentiment_data.items():
        positive_count = sentiment_counts['positive']
        negative_count = sentiment_counts['negative']
        rating = calculate_5_star_rating(positive_count, negative_count)

        summary_message = {
            'productName': product_name,
            'positiveReviews': positive_count,
            'negativeReviews': negative_count,
            'fiveStarRating': rating,
            'timestamp': time.time()
        }
        try:
            producer.send(summary_topic, summary_message)
            logging.info(f"Sent summary for {product_name} to {summary_topic}: {summary_message}")
        except KafkaError as e:
            logging.error(f"Failed to send summary for {product_name} to {summary_topic}: {e}")
    producer.flush()

def summary_producer_task(shared_sentiment_data, producer_config, summary_topic, interval_seconds=10):
    """
    Task for the summary producer thread.
    Periodically sends aggregated sentiment data to a summary topic.
    """
    producer = None
    try:
        producer = create_kafka_producer(
            producer_config['bootstrap_servers'],
            producer_config['username'],
            producer_config['password']
        )
        while True:
            time.sleep(interval_seconds)
            with producer_config['lock']:
                # Create a copy to avoid issues if shared_sentiment_data is modified during iteration
                current_data = shared_sentiment_data.copy()
                # Reset counts after sending, or keep cumulative counts
                # For this requirement, we'll reset to provide summaries of recent activity
                # If cumulative sums are needed, this reset should be removed
                for product_name in shared_data_initializer(producer_config['products_list']):
                    if product_name in shared_sentiment_data:
                        shared_sentiment_data[product_name] = {'positive': 0, 'negative': 0}
                    else:
                        shared_sentiment_data[product_name] = {'positive': 0, 'negative': 0}

            send_summary_report(producer, summary_topic, current_data)
    except KeyboardInterrupt:
        logging.info("Summary producer stopped by user.")
    except Exception as e:
        logging.critical(f"Unhandled error in summary producer task: {e}")
    finally:
        if producer:
            producer.close()
            logging.info("Kafka Summary Producer closed.")

def shared_data_initializer(products_list):
    """Initializes the shared sentiment data structure."""
    data = {}
    for product in products_list:
        data[product] = {'positive': 0, 'negative': 0}
    return data
