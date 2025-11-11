
import os
import logging
import threading
import time
from dotenv import load_dotenv

from .consumer import consumer_task
from .summary_producer import summary_producer_task, shared_data_initializer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv(dotenv_path='./config/.env')

# Kafka configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
KAFKA_REVIEW_INPUT_TOPIC = os.getenv('KAFKA_REVIEW_INPUT_TOPIC', 'product_reviews')
KAFKA_SUMMARY_TOPIC = os.getenv('KAFKA_SUMMARY_TOPIC', 'product_review_summaries')
KAFKA_DEAD_LETTER_TOPIC = os.getenv('KAFKA_DEAD_LETTER_TOPIC', 'review_dead_letter')

# List of products to monitor
PRODUCT_NAMES = [
    "SmartSpeaker Pro",
    "EcoFriendly Water Bottle",
    "SmartDoorbell Connect",
    "Fitness Tracker X10",
    "Gourmet Coffee Maker"
]

if __name__ == "__main__":
    logging.info("Starting Kafka Sentiment Analysis Consumer and Summary Producer...")

    # Shared data structure for sentiment counts and a lock for thread safety
    shared_sentiment_data = shared_data_initializer(PRODUCT_NAMES)
    data_lock = threading.Lock()

    # Consumer configuration
    consumer_config = {
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'username': KAFKA_USERNAME,
        'password': KAFKA_PASSWORD,
        'input_topic': KAFKA_REVIEW_INPUT_TOPIC,
        'dead_letter_topic': KAFKA_DEAD_LETTER_TOPIC,
        'group_id': os.getenv('CONSUMER_GROUP_ID', 'product_sentiment_analysis_group')
    }

    # Producer configuration for summaries
    producer_config = {
        'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
        'username': KAFKA_USERNAME,
        'password': KAFKA_PASSWORD,
        'lock': data_lock, # Pass the lock to the producer for synchronization
        'products_list': PRODUCT_NAMES # Pass the list of products for re-initialization
    }

    # Create and start the consumer thread
    consumer_thread = threading.Thread(
        target=consumer_task,
        args=(shared_sentiment_data, data_lock, consumer_config),
        name="ConsumerThread"
    )
    consumer_thread.daemon = True
    consumer_thread.start()

    # Create and start the summary producer thread
    summary_thread = threading.Thread(
        target=summary_producer_task,
        args=(shared_sentiment_data, producer_config, KAFKA_SUMMARY_TOPIC, 10),
        name="SummaryProducerThread"
    )
    summary_thread.daemon = True
    summary_thread.start()

    try:
        while True:
            time.sleep(1) # Keep main thread alive
            if not consumer_thread.is_alive():
                logging.error("Consumer thread died, exiting main.")
                break
            if not summary_thread.is_alive():
                logging.error("Summary Producer thread died, exiting main.")
                break
    except KeyboardInterrupt:
        logging.info("Main thread received KeyboardInterrupt, shutting down.")
    except Exception as e:
        logging.critical(f"Unhandled error in main thread: {e}")
    finally:
        # In a real application, you'd want more robust shutdown signaling.
        # For daemon threads, simply exiting main allows them to terminate.
        logging.info("Application shutdown complete.")
