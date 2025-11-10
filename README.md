# Kafka Sentiment Analysis Consumer

This project implements a Kafka consumer in Python that retrieves tweet data from an input topic, performs sentiment analysis, and then segregates the tweets into positive and negative sentiment topics. It uses `kafka-python` for Kafka interaction and `TextBlob` for sentiment analysis. All Kafka connection details and topic names are configured via environment variables.

## Features

*   **Kafka Integration:** Consumes messages from a specified input topic and produces to two output topics (positive and negative).
*   **Sentiment Analysis:** Uses `TextBlob` to determine the sentiment (positive, negative, or neutral) of tweet content.
*   **Environment-based Configuration:** All sensitive information and Kafka settings are loaded from a `.env` file.
*   **Retries with Backoff:** Implements retry logic with exponential backoff for message processing failures.
*   **Dead-Letter Topic:** Failed messages are sent to a dead-letter topic for further investigation.
*   **Dockerized:** Includes a `Dockerfile` for easy containerization and deployment.

## Project Structure

```
kafka_sentiment_consumer/
├── Dockerfile
├── README.md
├── requirements.txt
├── config/
│   └── .env
└── src/
    └── consumer.py
```

## Prerequisites

*   Python 3.8+
*   Docker (optional, for containerized deployment)
*   Access to a Kafka cluster with SASL_PLAINTEXT and SCRAM-SHA-512 authentication enabled.
*   The following Kafka topics should be created:
    *   `tweets_input` (for incoming tweets)
    *   `tweets_positive` (for positive sentiment tweets)
    *   `tweets_negative` (for negative sentiment tweets)
    *   `tweets_dead_letter` (for messages that failed processing)

## Setup and Installation

1.  **Clone the repository (if applicable):**

    ```bash
    git clone <repository_url>
    cd kafka_sentiment_consumer
    ```

2.  **Create and activate a Python virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    python -m textblob.download_corpora
    ```

4.  **Configure Environment Variables:**

    Edit the `config/.env` file with your Kafka broker details and credentials:

    ```ini
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    KAFKA_USERNAME=your_sasl_username
    KAFKA_PASSWORD=your_sasl_password
    KAFKA_INPUT_TOPIC=tweets_input
    KAFKA_POSITIVE_TOPIC=tweets_positive
    KAFKA_NEGATIVE_TOPIC=tweets_negative
    KAFKA_DEAD_LETTER_TOPIC=tweets_dead_letter
    ```

    *   Replace `localhost:9092` with your Kafka broker address.
    *   Replace `your_sasl_username` and `your_sasl_password` with your actual SASL credentials.

## Running the Consumer

### Locally

Make sure your virtual environment is activated and the `.env` file is configured.

```bash
python src/consumer.py
```

### Using Docker

1.  **Build the Docker image:**

    ```bash
    docker build -t kafka-sentiment-consumer .
    ```

2.  **Run the Docker container:**

    You will need to pass the environment variables to the container. You can do this using the `--env-file` option or by specifying each variable individually.

    ```bash
    docker run --env-file ./config/.env kafka-sentiment-consumer
    ```

    Or, if your Kafka is not running on `localhost` (e.g., in another Docker network or external):

    ```bash
    docker run \
        -e KAFKA_BOOTSTRAP_SERVERS="your_kafka_broker:9092" \
        -e KAFKA_USERNAME="your_sasl_username" \
        -e KAFKA_PASSWORD="your_sasl_password" \
        -e KAFKA_INPUT_TOPIC="tweets_input" \
        -e KAFKA_POSITIVE_TOPIC="tweets_positive" \
        -e KAFKA_NEGATIVE_TOPIC="tweets_negative" \
        -e KAFKA_DEAD_LETTER_TOPIC="tweets_dead_letter" \
        kafka-sentiment-consumer
    ```

## Example Tweet Data Format

The consumer expects messages in the input topic to be JSON objects with at least a `tweetContents` field. For example:

```json
{
    "userId": "c7e4092a-e800-4006-8b0d-d51019359969",
    "userName": "deansamantha",
    "product": "EcoFriendly Water Bottle",
    "tweetContents": "The EcoFriendly Water Bottle leaks constantly. Very disappointed with this purchase.",
    "timestamp": 1762771189858
}
```

## Testing (Manual)

To test the consumer, you can produce messages to the `tweets_input` topic using a Kafka producer. Here's a simple Python script to do that:

First, create a `producer.py` file in your `src` directory (for example) with the following content:

```python
import os
import json
import time
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
import uuid

load_dotenv(dotenv_path='./config/.env') # Adjust path if running from a different directory

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'tweets_input')

SASL_MECHANISM = 'SCRAM-SHA-512'
SECURITY_PROTOCOL = 'SASL_PLAINTEXT'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_tweets = [
    {"userId": str(uuid.uuid4()), "userName": "user1", "product": "Laptop", "tweetContents": "This new laptop is amazing! So fast and reliable.", "timestamp": int(time.time() * 1000)},
    {"userId": str(uuid.uuid4()), "userName": "user2", "product": "Coffee Machine", "tweetContents": "The coffee machine broke after a week. Very disappointed.", "timestamp": int(time.time() * 1000)},
    {"userId": str(uuid.uuid4()), "userName": "user3", "product": "Headphones", "tweetContents": "Sound quality is decent, nothing spectacular.", "timestamp": int(time.time() * 1000)},
    {"userId": str(uuid.uuid4()), "userName": "user4", "product": "Smartphone", "tweetContents": "Best phone I've ever had! Highly recommend it.", "timestamp": int(time.time() * 1000)},
    {"userId": str(uuid.uuid4()), "userName": "user5", "product": "Smartwatch", "tweetContents": "Battery life is terrible, dies in a few hours.", "timestamp": int(time.time() * 1000)}
]

for tweet in test_tweets:
    try:
        print(f"Sending tweet: {tweet['tweetContents']}")
        producer.send(KAFKA_INPUT_TOPIC, tweet)
        time.sleep(1) # Send one tweet per second
    except KafkaError as e:
        print(f"Error sending message: {e}")

producer.flush()
producer.close()
print("Finished sending test tweets.")
```

**Steps to Test:**

1.  Ensure your Kafka cluster is running and topics are created.
2.  Update the `.env` file with correct Kafka details.
3.  Run the `consumer.py` script in one terminal.
4.  Run the `producer.py` script in another terminal to send test data.
5.  Observe the logs of the `consumer.py` to see messages being processed and sent to positive/negative topics.
6.  You can also use a Kafka consumer tool (like `kafka-console-consumer`) to verify messages in `tweets_positive`, `tweets_negative`, and `tweets_dead_letter` topics.

    Example for consuming from output topics:

    ```bash
    # For positive tweets
    kafka-console-consumer --bootstrap-server localhost:9092 \
                           --topic tweets_positive \
                           --from-beginning \
                           --property print.key=true \
                           --consumer.config client.properties

    # For negative tweets
    kafka-console-consumer --bootstrap-server localhost:9092 \
                           --topic tweets_negative \
                           --from-beginning \
                           --property print.key=true \
                           --consumer.config client.properties

    # For dead-letter topic
    kafka-console-consumer --bootstrap-server localhost:9092 \
                           --topic tweets_dead_letter \
                           --from-beginning \
                           --property print.key=true \
                           --consumer.config client.properties
    ```

    **Note:** The `client.properties` file should contain your SASL authentication details if your Kafka cluster requires it. For example:

    ```properties
    security.protocol=SASL_PLAINTEXT
    sasl.mechanism=SCRAM-SHA-512
    sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="your_sasl_username" password="your_sasl_password";
    ```

