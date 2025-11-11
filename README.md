# Kafka Product Review Sentiment Analyzer and Summary Generator

This application consumes product reviews from a Kafka topic, performs sentiment analysis on them, aggregates sentiment counts for specific products, and periodically publishes a summary report to another Kafka topic.

## Features
- **Kafka Integration**: Consumes messages from an input topic and publishes to sentiment-specific topics and summaries to an output topic.
- **Sentiment Analysis**: Uses `TextBlob` to determine the sentiment (positive/negative/neutral) of product reviews.
- **Product-Specific Aggregation**: Tracks positive and negative review counts for a predefined list of products.
- **5-Star Rating Calculation**: Calculates a 5-star rating for each product based on aggregated sentiment.
- **Timed Summary Reports**: Publishes summary reports every 10 seconds to a dedicated summary topic.
- **Environment-based Configuration**: All Kafka connection details and topic names are configurable via environment variables.
- **Retry Mechanism**: Includes retry logic with exponential backoff for message processing failures.
- **Dead-Letter Topic**: Unprocessable or failed messages are sent to a dead-letter topic for further investigation.
- **Docker Support**: Provided `Dockerfile` for easy containerization.

## Prerequisites
- Docker (recommended for easy setup)
- Apache Kafka (running instance)
- Python 3.9+

## Setup

1.  **Clone the repository (if not already done by the agent):**
    ```bash
    git clone <repository-url>
    cd kafka_sentiment_consumer
    ```

2.  **Create and Configure `.env` file:**
    Copy the example environment file and fill in your Kafka broker details and credentials.
    ```bash
    cp config/.env.sample config/.env
    ```
    Edit `config/.env` with your specific Kafka setup:
    ```ini
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS="your_kafka_broker_address:9092" # e.g., localhost:9092 or a Confluent Cloud broker
    KAFKA_USERNAME="your_sasl_username"
    KAFKA_PASSWORD="your_sasl_password"

    # Kafka Topics
    KAFKA_REVIEW_INPUT_TOPIC="product_reviews"
    KAFKA_POSITIVE_TOPIC="product_reviews_positive"
    KAFKA_NEGATIVE_TOPIC="product_reviews_negative"
    KAFKA_SUMMARY_TOPIC="product_review_summaries"
    KAFKA_DEAD_LETTER_TOPIC="review_dead_letter"

    # Consumer Group ID
    CONSUMER_GROUP_ID="product_sentiment_analysis_group"
    ```
    **Note on Authentication**: The application is configured to use `SASL_PLAINTEXT` with `SCRAM-SHA-512` mechanism by default. If your Kafka broker requires `SASL_SSL`, you would need to adjust `SECURITY_PROTOCOL` in `src/consumer.py` and `src/summary_producer.py`.

3.  **Install Python Dependencies (if running without Docker):**
    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

### Using Docker (Recommended)

1.  **Build the Docker image:**
    ```bash
    docker build -t kafka-sentiment-consumer .
    ```

2.  **Run the Docker container:**
    Ensure your Kafka broker is accessible from where Docker is running.
    ```bash
    docker run --env-file ./config/.env kafka-sentiment-consumer
    ```
    The `--env-file` flag passes your environment variables from `config/.env` into the container.

### Running Locally (without Docker)

```bash
python -m src.main
```

## Kafka Topics

-   **`KAFKA_REVIEW_INPUT_TOPIC` (default: `product_reviews`)**: The topic from which product review messages are consumed.
-   **`KAFKA_POSITIVE_TOPIC` (default: `product_reviews_positive`)**: The topic to which positive product review messages are published.
-   **`KAFKA_NEGATIVE_TOPIC` (default: `product_reviews_negative`)**: The topic to which negative product review messages are published.
-   **`KAFKA_SUMMARY_TOPIC` (default: `product_review_summaries`)**: The topic to which aggregated sentiment summaries are published every 10 seconds.
-   **`KAFKA_DEAD_LETTER_TOPIC` (default: `review_dead_letter`)**: The topic for messages that failed processing after multiple retries or were malformed.

## Message Schemas

### Input Message Schema (`KAFKA_REVIEW_INPUT_TOPIC`)

Expected to be JSON messages with `product` and `tweetContents` fields (the example data format provided by the user):

```json
{
    "userId": "c7e4092a-e800-4006-8b0d-d51019359969",
    "userName": "deansamantha",
    "product": "EcoFriendly Water Bottle",
    "tweetContents": "The EcoFriendly Water Bottle leaks constantly. Very disappointed with this purchase.",
    "timestamp": 1762771189858
}
```

### Sentiment Output Message Schema (`KAFKA_POSITIVE_TOPIC`, `KAFKA_NEGATIVE_TOPIC`)

Published as JSON messages, which are the original review messages with an added `sentiment` field:

```json
{
    "userId": "c7e4092a-e800-4006-8b0d-d51019359969",
    "userName": "deansamantha",
    "product": "EcoFriendly Water Bottle",
    "tweetContents": "The EcoFriendly Water Bottle leaks constantly. Very disappointed with this purchase.",
    "timestamp": 1762771189858,
    "sentiment": "negative" 
}
```

### Output Summary Message Schema (`KAFKA_SUMMARY_TOPIC`)

Published as JSON messages containing aggregated sentiment data:

```json
{
    "productName": "SmartSpeaker Pro",
    "positiveReviews": 150,
    "negativeReviews": 10,
    "fiveStarRating": 4.75, 
    "timestamp": 1678886410.12345
}
```

## Products Monitored

The application specifically tracks sentiment for the following products:
- SmartSpeaker Pro
- EcoFriendly Water Bottle
- SmartDoorbell Connect
- Fitness Tracker X10
- Gourmet Coffee Maker
