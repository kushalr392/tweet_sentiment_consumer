
# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt ./requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the configuration directory into the container at /app/config
COPY config/ ./config/

# Copy the src directory into the container at /app/src
COPY src/ ./src/

# Set environment variables for Kafka (optional, can be passed at runtime)
# ENV KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
# ENV KAFKA_USERNAME="your_sasl_username"
# ENV KAFKA_PASSWORD="your_sasl_password"
# ENV KAFKA_INPUT_TOPIC="tweets_input"
# ENV KAFKA_POSITIVE_TOPIC="tweets_positive"
# ENV KAFKA_NEGATIVE_TOPIC="tweets_negative"
# ENV KAFKA_DEAD_LETTER_TOPIC="tweets_dead_letter"

# Command to run the application
CMD ["python", "./src/consumer.py"]
