# Use a lightweight Python image
FROM python:3.9-slim-buster

# Set working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source code and configuration
COPY src ./src
COPY config ./config

# Command to run the application
# Using python -m src.main allows Python to find modules correctly
CMD ["python", "-m", "src.main"]
