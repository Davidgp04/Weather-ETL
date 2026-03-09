FROM apache/airflow:3.1.7

USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Ensure Python can see the project root
ENV PYTHONPATH="/opt/airflow"