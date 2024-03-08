
---
# Real-time Log Monitoring System

## Overview

This project demonstrates a real-time log monitoring system using Apache Kafka for data ingestion, Apache Storm for real-time processing, and Apache Kafka for streaming log data. The system performs anomaly detection on incoming log data and emits alerts for anomalies detected.

Data Collected from kaggle: https://www.kaggle.com/code/eliasdabbas/log-file-analysis-with-python/input?select=log_file.log 

## Features

- Ingests streaming log data from Kafka topics.
- Performs real-time processing and anomaly detection using Apache Storm.
- Emits alerts for detected anomalies.
- Easy to integrate and extend with existing Kafka and Storm deployments.

## Requirements

- Python 3.7
- Apache Kafka
- Apache Storm
- Kafka-python library (`pip install kafka-python`)
- Storm library (Installation instructions: [Storm Installation Guide](https://storm.apache.org/releases/2.2.0/Setting-up-development-environment.html))

## Setup

1. Install Apache Kafka and Apache Storm following the official documentation.
2. Install required Python libraries using `pip install kafka-python storm`.
3. Clone repository:

    ```bash
    git clone https://github.com/your_username/real-time-log-monitoring.git
    cd real-time-log-monitoring
    ```

4. Configure Apache Kafka to create a topic named `log-analytics`.
5. Update Kafka broker address in the code if necessary (`bootstrap_servers=['localhost:9092']`).

## Usage

1. Start Apache Kafka and Apache Storm services.
2. Run the Kafka producer script to produce parsed log messages to the `log-topic` Kafka topic:

    ```bash
    python kafka_producer.py
    ```

3. Run the real-time log monitoring script to consume log data from Kafka and perform real-time processing:

    ```bash
    python real_time_log_monitoring.py
    ```

4. Monitor the console output for alerts emitted when anomalies are detected. [You can use dashboards to monitor the logs]
---
