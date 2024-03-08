## Overview

In IT environments, monitoring and analyzing log data is crucial for maintaining system health, detecting issues, and ensuring optimal performance. However, traditional log monitoring solutions cannot often process log data in real time, leading to delayed detection of anomalies and potential issues.

Ingest streaming log data: The system should be capable of ingesting streaming log data from various sources, such as web servers, applications, and network devices.

Perform real-time processing: Real-time processing of log data is essential for timely detection of anomalies and unusual patterns. The system should be able to process incoming log data in real time, apply relevant transformations, and perform calculations to identify anomalies.

Detect anomalies: Anomaly detection algorithms to identify deviations from normal behavior in the log data which includes detecting unusual request rates, response times, error rates, or resource utilization patterns depending on the input data gathered.

Emit alerts: Upon detecting anomalies, the system should emit alerts or notifications to alert administrators or relevant stakeholders. These alerts should provide actionable insights to address the detected issues promptly.

By addressing these requirements, the real-time log monitoring system aims to improve system reliability, optimize performance, and enhance overall operational efficiency.

_**Data Collected from Kaggle:** https://www.kaggle.com/code/eliasdabbas/log-file-analysis-with-python/input?select=log_file.log_ 

## Objective

The objective of this project is to develop a real-time log monitoring system that can ingest streaming log data, perform real-time processing and anomaly detection, and emit alerts for detected anomalies. The system aims to provide timely insights into system sanity that is health and performance by monitoring and analyzing log data in real time.

## Tech Stack

The real-time log monitoring system leverages a combination of technologies, including Apache Kafka, Apache Storm, and Hadoop MapReduce logic. 
Kafka-python is used for interacting with Kafka, Storm for real-time processing and anomaly detection, and Hadoop MapReduce for batch processing and analysis. 

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
