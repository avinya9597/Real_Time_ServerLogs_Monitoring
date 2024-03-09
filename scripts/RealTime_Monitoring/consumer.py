#!/usr/bin/env python
# coding: utf-8

# ### Apache Kafka is a distributed streaming platform that allows for building real-time data pipelines and applications. It is used as the messaging backbone for ingesting and processing streaming log data.

# ### Apache Kafka: Continuously ingest streaming log data into Kafka topics.
# 
# 1. We first set up a Kafka producer to send messages to the Kafka broker running on 'localhost:9092'.
# 2. We define parsed log messages in the form of dictionaries. These messages contain the parsed log data, including hour, status code, response size, error count, and request count.
# 3. We then convert each log message to JSON format and send it to the 'log-topic' Kafka topic using the producer.

# In[1]:


# !pip install kafka-python
from kafka import KafkaConsumer
import json


# In[ ]:


# Set up Kafka producer to send parsed log messages
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Produce parsed log messages to the 'log-analytics' Kafka topic
for log_data in parsed_logs:
    # Convert log data to JSON format
    message = json.dumps(log_data).encode('utf-8')
    
    # Send log message to Kafka
    producer.send('log-analytics', value=message)

# Flush the producer to ensure all messages are sent
producer.flush()


# In[ ]:


#***************************************************************************************************************************

