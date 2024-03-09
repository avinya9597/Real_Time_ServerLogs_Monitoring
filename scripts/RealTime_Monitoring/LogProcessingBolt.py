#!/usr/bin/env python
# coding: utf-8

# <!-- We've integrated Apache Storm into the system by defining a Storm bolt (LogProcessingBolt) to perform real-time processing of log data.
# The Kafka consumer continuously consumes log data from the Kafka topic, and each log message is emitted to the Storm topology for processing.
# Inside the Storm bolt, real-time processing and anomaly detection logic can be implemented. If an anomaly is detected, an alert can be emitted using the emit_alert function.
# This setup allows for scalable, fault-tolerant, and real-time processing of log data using Apache Storm in conjunction with Apache Kafka. -->

# ## We've integrated Apache Storm into the system by defining a Storm bolt (LogProcessingBolt) to perform real-time processing of log data.
# ## The Kafka consumer continuously consumes log data from the Kafka topic, and each log message is emitted to the Storm topology for processing.
# ## Inside the Storm bolt, real-time processing and anomaly detection logic can be implemented. If an anomaly is detected, an alert can be emitted using the emit_alert function.
# ## This setup allows for scalable, fault-tolerant, and real-time processing of log data using Apache Storm in conjunction with Apache Kafka.

# In[3]:


# !pip install kafka-python
# !pip install storm
from kafka import KafkaProducer
import json


# In[5]:


from kafka import KafkaConsumer
from collections import defaultdict
from storm import BasicBolt, Storm


# In[ ]:


# Set up Kafka consumer to ingest streaming log data
consumer = KafkaConsumer('log-analytics', group_id='log-group', bootstrap_servers=['localhost:9092'])

# Define thresholds
error_rate_threshold = 5  # 5% error rate threshold
response_size_lower_threshold = 20000  # Lower response size threshold
response_size_upper_threshold = 60000  # Upper response size threshold


# In[6]:


# Define a function to determine if log data represents an anomaly
def is_anomaly(log_data):
    error_rate = (log_data['error_count'] / log_data['request_count']) * 100 if log_data['request_count'] > 0 else 0
    average_response_size = log_data['total_response_size'] / log_data['request_count'] if log_data['request_count'] > 0 else 0
    
    if error_rate > error_rate_threshold or average_response_size < response_size_lower_threshold or average_response_size > response_size_upper_threshold:
        return True
    return False


# In[7]:


# Define a function to emit an alert for the detected anomaly
def emit_alert(log_data):
    print("Anomaly detected!")
    print("Log data:", log_data)


# In[ ]:


# Define a Storm bolt to perform real-time processing
class LogProcessingBolt(BasicBolt):
    def process(self, tup):
        log_data = tup.values[0]
        
        # Perform real-time processing and anomaly detection logic here
        # Update metrics
         hour_metrics[log_data['hour']] = (hour_metrics[log_data['hour']][0] + 1,
                                      hour_metrics[log_data['hour']][1] + int(log_data['response_size']),
                                      hour_metrics[log_data['hour']][2] + (1 if log_data['status_code'] != '200' else 0))
        
        # If an anomaly is detected, emit an alert
        if is_anomaly(log_data):
            emit_alert(log_data)


# In[ ]:


# Set up Apache Storm topology
storm = Storm()
storm.add_bolt(LogProcessingBolt)

# Continuously consume streaming log data
for message in consumer:
    log_data = message.value  # Assuming log data is in JSON format
    
    # Emit log data to Storm for processing
    storm.emit({'log_data': log_data})


# In[ ]:


#***************************************************************************************************************************

