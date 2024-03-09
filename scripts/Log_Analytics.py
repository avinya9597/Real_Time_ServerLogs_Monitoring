#!/usr/bin/env python
# coding: utf-8

# In[52]:


import re
# !pip install mrjob
from collections import defaultdict
from tqdm import tqdm


# ## Data Cleaning - Parse the logs

# #### Provide the log pattern as per the input data (logs)
# 

# In[49]:


log_pattern = r'(?P<ip>\d+\.\d+\.\d+\.\d+) - - \[(?P<timestamp>.*?)\] "(?P<method>\S+) (?P<url>\S+) (?P<http_version>.*?)" (?P<status_code>\d+) (?P<response_size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'


# In[62]:


# Function to parse log entries
def parse_log_entry(log_entry):
    match = re.match(log_pattern, log_entry)
    if match:
        return match.groupdict()
    else:
        return None

# Function to read log file and parse log entries
def parse_log_file(file_path):
    parsed_logs = []
    with open(file_path, 'r') as file:
        for line in file:
            log_entry = line.strip()
            parsed_log_entry = parse_log_entry(log_entry)
            if parsed_log_entry:
                parsed_logs.append(parsed_log_entry)
    return parsed_logs


# In[63]:


file_path = '../Downloads/log_file.log'
parsed_logs = parse_log_file(file_path)
#     for log in parsed_logs:
#         print(log)

if __name__ == "__main__":
    main()


# In[64]:


parsed_logs[0]


# In[36]:


type(parsed_logs)


# ## MapReduce Algorithm

# ### Hadoop MapReduce logic  identify patterns and trends. MapReduce jobs  count the occurrence of specific log messages, identify common error patterns, or detect unusual access patterns.

# In[50]:


def map_function(log):
    # Extract relevant information from the log
    url = log['url']
    status_code = log['status_code']
    
    # Emit key-value pairs (pattern, 1)
    if status_code != '200':
        yield ('error', 1)
    
    if '/wp-content/' in url:
        yield ('wp_content', 1)


# In[51]:


def reduce_function(pairs):
    # Aggregate counts for each pattern
    pattern_counts = defaultdict(int)
    for pattern, count in pairs:
        pattern_counts[pattern] += count
    return pattern_counts.items()


# In[60]:


# parsed logs
logs = parsed_logs

# Map phase
mapped_pairs = [result for log in logs for result in map_function(log)]

# Reduce phase
reduced_output = reduce_function(mapped_pairs)


# ## Output Response from MapReduce

# In[61]:


# Output the result
for pattern, count in tqdm(reduced_output):
    print(f'{pattern}: {count}')


# In[57]:


mapped_pairs[0:10]


# In[59]:


reduced_output


# In[65]:


def map_function_anomaly(log):
    # Extract relevant information from the log
    timestamp = log['timestamp']
    status_code = log['status_code']
    response_size = int(log['response_size'])
    
    # Extract hour from timestamp
    hour = timestamp.split(':')[1]
    
    # Emit key-value pairs (hour, (1, response_size, status_code))
    yield (hour, (1, response_size, status_code))


# In[66]:


def reduce_function_anomaly(pairs):
    # Aggregate counts, total response size, and error counts for each hour
    hour_metrics = defaultdict(lambda: (0, 0, 0))  # (count, total_response_size, error_count)
    for hour, metrics in pairs:
        count, response_size, status_code = metrics
        hour_metrics[hour] = (hour_metrics[hour][0] + count,
                              hour_metrics[hour][1] + response_size,
                              hour_metrics[hour][2] + (1 if status_code != '200' else 0))
    return hour_metrics.items()


# In[73]:


# parsed logs
logs = parsed_logs


# In[74]:


# Map phase for anomaly detection
mapped_pairs_anomaly = [result for log in logs for result in map_function_anomaly(log)]


# In[76]:


mapped_pairs_anomaly[0:5]


# In[77]:


# Reduce phase for anomaly detection
reduced_output_anomaly = reduce_function_anomaly(mapped_pairs_anomaly)


# In[79]:


reduced_output_anomaly


# #### The output indicates that for the hour , the average response size and the error rate giving insights into the behavior of your system over time. This information can be valuable for identifying anomalies or trends in the log data.
# 
# #### Hour: The hour of the day for which the metrics are calculated.
# #### Average Response Size: The average size of responses received during that hour.
# #### Error Rate: The percentage of requests that resulted in an error during that hour.

# In[80]:


# Output the result
for hour, metrics in reduced_output_anomaly:
    count, total_response_size, error_count = metrics
    average_response_size = total_response_size / count if count > 0 else 0
    error_rate = (error_count / count) * 100 if count > 0 else 0
    print(f'Hour: {hour}, Average Response Size: {average_response_size}, Error Rate: {error_rate}%')


# ### Define thresholds for error rate and response size
# 

# In[87]:


# Define thresholds
error_rate_threshold = 5  # 5% error rate threshold
response_size_lower_threshold = 20000  # Lower response size threshold
response_size_upper_threshold = 60000  # Upper response size threshold

# Output the result with anomaly detection
for hour, metrics in reduced_output_anomaly:
    count, total_response_size, error_count = metrics
    average_response_size = total_response_size / count if count > 0 else 0
    error_rate = (error_count / count) * 100 if count > 0 else 0
    
    # Check for anomalies
    anomaly_detected = False
    if error_rate > error_rate_threshold:
        anomaly_detected = True
        anomaly_type = 'High Error Rate'
    elif average_response_size < response_size_lower_threshold:
        anomaly_detected = True
        anomaly_type = 'Low Average Response Size'
    elif average_response_size > response_size_upper_threshold:
        anomaly_detected = True
        anomaly_type = 'High Average Response Size'
    
    # Print the result
    if anomaly_detected:
        print(f'Hour: {hour}, Average Response Size: {average_response_size}, Error Rate: {error_rate}%, Anomaly Detected: {anomaly_type}')


# #### Hour 3 and Hour 4 are flagged as anomalies due to their high average response sizes.
# Hour 5 is flagged as an anomaly due to its high error rate.
# This indicates that during these hours, there were significant deviations from normal behavior, either in terms of response size or error rate, which may require further investigation or action. This approach helps in proactively identifying potential issues or unusual patterns in the log data.

# ## To implement real-time monitoring in conjunction with MapReduce for near-real-time analysis of log data, you can integrate technologies like Apache Kafka for streaming log data and Apache Storm for real-time processing. Here's how you can extend the existing implementation to include real-time monitoring:
# 

# In[ ]:


# !pip install kafka-python
from kafka import KafkaConsumer
from kafka import KafkaProducer
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


# In[91]:


from kafka import KafkaConsumer
from collections import defaultdict
from storm import BasicBolt, Storm

# Set up Kafka consumer to ingest streaming log data
consumer = KafkaConsumer('log-analytics', group_id='log-group', bootstrap_servers=['localhost:9092'])

# Define thresholds
error_rate_threshold = 5  # 5% error rate threshold
response_size_lower_threshold = 20000  # Lower response size threshold
response_size_upper_threshold = 60000  # Upper response size threshold


# In[92]:


# Define a function to determine if log data represents an anomaly
def is_anomaly(log_data):
    error_rate = (log_data['error_count'] / log_data['request_count']) * 100 if log_data['request_count'] > 0 else 0
    average_response_size = log_data['total_response_size'] / log_data['request_count'] if log_data['request_count'] > 0 else 0
    
    if error_rate > error_rate_threshold or average_response_size < response_size_lower_threshold or average_response_size > response_size_upper_threshold:
        return True
    return False


# In[93]:


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


# **************************************************************************************************************************

