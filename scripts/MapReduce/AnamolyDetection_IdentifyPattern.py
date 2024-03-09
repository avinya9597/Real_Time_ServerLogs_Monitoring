#!/usr/bin/env python
# coding: utf-8

# ## Anomaly Detection
# 
# ### MapReduce can be employed to detect anomalies or outliers in the log data. This could involve analyzing metrics such as request rates, response times, error rates, or resource utilization to identify deviations from normal behavior that may indicate performance issues, security threats, or system failures.
# 

# In[1]:


def map_function_anomaly(log):
    # Extract relevant information from the log
    timestamp = log['timestamp']
    status_code = log['status_code']
    response_size = int(log['response_size'])
    
    # Extract hour from timestamp
    hour = timestamp.split(':')[1]
    
    # Emit key-value pairs (hour, (1, response_size, status_code))
    yield (hour, (1, response_size, status_code))


# In[2]:


def reduce_function_anomaly(pairs):
    # Aggregate counts, total response size, and error counts for each hour
    hour_metrics = defaultdict(lambda: (0, 0, 0))  # (count, total_response_size, error_count)
    for hour, metrics in pairs:
        count, response_size, status_code = metrics
        hour_metrics[hour] = (hour_metrics[hour][0] + count,
                              hour_metrics[hour][1] + response_size,
                              hour_metrics[hour][2] + (1 if status_code != '200' else 0))
    return hour_metrics.items()


# In[ ]:


# parsed logs
logs = parsed_logs


# In[ ]:


# Map phase for anomaly detection
mapped_pairs_anomaly = [result for log in logs for result in map_function_anomaly(log)]


# In[ ]:


mapped_pairs_anomaly[0:5]


# In[ ]:


# Reduce phase for anomaly detection
reduced_output_anomaly = reduce_function_anomaly(mapped_pairs_anomaly)


# In[ ]:


reduced_output_anomaly


# #### The output indicates that for the hour , the average response size and the error rate giving insights into the behavior of your system over time. This information can be valuable for identifying anomalies or trends in the log data.
# 
# #### Hour: The hour of the day for which the metrics are calculated.
# #### Average Response Size: The average size of responses received during that hour.
# #### Error Rate: The percentage of requests that resulted in an error during that hour.

# In[ ]:


# Output the result
for hour, metrics in reduced_output_anomaly:
    count, total_response_size, error_count = metrics
    average_response_size = total_response_size / count if count > 0 else 0
    error_rate = (error_count / count) * 100 if count > 0 else 0
    print(f'Hour: {hour}, Average Response Size: {average_response_size}, Error Rate: {error_rate}%')


# ### Define thresholds for error rate and response size

# In[ ]:


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

# In[ ]:


#***************************************************************************************************************************

