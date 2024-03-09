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


# In[ ]:


# **************************************************************************************************************************

