#!/usr/bin/env python
# coding: utf-8

# ## MapReduce Algorithm

# ### Hadoop MapReduce logic  identify patterns and trends. MapReduce jobs  count the occurrence of specific log messages, identify common error patterns, or detect unusual access patterns.

# In[1]:


def map_function(log):
    # Extract relevant information from the log
    url = log['url']
    status_code = log['status_code']
    
    # Emit key-value pairs (pattern, 1)
    if status_code != '200':
        yield ('error', 1)
    
    if '/wp-content/' in url:
        yield ('wp_content', 1)


# In[2]:


def reduce_function(pairs):
    # Aggregate counts for each pattern
    pattern_counts = defaultdict(int)
    for pattern, count in pairs:
        pattern_counts[pattern] += count
    return pattern_counts.items()


# In[ ]:


# parsed logs
logs = parsed_logs

# Map phase
mapped_pairs = [result for log in logs for result in map_function(log)]

# Reduce phase
reduced_output = reduce_function(mapped_pairs)


# ## Output Response from MapReduce

# In[ ]:


# Output the result
for pattern, count in tqdm(reduced_output):
    print(f'{pattern}: {count}')


# In[ ]:


mapped_pairs[0:10]


# In[ ]:


reduced_output


# In[ ]:


#***************************************************************************************************************************

