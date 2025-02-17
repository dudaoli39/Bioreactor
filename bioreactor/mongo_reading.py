#!/usr/bin/env python
# coding: utf-8

# https://www.mongodb.com/docs/rapid/core/timeseries/timeseries-procedures/ <br>
# https://www.w3schools.com/python/python_mongodb_insert.asp <br>
# https://www.mongodb.com/docs/languages/python/pymongo-arrow-driver/current/quick-start/#std-label-pymongo-arrow-quick-start

# In[57]:


from pymongo import MongoClient
from datetime import datetime
import json


# In[58]:


reactor_name = "01"

topic = 1

start = "2025-02-16 02:50:00"
finish = "2025-02-16 02:51:00"


# In[59]:


connections_file = open("reactor_config/connections.json", "r")
connections = json.load(connections_file)


# In[60]:


# Connect to MongoDB
client = MongoClient(connections["SERVER"])


# In[61]:


# Select the database and collection
db = client[connections["DATABASE"]]


# In[62]:


query_topics = {
    1: "temperature",
    2: "pressure",
    3: "humidity",
    4: "oxigen"
}
query_sensors = {
    "temperature": "s_temp_"+reactor_name,
    "pressure": "s_pres_"+reactor_name,
    "humidity": "s_humi_"+reactor_name,
    "oxigen": "s_oxig_"+reactor_name,
}


# In[63]:


collection = db[query_topics[topic]]


# In[64]:


# Query time series data
start_time = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
end_time = datetime.strptime(finish, "%Y-%m-%d %H:%M:%S")


# In[65]:


query = {
    "timestamp": {'$gte': start_time, '$lte': end_time},
    "metadata.sensor": query_sensors[query_topics[topic]]
}

results = collection.find(query)


# In[66]:


for record in results:
    print(record)


# In[67]:


client.close()

