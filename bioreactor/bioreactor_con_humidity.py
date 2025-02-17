#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import json


# In[2]:


connections_file = open("reactor_config/connections.json", "r")
connections = json.load(connections_file)


# TOPIC

# In[3]:


# Define the topic and message
topic = 'humidity'


# CONNECTING TO MONGODB

# In[4]:


# Connect to MongoDB
client = MongoClient(connections["SERVER"])


# In[5]:


# Select the database and collection
db = client[connections["DATABASE"]]
collection = db[topic]


# In[6]:


# Kafka configuration
# Initialize the Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=connections["KAFKA_SERVER"],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


# In[7]:


# Infinite loop to consume messages
try:
    for message in consumer:
        document = message.value
        document["timestamp"] = datetime.strptime(document["timestamp"], '%Y-%m-%d %H:%M:%S.%f')
        #print(document)
        # Insert the document
        result = collection.insert_one(document)
        #print("Saved message from " + str(document["timestamp"]) + " and ID " + str(result.inserted_id))
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
    # Close the connection
    client.close()


# In[ ]:




