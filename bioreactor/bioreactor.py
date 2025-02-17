#!/usr/bin/env python
# coding: utf-8

# Create the default settings for a bio-reactor<BR>
# This Bio-Reactor will have a group of sensors that will generate random data in a certain defined range to emulate a reactor<BR>
# There will also be a group of associations between those sensors to bring more real-like situation<BR>
# Each sensor will generate a value each X seconds that are configurable by a CONFIG file<BR>

# In[1]:


from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime


# In[2]:


bio_config_file = open("reactor_config/configurations.json", "r")
bio_config = json.load(bio_config_file)

connections_file = open("reactor_config/connections.json", "r")
connections = json.load(connections_file)


# INDICATE THE REACTOR NAME TO COLLECT THE DATA FROM

# In[3]:


reactor_name = "01"


# PREPARING PRODUCER TO KAFKA

# In[4]:


# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=connections["KAFKA_SERVER"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# CREATING THE MESSAGE TO BE DELIVERED

# In[5]:


def create_message(sensor, value, time_run):
    message = {
        "timestamp": time_run,
        "metadata": {"sensor" : sensor},
        "value": value
    }
    return message
    


# In[6]:


def send_message(topic, message):
    # Send the message
    producer.send(topic, message)
    # Ensure all messages are sent
    producer.flush()
    #print("-- message " + str(repeat+1) + " sent...")


# CREATING THE RELATIONSHIP FUNCTIONS
# TEMPERATURE WILL BE THE BASE

# In[7]:


def temperature_humidity(before, now):
    variation = now - before
    #1 -> json
    #var -> y
    #y = json x var
    return variation * bio_config["TEMPERATURE_HUMIDITY"]
    


# In[8]:


def humidity_pressure(before, now):
    variation = now - before
    #1 -> json
    #var -> y
    #y = json x var
    return variation * bio_config["HUMIDITY_PRESSURE"]
    


# In[9]:


def temperature_oxigen(before, now):
    variation = now - before
    #1 -> json
    #var -> y
    #y = json x var
    return variation * bio_config["TEMPERATURE_OXIGEN"]
    


# In[10]:


def pressure_oxigen(before, now):
    variation = now - before
    #1 -> json
    #var -> y
    #y = json x var
    return variation * bio_config["PRESSURE_OXIGEN"]
    


# CREATING THE TEMPERATURE SENSOR

# SETTING UP RANDOM INITIAL VARIABLES FOR THE SENSORS

# In[11]:


temperature = random.uniform(bio_config["TEMPERATURE_MIN"], bio_config["TEMPERATURE_MAX"])


# In[12]:


pressure = random.uniform(bio_config["PRESSURE_MIN"], bio_config["PRESSURE_MAX"])


# In[13]:


humidity = random.uniform(bio_config["HUMIDITY_MIN"], bio_config["HUMIDITY_MAX"])


# In[14]:


oxigen = random.uniform(bio_config["OXIGEN_MIN"], bio_config["OXIGEN_MAX"])


# In[ ]:





# RUNNING THE BIOREACTOR

# In[15]:


#creating accepctance variation OUT of the ideal limits of temperature
limit_temperature_low = (bio_config["TEMPERATURE_MIN"] - (bio_config["TEMPERATURE_MIN"]*bio_config["ACCEPTED_VARIANCE"]))
limit_temperature_high = (bio_config["TEMPERATURE_MAX"] + (bio_config["TEMPERATURE_MAX"]*bio_config["ACCEPTED_VARIANCE"]))

# Infinite loop to consume messages
try:
    while True:
        #print(str(repeat+1)+" - RUN")
        
        time_run = str(datetime.now())

        # randomly select a variation for temperature and then calculate the variation for other parameters based on rules
        variation = random.uniform(-bio_config["RANDOM_IMPACT"], bio_config["RANDOM_IMPACT"])
        new_temperature = temperature + variation
        
        # need to put a trigger in case the variation is out of the expected % range, otherwise the readings can be too weird 
        if ((new_temperature < limit_temperature_low) or (new_temperature > limit_temperature_high)):
            variation = variation * -1
            new_temperature = temperature + variation

        new_humidity = humidity + temperature_humidity(temperature, new_temperature)
        new_pressure = pressure + humidity_pressure(humidity, new_humidity)
        new_oxigen = oxigen + (temperature_oxigen(temperature, new_temperature) + pressure_oxigen(pressure, new_pressure))/2

        #update the new values for each sensor
        temperature = new_temperature
        humidity = new_humidity
        pressure = new_pressure
        oxigen = new_oxigen

        #send the new sensor values to messaging service
        message = create_message("s_temp_"+reactor_name, temperature, time_run)
        send_message("temperature", message)
        message = create_message("s_humi_"+reactor_name, humidity, time_run)
        send_message("humidity", message)
        message = create_message("s_pres_"+reactor_name, pressure, time_run)
        send_message("pressure", message)
        message = create_message("s_oxig_"+reactor_name, oxigen, time_run)
        send_message("oxigen", message)

        #wait to start all over based on frequence
        time.sleep(bio_config["FREQUENCE"])
        
except KeyboardInterrupt:
    print("Bioreactor stopped.")
finally:
    producer.close()
    bio_config_file.close()
    connections_file.close()

# In[ ]:




