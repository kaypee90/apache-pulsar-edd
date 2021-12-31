"""
Author: Kaypee90
"""
import os
import pulsar
from dotenv import load_dotenv

load_dotenv()

pulsar_url = os.getenv("PULSAR_URL")
pulsar_jwt = os.getenv("PULSAR_JWT")
pulsar_topic = os.getenv("PULSAR_TOPIC")
subscription_name = os.getenv("SUBSCRIPTION")

client = pulsar.Client(pulsar_url, authentication=pulsar.AuthenticationToken(pulsar_jwt))

consumer = client.subscribe(pulsar_topic, subscription_name)

while True:
    msg = consumer.receive()
    try:
        print("Received message '{}' id='{}'".format(msg.value(), msg.message_id()))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()
