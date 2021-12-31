"""
Author: Kaypee90
"""

import os
import pulsar
import logging
from dotenv import load_dotenv

load_dotenv()

# create logger
ch = logging.getLogger()
ch.setLevel(logging.DEBUG)

pulsar_url = os.getenv("PULSAR_URL")
pulsar_jwt = os.getenv("PULSAR_JWT")
pulsar_topic = os.getenv("PULSAR_TOPIC")

client = pulsar.Client(pulsar_url, authentication=pulsar.AuthenticationToken(pulsar_jwt))

producer = client.create_producer(pulsar_topic)

producer.send("Hello world!".encode("utf-8"))

client.close()
