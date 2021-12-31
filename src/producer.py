import os
import pulsar
import logging
from dotenv import load_dotenv

load_dotenv()

# create logger
ch = logging.getLogger()
ch.setLevel(logging.DEBUG)

pulsarURL = os.getenv("PULSAR_URL")
pulsarJWT = os.getenv("PULSAR_JWT")
pulsarTopic = os.getenv("PULSAR_TOPIC")

client = pulsar.Client(pulsarURL, authentication=pulsar.AuthenticationToken(pulsarJWT))

producer = client.create_producer(pulsarTopic)

producer.send("Hello world!".encode("utf-8"))

client.close()
