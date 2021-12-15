import os
import pulsar
from dotenv import load_dotenv

load_dotenv()

pulsarURL = os.getenv('PULSAR_URL')
pulsarJWT = os.getenv('PULSAR_JWT')
pulsarTopic = os.getenv('PULSAR_TOPIC')
subscriptionName = os.getenv('SUBSCRIPTION')

client = pulsar.Client(pulsarURL, authentication=pulsar.AuthenticationToken(pulsarJWT))

consumer = client.subscribe(pulsarTopic,subscriptionName)

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
