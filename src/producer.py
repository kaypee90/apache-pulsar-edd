import pulsar
import logging

# create logger
ch = logging.getLogger()
ch.setLevel(logging.DEBUG)

pulsarURL = ''
pulsarJWT = ''
pulsarTopic = ''

client = pulsar.Client(pulsarURL,authentication=pulsar.AuthenticationToken(pulsarJWT))

producer = client.create_producer(pulsarTopic)

producer.send('Hello world!'.encode('utf-8'))

client.close()
