import requests
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

with requests.get("http://127.0.0.1:5000/very_large_request/1000", stream=True) as r:
    buffer = ""
    for chunk in r.iter_content(chunk_size=1):
        if chunk.endswith(b'\n'):
            t = eval(buffer)
            t = str(t[0]) + "|" + str(t[1]) + "|" + str(t[2])
            buffer = ""
            producer.send('MockStreamTopic',str.encode(t))
        else:
            buffer +=chunk.decode()
