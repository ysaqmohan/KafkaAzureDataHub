from kafka import KafkaConsumer
from azure.eventhub import EventHubProducerClient, EventData 

#Initialize Kafka Consumer for Topic
consumer = KafkaConsumer('MockStreamTopic')

##EVENT HUB DETAILS
connection_str = 'Endpoint=sb://streamuckafka.servicebus.windows.net/;SharedAccessKeyName=StreamUCPolicy;SharedAccessKey=C1ds2c1s+nsO0OWk+IlGRfDn3IY2pnWVTrXJ1EF2kMs='
eventhub_name = 'streamuckafkaeventhub'
producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
event_data_batch = producer.create_batch()

i=0

for message in consumer:
    try:
        event_data_batch.add(EventData(str(message)))
        print(i, "message added")
        i+=1
    except ValueError:
        print("New batch")
        producer.send_batch(event_data_batch)
        event_data_batch = producer.create_batch()
        print("Sending the batch")
        event_data_batch.add(EventData(str(message)))

print("Sending the batch")
producer.send_batch(event_data_batch)
print("Closing")
producer.close()


    
