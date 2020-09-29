from azure.eventhub import EventHubConsumerClient
import logging

##EVENT HUB DETAILS
connection_str = 'Endpoint=sb://streamuckafka.servicebus.windows.net/;SharedAccessKeyName=StreamUCPolicy;SharedAccessKey=C1ds2c1s+nsO0OWk+IlGRfDn3IY2pnWVTrXJ1EF2kMs='
eventhub_name = 'streamuckafkaeventhub'
consumer_group = "$default"
client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)

logger = logging.getLogger("azure.eventhub")
logging.basicConfig(level=logging.INFO)

def on_event(partition_context, event):
    logger.info("Received event from partition {}".format(partition_context.partition_id))
    logger.info("Message received:{}".format(event))
    partition_context.update_checkpoint(event)

with client:
    client.receive(on_event=on_event, starting_position="-1")
