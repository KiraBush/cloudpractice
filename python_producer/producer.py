from time import time
from uuid import uuid4
from confluent_kafka import DeserializingConsumer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

SCHEMA_REGISTRY_URL="http://140.238.221.152:8081"
KAFKA_BROKERS="140.238.221.152:9092"

class Response(object):
    def __init__(self, student, original_message, original_message_ts):
        self.student = "Fedorov V.S."
        self.original_message = original_message
        self.original_message_ts = original_message_ts

class Source(object):
    def __init__(self, message, ts):
        self.message=message
        self.ts=ts

def dict_to_test_message(obj, ctx):
    if obj is None:
        return None

    return Source(message=obj['message'],
                  ts=obj['ts'])

def test_message_to_dict(msg, ctx):
    return dict(student=msg.student,
                original_message=msg.original_message,
                original_message_ts=msg.original_message_ts)

schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
schema_str1 = """ {
"type":"record",
"name":"sourceRecord",
"namespace":"usatu",
"fields":[
{
"name":"message",
"type":"string"
},
{
"name":"ts",
"type":{
"type": "long",
"logicalType": "timestamp-millis"
}
}
]
} """

schema_str2 = """
{
"type":"record",
"name":"responseRecord",
"namespace":"usatu",
"fields":[
{
"name": "student",
"type": "string"
},
{
"name":"original_message",
"type":"string"
},
{
"name":"original_message_ts",
"type":{
"type": "long",
"logicalType": "timestamp-millis"
}
}
]
}

"""

value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
schema_str=schema_str1,
from_dict=dict_to_test_message)

value_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
schema_str=schema_str2,
to_dict=test_message_to_dict)

consumer = DeserializingConsumer({
'bootstrap.servers': KAFKA_BROKERS,
'value.deserializer': value_deserializer,
'group.id': 1703
})

producer = SerializingProducer({
'bootstrap.servers': KAFKA_BROKERS,
'value.serializer': value_serializer
})

def delivery_report(err, msg):
""" Called once for each message produced to indicate delivery result.
Triggered by poll() or flush(). """
    if err is not None:
        print("Delivery failed because, well, {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

if __name__ == "__main__":

    consumer.subscribe(['source'])

    while True:
        try:
    # Timeout for polling is 1 sec
    msg = consumer.poll(1)

if msg is None:
continue

val = msg.value()
if val is not None:
message = Response("Grigorev Artur", val.message, val.ts)
producer.produce(topic='response', value=message, key=str(uuid4()), on_delivery=delivery_report)
producer.flush()
except KeyboardInterrupt:
break

consumer.close()
