"""kafka-python 3.x serializers for JSON payloads.

Why classes instead of lambdas: kafka-python 3.x emits a DeprecationWarning
when a plain callable is passed as ``value_serializer`` / ``value_deserializer``;
it wants objects implementing ``kafka.serializer.Serializer`` /
``Deserializer``. Using these classes keeps the producer/consumer warning-free.
"""

import json

from kafka.serializer import Deserializer, Serializer


class JSONSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return json.dumps(data).encode("utf-8")


class JSONDeserializer(Deserializer):
    def deserialize(self, topic, headers, data):
        return json.loads(data.decode("utf-8"))
