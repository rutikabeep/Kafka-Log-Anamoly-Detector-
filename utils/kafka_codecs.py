import json


def json_serializer(value):
    return json.dumps(value).encode("utf-8")


def json_deserializer(value):
    return json.loads(value.decode("utf-8"))


def utf8_serializer(value):
    return value.encode("utf-8") if value else None


def utf8_deserializer(value):
    return value.decode("utf-8") if value else None
