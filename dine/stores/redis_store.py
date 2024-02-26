from typing import Any, get_origin

import orjson
import xxhash
from pydantic import BaseModel, RootModel
from pydantic_core import PydanticUndefined
import redis.asyncio as redis

from dine.structs import Entity


ENTITY_HASH_LENGTH = 128
FIELD_HASH_LENGTH = 64
DTYPE_HASH_LENGTH = 32
VERSION_HASH_LENGTH = 32


class RedisStore:
  def __init__(self, redis_config: dict = {}):
    self.redis = redis.Redis(**redis_config)

  async def put(self, items: list[Entity]):
    pipe = self.redis.pipeline()
    for item in items:
      hashed_key = RedisStore.hash_entity(item.id, item._name, item._version)
      if item._partial:
        hashed_field = RedisStore.hash_value(item.field, FIELD_HASH_LENGTH)
        pipe.hset(
          hashed_key,
          hashed_field,
          _field_encode(item.value, item.instance.model_fields[item.field].annotation),
        )
      else:
        encoded = RedisStore.encode(item.value)
        pipe.hset(hashed_key, mapping=encoded)

    await pipe.execute()

  async def retrieve(self, items: list[Entity]) -> list[Any]:
    pipe = self.redis.pipeline()
    for item in items:
      hashed_key = hashed_key = RedisStore.hash_entity(
        item.id, item._name, item._version
      )

      if item._partial:
        hashed_field = RedisStore.hash_value(item.field, FIELD_HASH_LENGTH)
        pipe.hget(hashed_key, hashed_field)
      else:
        pipe.hgetall(hashed_key)

    results = await pipe.execute()

    converted = []
    for item, res in zip(items, results):
      if res is None or res == {}:
        converted.append(None)
        continue

      if item._partial:
        converted.append(
          _field_decode(res, item.instance.model_fields[item.field].annotation)
        )
      else:
        converted.append(RedisStore.decode(res, item.instance))

    return converted

  async def remove(self, items: list[Entity]):
    pipe = self.redis.pipeline()
    for item in items:
      hashed_key = RedisStore.hash_entity(item.id, item._name, item._version)
      if item._partial:
        if item.instance.model_fields[item.field].default == PydanticUndefined:
          raise ValueError("Deleting a partial field that do not have a default value.")
        hashed_field = RedisStore.hash_value(item.field, FIELD_HASH_LENGTH)
        pipe.hdel(hashed_key, hashed_field)
      else:
        pipe.delete(hashed_key)

    await pipe.execute()

  @staticmethod
  def hash_entity(entity_id: str, dtype: str, version: str) -> bytes:
    return (
      RedisStore.hash_value(dtype, DTYPE_HASH_LENGTH)
      + RedisStore.hash_value(entity_id, ENTITY_HASH_LENGTH)
      + RedisStore.hash_value(version, VERSION_HASH_LENGTH)
    )

  @staticmethod
  def hash_value(value: str, length: int) -> bytes:
    match length:
      case 32:
        return xxhash.xxh32_digest(value)
      case 64:
        return xxhash.xxh3_64_digest(value)
      case _:
        return xxhash.xxh3_128_digest(value)

  @staticmethod
  def encode(obj: BaseModel) -> dict:
    converted = {}
    original = obj.model_dump(mode="json", exclude_defaults=True)

    for field, info in obj.model_fields.items():
      if field not in original:
        continue

      value = original[field]
      hashed = RedisStore.hash_value(field, FIELD_HASH_LENGTH)

      if not info.is_required and (value is info.default or value == info.default):
        continue

      converted[hashed] = _field_encode(value, info.annotation)

    return converted

  @staticmethod
  def decode(items: dict[str, Any], base_class) -> BaseModel:
    converted = {}

    for field, info in base_class.model_fields.items():
      hashed = RedisStore.hash_value(field, FIELD_HASH_LENGTH)
      if hashed not in items:
        continue

      converted[field] = _field_decode(items[hashed], info.annotation)

    return base_class(**converted)


def _field_encode(value, annotation):
  rm = RootModel[annotation]

  if annotation in (int, float, bool):
    return value

  if annotation == str or isinstance(value, str):
    return value.encode()

  if get_origin(annotation) in (list, tuple, dict, set):
    return rm(value).model_dump_json()

  return orjson.loads(
    rm(value).model_dump_json()
  )  # TODO: assumes basic structure that will be a string once converted, thus, a string within a sting


def _field_decode(value, annotation):
  if annotation in (int, float, bool):
    return annotation(value)

  if get_origin(annotation) in (list, tuple, dict, set):
    return orjson.loads(value)

  return value.decode()
