from typing import Any, get_origin

import ujson
import xxhash
from pydantic import BaseModel
import redis.asyncio as redis


class RedisStore:
  def __init__(self, redis_config: dict = {}):
    self.redis = redis.Redis(**redis_config)

  async def put(self, items: list):
    pipe = self.redis.pipeline()
    for key, obj in items:
      hashed_key = RedisStore.hash_entity(key, obj.__class__.__name__, "v0")
      encoded = RedisStore.encode(obj)
      pipe.hset(hashed_key, mapping=encoded)

    await pipe.execute()

  async def retrieve(self, items: list) -> list[Any]:
    pipe = self.redis.pipeline()
    for key, class_name in items:
      hashed_key = hashed_key = RedisStore.hash_entity(key, class_name.__name__, "v0")
      pipe.hgetall(hashed_key)

    results = await pipe.execute()

    converted = []
    for (_, class_name), res in zip(items, results):
      if res is None or res == {}:
        converted.append(None)
        continue

      converted.append(RedisStore.decode(res, class_name))

    return converted

  @staticmethod
  def hash_entity(entity_id: str, dtype: str, version: str) -> bytes:
    return (
      xxhash.xxh32_digest(dtype)[:24]
      + xxhash.xxh128_digest(entity_id)[:96]
      + xxhash.xxh32_digest(version)[:8]
    )

  @staticmethod
  def encode(obj: BaseModel) -> dict:
    converted = {}
    original = obj.model_dump(mode="json", exclude_defaults=True)

    for field, info in obj.model_fields.items():
      if field not in original:
        continue

      value = original[field]
      hashed = xxhash.xxh64_digest(field)

      if not info.is_required and (value is info.default or value == info.default):
        continue

      if info.annotation in (int, float, bool):
        converted[hashed] = value

      elif info.annotation == str or isinstance(value, str):
        converted[hashed] = value.encode()

      elif get_origin(info.annotation) in (list, tuple, dict, set):
        converted[hashed] = ujson.dumps(value).encode()

    return converted

  @staticmethod
  def decode(items: dict[str, Any], base_class) -> BaseModel:
    converted = {}

    for field, info in base_class.model_fields.items():
      hashed = xxhash.xxh64_digest(field)
      if hashed not in items:
        continue

      print(field, items[hashed])

      if info.annotation in (int, float, bool):
        converted[field] = items[hashed]

      elif get_origin(info.annotation) in (list, tuple, dict, set):
        converted[field] = ujson.loads(items[hashed].decode())

      else:
        converted[field] = items[hashed].decode()

    return base_class(**converted)
