from dine.stores.redis_store import RedisStore
from dine.structs import Entity
from dine.sync_bridge import sync_async_bridge


class FeatureStore:
  def __init__(self, online_store: RedisStore):
    self._online = online_store

  @sync_async_bridge
  async def get_online_features(self, entities: list[Entity]):
    objs = await self._online.retrieve(entities)
    return [x.featurize() if x is not None else None for x in objs]

  @sync_async_bridge
  async def put_online_features(self, entities: list[Entity]):
    await self._online.put(entities)

  @sync_async_bridge
  async def del_online_features(self, entities: list[Entity]):
    await self._online.remove(entities)
