from datetime import datetime

import pytest
from fakeredis import aioredis

from dine import RedisStore, FeatureStore, Entity
from .models import Order, Item, Category


@pytest.mark.asyncio
async def test_basic_feature_store_flow():
  obj1 = Order(
    date=datetime(2023, 1, 1, 10, 0, 0),
    value=12.34,
    email="buyer@good.store",
    items=[
      Item(category=Category.DAIRY, price=1.29, units=1),
      Item(category=Category.BAKERY, price=0.49, units=3),
    ],
  )

  obj2 = Order(
    date=datetime(2022, 2, 2, 11, 0, 0),
    value=12.34,
    email="mark@good.store",
    items=[
      Item(category=Category.BAKERY, price=0.49, units=3),
      Item(category=Category.DAIRY, price=1.29, units=1),
    ],
  )

  redis = RedisStore()
  redis.redis = aioredis.FakeRedis()

  store = FeatureStore(online_store=redis)

  await store.put_online_features(
    [Entity("test:12345", value=obj1), Entity("test:54321", value=obj2)]
  )
  res = await store.get_online_features(
    [
      Entity(id="test:12345", instance=Order),
      Entity(id="test:54321", instance=Order),
      Entity(id="test:11111", instance=Order),
    ]
  )

  assert isinstance(res, list)
  assert len(res) == 3
  assert res[0] == obj1.featurize()
  assert res[1] == obj2.featurize()
  assert res[2] is None


def test_sync_bridge():
  obj1 = Order(
    date=datetime(2023, 1, 1, 10, 0, 0),
    value=12.34,
    email="buyer@good.store",
    items=[
      Item(category=Category.DAIRY, price=1.29, units=1),
      Item(category=Category.BAKERY, price=0.49, units=3),
    ],
  )

  obj2 = Order(
    date=datetime(2022, 2, 2, 11, 0, 0),
    value=12.34,
    email="mark@good.store",
    items=[
      Item(category=Category.BAKERY, price=0.49, units=3),
      Item(category=Category.DAIRY, price=1.29, units=1),
    ],
  )

  redis = RedisStore()
  redis.redis = aioredis.FakeRedis()

  store = FeatureStore(online_store=redis)

  store.put_online_features(
    [Entity("test:12345", value=obj1), Entity("test:54321", value=obj2)]
  )
  res = store.get_online_features(
    [
      Entity(id="test:12345", instance=Order),
      Entity(id="test:54321", instance=Order),
      Entity(id="test:11111", instance=Order),
    ]
  )

  assert isinstance(res, list)
  assert len(res) == 3
  assert res[0] == obj1.featurize()
  assert res[1] == obj2.featurize()
  assert res[2] is None
