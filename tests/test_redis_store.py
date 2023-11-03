from datetime import datetime
from itertools import product

import pytest

from dine import RedisStore
from .models import Order, Item, Category


def test_unique_keys():
  entity = "qwerty:123456"
  dtypes = ["test", "bla", "teest", "ext"]
  versions = [f"v{i}" for i in range(1, 10_000)]
  combos = list(product(dtypes, versions))

  keys = [RedisStore.hash_entity(entity, d, v) for d, v in combos]
  assert len(keys) == len(set(keys))


def test_order_encoding():
  obj = Order(
    date=datetime(2023, 1, 1, 10, 0, 0),
    value=12.34,
    email="buyer@good.store",
    items=[
      Item(category=Category.DAIRY, price=1.29, units=1),
      Item(category=Category.BAKERY, price=0.49, units=3),
    ],
  )
  encoded = RedisStore.encode(obj)
  decoded = RedisStore.decode(encoded, Order)
  assert obj == decoded


@pytest.mark.asyncio
async def test_redis_upload():
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

  store = RedisStore()
  await store.put([("test:12345", obj1), ("test:54321", obj2)])
  res = await store.retrieve(
    [("test:12345", Order), ("test:54321", Order), ("test:111111", Order)]
  )

  assert obj1 == res[0]
  assert obj1.items == res[0].items
  assert obj2 == res[1]
  assert obj2.items == res[1].items
  assert res[2] is None
