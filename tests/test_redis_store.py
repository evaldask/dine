from datetime import datetime
from itertools import product

import pytest

from dine import RedisStore, Entity
from .models import Order, Item, Category


def test_unique_keys():
  entity = "qwerty:123456"
  dtypes = ["test", "bla", "teest", "ext"]
  versions = [f"v{i}" for i in range(1, 10_000)]
  combos = list(product(dtypes, versions))

  keys = [RedisStore.hash_entity(entity, d, v) for d, v in combos]
  assert len(keys) == len(set(keys))


def test_store_hashing():
  entity = "qwerty:123456"
  dtype = "Order"
  version = "v0"

  value = RedisStore.hash_entity(entity, dtype, version)
  assert isinstance(value, bytes)
  assert len(value) == 24


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
  await store.put([Entity("test:12345", value=obj1), Entity("test:54321", value=obj2)])
  res = await store.retrieve(
    [
      Entity(id="test:12345", instance=Order),
      Entity(id="test:54321", instance=Order),
      Entity(id="test:11111", instance=Order),
    ]
  )

  assert obj1 == res[0]
  assert obj1.items == res[0].items
  assert obj2 == res[1]
  assert obj2.items == res[1].items
  assert res[2] is None


@pytest.mark.asyncio
async def test_partial_operations():
  obj = Order(
    date=datetime(2023, 1, 1, 10, 0, 0),
    value=12.34,
    email="buyer@good.store",
    items=[
      Item(category=Category.DAIRY, price=1.29, units=1),
      Item(category=Category.BAKERY, price=0.49, units=3),
    ],
  )

  store = RedisStore()
  await store.put([Entity("test:12345", value=obj)])
  await store.put(
    [
      Entity(id="test:12345", value="other@email.com", instance=Order, field="email"),
      Entity(id="test:12345", value=11.22, instance=Order, field="value"),
      Entity(
        id="test:12345",
        value=datetime(2020, 2, 2, 12, 0, 0),
        instance=Order,
        field="date",
      ),
      Entity(
        id="test:12345",
        value=[Item(category=Category.VEGETABLES, price=0.11, units=50)],
        instance=Order,
        field="items",
      ),
    ]
  )

  results = await store.retrieve(
    [
      Entity(id="test:12345", instance=Order, field="email"),
      Entity(id="test:12345", instance=Order, field="value"),
      Entity(id="test:12345", instance=Order),
    ]
  )

  assert results[0] == "other@email.com"
  assert results[1] == 11.22

  assert results[2] == Order(
    date=datetime(2020, 2, 2, 12, 0, 0),
    value=11.22,
    email="other@email.com",
    items=[Item(category=Category.VEGETABLES, price=0.11, units=50)],
  )
