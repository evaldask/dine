from datetime import datetime

import pytest
from pydantic import ValidationError
from dine import Entity
from .models import Order, Item, Category


def test_initialization():
  obj = Order(
    date=datetime(2023, 1, 1, 10, 0, 0),
    value=12.34,
    email="buyer@good.store",
    items=[
      Item(category=Category.DAIRY, price=1.29, units=1),
      Item(category=Category.BAKERY, price=0.49, units=3),
    ],
  )

  ent1 = Entity(id="123", value=obj)
  assert ent1._partial is False

  ent2 = Entity(id="123", instance=Order)
  assert ent2._partial is False

  ent3 = Entity(id="123", instance=Order, field="date")
  assert ent3._partial is True

  ent4 = Entity(
    id="123", value=datetime(2024, 1, 1, 10, 0, 0), instance=Order, field="date"
  )
  assert ent4._partial is True

  with pytest.raises(ValueError):
    Entity(id="123", value=obj, instance=Order)

  with pytest.raises(ValueError):
    Entity(id="123", value=datetime(2024, 1, 1, 10, 0, 0), instance=Order)

  with pytest.raises(ValueError):
    Entity(id="123", value=datetime(2024, 1, 1, 10, 0, 0), field="date")

  with pytest.raises(ValueError):
    Entity(id="123", value=Order)

  with pytest.raises(ValueError):
    Entity(id="123", instance=obj)


def test_entity_value_validation():
  Entity("123", value=datetime(2023, 1, 1, 10, 0, 0), instance=Order, field="date")
  Entity("123", value="good@email.com", instance=Order, field="email")

  with pytest.raises(ValidationError):
    Entity("123", value=datetime(2023, 1, 1, 10, 0, 0), instance=Order, field="email")

  with pytest.raises(ValidationError):
    Entity("123", value="i'm email", instance=Order, field="email")
