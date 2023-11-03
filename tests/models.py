from enum import Enum
from typing import NamedTuple
from datetime import datetime

from pydantic import BaseModel, EmailStr


class Category(Enum):
  DAIRY = 1
  BAKERY = 2
  SWEETS = 3
  MEAT = 4
  VEGETABLES = 5
  DRINKS = 6


class Item(NamedTuple):
  category: Category
  price: float
  units: int


class Order(BaseModel):
  date: datetime
  value: float
  email: EmailStr
  items: list[Item]
  discount_code: str | None = None
  discount_rate: float = 0.0
