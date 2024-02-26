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

  def featurize(self) -> dict:
    return {
      "price_per_unit": self.price / self.units,
      "category_enum": self.category.value,
    }


class Order(BaseModel):
  date: datetime
  value: float
  email: EmailStr
  items: list[Item]
  discount_code: str | None = None
  discount_rate: float = 0.0

  def featurize(self) -> dict:
    return {
      "value_per_item": self.value / len(self.items),
      "total_items": len(self.items),
      "total_discount": self.value * self.discount_rate,
    }


class HomeOwnership(Enum):
  RENT = 1
  MORTGAGE = 2
  OWN = 3
  OTHER = 4


class Loan(BaseModel):
  zipcode: int
  person_age: int
  person_income: int
  person_home_ownership: HomeOwnership
  person_emp_length: float
  loan_intent: str
  loan_amnt: int
  loan_int_rate: float
  loan_status: int

  class DineConfig:
    entity_id: str = "loan_id"
    timestamp_field: str = "event_timestamp"
    entity_version: str = "v0"


class Zipcode(BaseModel):
  city: str
  state: str
  location_type: str
  tax_returns_filed: float
  population: int
  total_wages: int

  class DineConfig:
    entity_id: str = "zipcode"
    timestamp_field: str = "event_timestamp"
    entity_version: str = "v0"
