from typing import Any
from pydantic import BaseModel, RootModel
from pydantic._internal._model_construction import ModelMetaclass


class Entity:
  def __init__(
    self,
    id: str,
    value: BaseModel | Any | None = None,
    instance: ModelMetaclass | None = None,
    field: str | None = None,
  ):
    if field is None and value is not None:
      if not isinstance(value, BaseModel):
        raise ValueError("Passed value is not a BaseModel object.")

    if value is None and instance is None:
      raise ValueError("Either value and instance should be set. Pass one of them.")

    if isinstance(value, BaseModel) and instance is not None:
      raise ValueError("Both value and instance cannot be passed. Pass one of them.")

    if field is not None and instance is None:
      raise ValueError(
        "A key without instance cannot be retrieved/set. Provide instance."
      )

    if instance is not None and not isinstance(instance, ModelMetaclass):
      raise ValueError("Passed instance should be BaseModel class type.")

    self.id = id
    self.value = value
    self.instance = instance
    self.field = field

    if self.field is not None and self.value is not None:
      self._validate()

  @property
  def _name(self) -> str:
    if self.instance is not None:
      return self.instance.__name__

    return self.value.__class__.__name__

  @property
  def _version(self) -> str:
    return "v0"

  @property
  def _partial(self) -> bool:
    return self.field is not None

  def _validate(self):
    rm = RootModel[self.instance.model_fields[self.field].annotation]
    rm.model_validate(self.value)
