from pydantic import BaseModel, Extra

from corva.event.event import Event


class BaseContext(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    event: Event


class BaseEventData(BaseModel):
    class Config:
        extra = Extra.allow
