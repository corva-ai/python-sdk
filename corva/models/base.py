from pydantic import BaseModel

from corva.event.event import Event


class BaseContext(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    event: Event
