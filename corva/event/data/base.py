from pydantic.main import BaseModel, Extra


class BaseEventData(BaseModel):
    class Config:
        extra = Extra.allow
