from collections import UserList
from typing import List

from corva.event.data.base import BaseEventData


class Event(UserList):
    def __init__(self, data: List[BaseEventData]):
        super().__init__(data)
