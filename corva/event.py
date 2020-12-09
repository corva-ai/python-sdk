from __future__ import annotations

from collections import UserList


class Event(UserList):
    def __eq__(self, other):
        return (
             super().__eq__(other)
             and
             all(
                 type(self_value) == type(other_value)
                 for self_value, other_value in zip(self, other)
             )
        )
