from typing import List, Union

EVENT_TYPE = Union[
    List[List[dict]],  # scheduled event
    List[dict]  # stream event
]