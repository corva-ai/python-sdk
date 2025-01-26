import contextlib
import functools
import json
import os
from typing import Any, Dict, Optional, Type

import pydantic

from corva.models.base import RawBaseEvent
from corva.models.merge.raw import RawPartialRerunMergeEvent
from corva.models.scheduled.raw import RawScheduledEvent
from corva.models.stream.raw import RawStreamEvent
from corva.models.task import RawTaskEvent

BASE_EVENT_CLS_TO_APP_TYPE_MAPPING: Dict[str, Type[RawBaseEvent]] = {
    "task": RawTaskEvent,
    "stream": RawStreamEvent,
    "scheduled": RawScheduledEvent,
}


def find_leaf_subclasses(base_class):
    leaf_classes = []
    for subclass in base_class.__subclasses__():
        if not subclass.__subclasses__():
            leaf_classes.append(subclass)
        else:
            leaf_classes.extend(find_leaf_subclasses(subclass))
    return leaf_classes


def guess_event_type(aws_event: Any) -> Optional[Type[RawBaseEvent]]:
    all_children = find_leaf_subclasses(RawBaseEvent)
    for child_cls in all_children:
        with contextlib.suppress(pydantic.ValidationError):
            child_cls.from_raw_event(aws_event)
            return child_cls
    return None


def validate_manifested_type(manifest: Dict[str, Any], app_decorator_type: str) -> None:
    manifest_app_type = manifest.get("application", {}).get("type")
    if manifest_app_type and manifest_app_type != app_decorator_type:
        raise RuntimeError(
            f'Application with type "{manifest_app_type}" '
            f'can\'t invoke a "{app_decorator_type}" handler'
        )


def validate_event_payload(aws_event, app_decorator_type) -> None:

    if event_cls := guess_event_type(aws_event):
        if issubclass(event_cls, RawPartialRerunMergeEvent):
            # RawPartialRerunMergeEvent(-s) should be ignored here since
            # it is not new app type itself it's just a run mode for existing app types
            return

        expected_base_event_cls = BASE_EVENT_CLS_TO_APP_TYPE_MAPPING[app_decorator_type]
        if not issubclass(event_cls, expected_base_event_cls):
            raise RuntimeError(
                f'Application with type "{app_decorator_type}" '
                f'was invoked with "{event_cls}" event type'
            )
    else:
        raise RuntimeError(
            f'Application with type "{app_decorator_type}" '
            'was invoked with "unknown" event type'
        )


@functools.lru_cache(maxsize=None)
def read_manifest() -> Optional[Dict[str, Any]]:
    manifest_json_path = os.path.join(os.getcwd(), "manifest.json")
    if os.path.exists(manifest_json_path):
        with open(manifest_json_path, "r") as manifest_json_file:
            return json.load(manifest_json_file)
    return None


def validate_app_type_context(aws_event: Any, app_decorator_type: str) -> None:
    if manifest := read_manifest():
        validate_manifested_type(manifest, app_decorator_type)
    else:
        validate_event_payload(aws_event, app_decorator_type)
