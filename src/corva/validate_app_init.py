import contextlib
import functools
import json
import os
from typing import Any, Dict, Optional, Type

import pydantic

from corva.models.base import AppType, RawBaseEvent


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


def validate_manifested_type(
    manifest_app_type: AppType, raw_event_type: Type[RawBaseEvent]
) -> None:
    if manifest_app_type != raw_event_type.get_app_type():
        raise RuntimeError(
            f'Application with type "{manifest_app_type.value}" '
            f'can\'t invoke a "{raw_event_type.get_app_type().value}" handler'
        )


def validate_event_payload(aws_event: Any, raw_event_type: Type[RawBaseEvent]) -> None:

    if event_cls := guess_event_type(aws_event):
        if not issubclass(event_cls, raw_event_type):
            raise RuntimeError(
                f'Application with type "{raw_event_type.get_app_type().value}" '
                f'was invoked with "{event_cls}" event type'
            )


@functools.lru_cache(maxsize=1)
def read_manifest() -> Optional[Dict[str, Any]]:
    manifest_json_path = os.path.join(os.getcwd(), "manifest.json")
    if os.path.exists(manifest_json_path):
        with open(manifest_json_path, "r") as manifest_json_file:
            return json.load(manifest_json_file)
    return None


def get_manifested_type() -> Optional[AppType]:
    if manifest := read_manifest():
        application_type = manifest.get("application", {}).get("type")
        return AppType(application_type) if application_type else None
    return None


def validate_app_type_context(
    aws_event: Any, raw_event_type: Type[RawBaseEvent]
) -> None:
    if manifested_type := get_manifested_type():
        validate_manifested_type(manifested_type, raw_event_type)
    else:
        validate_event_payload(aws_event, raw_event_type)
