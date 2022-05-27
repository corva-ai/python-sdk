from __future__ import annotations

import contextlib
from typing import Any, ContextManager
from unittest import mock

import pydantic


class AwsEventWithClientContext(pydantic.BaseModel):
    client_context: dict


class Env(pydantic.BaseModel):
    API_KEY: str


class ClientContext(pydantic.BaseModel):
    class Config:
        orm_mode = True

    env: Env


class CorvaContext(pydantic.BaseModel):
    """AWS context, expected by Corva."""

    class Config:
        orm_mode = True

    aws_request_id: str
    client_context: ClientContext

    @property
    def api_key(self) -> str:
        return self.client_context.env.API_KEY

    @classmethod
    def from_aws(cls, aws_event: Any, aws_context: Any) -> CorvaContext:
        parse_ctx: ContextManager = contextlib.nullcontext()

        if aws_context.client_context is None:
            parse_ctx = mock.patch.object(
                aws_context,
                'client_context',
                AwsEventWithClientContext.parse_obj(aws_event).client_context,
            )

        with parse_ctx:
            return CorvaContext.from_orm(aws_context)
