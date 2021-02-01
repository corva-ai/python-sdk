from __future__ import annotations

from typing import Any, Optional

import pydantic


class CorvaLambdaClientContext(pydantic.BaseModel):
    @classmethod
    def from_context(cls, context: Any) -> CorvaLambdaClientContext:
        return cls(api_key=getattr(context.client_context, 'api_key', None))

    api_key: Optional[str] = None
