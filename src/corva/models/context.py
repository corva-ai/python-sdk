import pydantic


class CorvaContext(pydantic.BaseModel):
    """AWS context, expected by Corva."""

    aws_request_id: str
    client_context: pydantic.create_model(
        "ClientContext",  # noqa: F821
        env=(pydantic.create_model("Env", API_KEY=(str, ...)), ...),  # noqa: F821
    )

    @property
    def api_key(self) -> str:
        return self.client_context.env.API_KEY
