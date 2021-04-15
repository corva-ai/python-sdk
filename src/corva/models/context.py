import pydantic


class CorvaContext(pydantic.BaseModel):
    aws_request_id: str
    client_context: pydantic.create_model(
        "ClientContext", env=(pydantic.create_model("Env", API_KEY=(str, ...)), ...)
    )

    @property
    def api_key(self) -> str:
        return self.client_context.env.API_KEY
