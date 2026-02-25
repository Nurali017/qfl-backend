"""Common response schemas shared across endpoints."""

from pydantic import BaseModel


class OkResponse(BaseModel):
    ok: bool


class MessageResponse(BaseModel):
    message: str
