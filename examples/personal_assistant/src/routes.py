from urllib.parse import unquote

from fastapi import APIRouter, WebSocket, WebSocketException
from pydantic import BaseModel

import harness

router = APIRouter()
ORIGINS = {"http://localhost:8000", "http://127.0.0.1:8000"}


class Nudge(BaseModel):
    to: str
    about: str


@router.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    name = unquote(websocket.cookies.get("name", "")).strip()
    if not name or websocket.headers.get("origin") not in ORIGINS:
        raise WebSocketException(code=1008)
    await websocket.accept()
    await harness.serve(websocket, name)


@router.post("/nudge")
async def nudge(body: Nudge) -> dict:
    return await harness.nudge(body.to, body.about)
