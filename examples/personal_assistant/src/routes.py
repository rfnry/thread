from urllib.parse import unquote

from fastapi import APIRouter, WebSocket
from pydantic import BaseModel

import server

router = APIRouter()
ORIGINS = {"http://localhost:8000", "http://127.0.0.1:8000"}


class Nudge(BaseModel):
    to: str
    about: str


@router.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    name = unquote(websocket.cookies.get("name", "")).strip()
    await websocket.accept()
    if not name or websocket.headers.get("origin") not in ORIGINS:
        await websocket.close(code=1008)
        return
    await server.serve(websocket, name)


@router.post("/nudge")
async def nudge(body: Nudge) -> dict:
    return await server.nudge(body.to, body.about)
