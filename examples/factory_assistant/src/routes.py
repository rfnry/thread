from typing import Annotated
from urllib.parse import unquote

from fastapi import APIRouter, Cookie, Depends, WebSocket, WebSocketException

import server

router = APIRouter()
ORIGINS = {"http://localhost:8000", "http://127.0.0.1:8000"}


def signed_in(name: Annotated[str, Cookie()]) -> str:
    return unquote(name).strip()


Name = Annotated[str, Depends(signed_in)]


@router.websocket("/ws")
async def ws(websocket: WebSocket, name: Name) -> None:
    if not name or websocket.headers.get("origin") not in ORIGINS:
        raise WebSocketException(code=1008)
    await websocket.accept()
    await server.serve(websocket, name)


@router.get("/channels")
async def channels() -> list[dict]:
    return await server.channels()


@router.post("/channels/{channel_id}/join")
async def join(channel_id: str, name: Name) -> dict:
    return await server.join(name, channel_id)
