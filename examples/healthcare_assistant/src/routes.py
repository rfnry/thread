from typing import Annotated
from urllib.parse import unquote

from fastapi import APIRouter, Cookie, Depends, WebSocket
from pydantic import BaseModel

import server

router = APIRouter()
ORIGINS = {"http://localhost:8000", "http://127.0.0.1:8000"}


def signed_in(name: Annotated[str, Cookie()]) -> str:
    return unquote(name).strip()


Name = Annotated[str, Depends(signed_in)]


class Intake(BaseModel):
    patient: str
    message: str


@router.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    name = unquote(websocket.cookies.get("name", "")).strip()
    await websocket.accept()
    if not name or websocket.headers.get("origin") not in ORIGINS:
        await websocket.close(code=1008)
        return
    await server.serve(websocket, name)


@router.post("/threads")
async def open_thread(body: Intake, name: Name) -> dict:
    return await server.open_thread(name, body.patient, body.message)


@router.get("/threads")
async def threads() -> list[dict]:
    return await server.threads()


@router.post("/threads/{thread_id}/join")
async def join(thread_id: str, name: Name) -> dict:
    return await server.join(name, thread_id)
