import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import unquote

import uvicorn
from fastapi import (
    Cookie,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Request,
    WebSocket,
    WebSocketException,
)
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from rfnry_thread import ForbiddenError, NotFoundError

import harness

CLIENT = Path(__file__).parents[2] / "packages/typescript/dist"
ORIGINS = {"http://localhost:8000", "http://127.0.0.1:8000"}


def known(x_tenant: Annotated[str, Header()]) -> str:
    if x_tenant not in harness.PLANTS:
        raise HTTPException(404, f"there is no plant {x_tenant}")
    return x_tenant


Plant = Annotated[str, Depends(known)]
Name = Annotated[str, Cookie()]


class Question(BaseModel):
    line: str
    title: str
    task: Literal["troubleshoot", "procedure"] | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    listening = asyncio.create_task(harness.listen())
    yield
    listening.cancel()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(NotFoundError)
async def not_found(request: Request, error: NotFoundError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status_code=404)


@app.exception_handler(ForbiddenError)
async def forbidden(request: Request, error: ForbiddenError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status_code=403)


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    name = unquote(websocket.cookies.get("name", "")).strip()
    plant = websocket.cookies.get("plant")
    if not name or plant not in harness.PLANTS or websocket.headers.get("origin") not in ORIGINS:
        raise WebSocketException(code=1008)
    await websocket.accept()
    await harness.serve(websocket, name, plant)


@app.get("/channels")
async def channels(plant: Plant) -> list[dict]:
    return await harness.channels(plant)


@app.post("/threads")
async def ask(body: Question, plant: Plant, name: Name) -> dict:
    if body.line not in harness.PLANTS[plant]["lines"]:
        raise HTTPException(404, f"there is no {body.line} at {plant}")
    return await harness.ask(plant, unquote(name).strip(), body.line, body.title, body.task)


@app.post("/threads/{thread_id}/elevate")
async def elevate(thread_id: str, plant: Plant, name: Name) -> dict:
    return await harness.elevate(plant, unquote(name).strip(), thread_id)


@app.post("/threads/{thread_id}/join")
async def join(thread_id: str, plant: Plant, name: Name) -> dict:
    return await harness.join(plant, unquote(name).strip(), thread_id)


app.mount("/client", StaticFiles(directory=CLIENT), name="client")
app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
