import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import unquote

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketException
from fastapi.staticfiles import StaticFiles

import harness

CLIENT = Path(__file__).parents[2] / "packages/typescript/dist"
ORIGINS = {"http://localhost:8000", "http://127.0.0.1:8000"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    listening = asyncio.create_task(harness.listen())
    yield
    listening.cancel()


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    name = unquote(websocket.cookies.get("name", "")).strip()
    if not name or websocket.headers.get("origin") not in ORIGINS:
        raise WebSocketException(code=1008)
    await websocket.accept()
    await harness.serve(websocket, name)


app.mount("/client", StaticFiles(directory=CLIENT), name="client")
app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
