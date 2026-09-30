import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated
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
from rfnry_thread import NotFoundError

import harness

CLIENT = Path(__file__).parents[2] / "packages/typescript/dist"
ORIGINS = {"http://localhost:8000", "http://127.0.0.1:8000"}


def known(x_tenant: Annotated[str, Header()]) -> str:
    if x_tenant not in harness.CLINICS:
        raise HTTPException(404, f"there is no clinic {x_tenant}")
    return x_tenant


Clinic = Annotated[str, Depends(known)]
Name = Annotated[str, Cookie()]


class Patient(BaseModel):
    patient: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    listening = asyncio.create_task(harness.listen())
    yield
    listening.cancel()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(NotFoundError)
async def not_found(request: Request, error: NotFoundError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status_code=404)


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    name = unquote(websocket.cookies.get("name", "")).strip()
    clinic = websocket.cookies.get("clinic")
    if not name or clinic not in harness.CLINICS or websocket.headers.get("origin") not in ORIGINS:
        raise WebSocketException(code=1008)
    await websocket.accept()
    await harness.serve(websocket, name, clinic)


@app.post("/threads")
async def open_thread(body: Patient, clinic: Clinic, name: Name) -> dict:
    return await harness.open_thread(clinic, unquote(name).strip(), body.patient)


@app.post("/threads/{thread_id}/handoff")
async def handoff(thread_id: str, clinic: Clinic) -> dict:
    return await harness.handoff(clinic, thread_id)


app.mount("/client", StaticFiles(directory=CLIENT), name="client")
app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
