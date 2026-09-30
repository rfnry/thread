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
from rfnry_engine import InputError, NotFoundError, Review

import harness

CLIENT = Path(__file__).parents[2] / "packages/typescript/dist"
ORIGINS = {"http://localhost:8000", "http://127.0.0.1:8000"}


def known(x_tenant: Annotated[str, Header()]) -> str:
    if x_tenant not in harness.FIRMS:
        raise HTTPException(404, f"there is no firm {x_tenant}")
    return x_tenant


Firm = Annotated[str, Depends(known)]
Name = Annotated[str, Cookie()]


class Matter(BaseModel):
    contract: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    listening = asyncio.create_task(harness.listen())
    yield
    listening.cancel()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(NotFoundError)
async def not_found(request: Request, error: NotFoundError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status_code=404)


@app.exception_handler(InputError)
async def refused(request: Request, error: InputError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status_code=422)


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    name = unquote(websocket.cookies.get("name", "")).strip()
    firm = websocket.cookies.get("firm")
    if not name or firm not in harness.FIRMS or websocket.headers.get("origin") not in ORIGINS:
        raise WebSocketException(code=1008)
    await websocket.accept()
    await harness.serve(websocket, name, firm)


@app.post("/threads")
async def open_matter(body: Matter, firm: Firm, name: Name) -> dict:
    if body.contract not in harness.CONTRACTS:
        raise HTTPException(404, f"there is no contract {body.contract}")
    return await harness.open_matter(firm, unquote(name).strip(), body.contract)


@app.post("/runs/{run_id}/review")
async def review_run(run_id: str, body: Review, firm: Firm) -> dict:
    return await harness.review_run(firm, run_id, body)


@app.post("/export")
async def export(firm: Firm) -> dict:
    return await harness.export(firm)


app.mount("/client", StaticFiles(directory=CLIENT), name="client")
app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
