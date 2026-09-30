import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import harness
from routes import router

ROOT = Path(__file__).resolve().parent.parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    listening = asyncio.create_task(harness.listen())
    yield
    listening.cancel()


app = FastAPI(lifespan=lifespan)
app.include_router(router)
app.mount("/client", StaticFiles(directory=ROOT.parents[1] / "packages/typescript/dist"))
app.mount("/", StaticFiles(directory=ROOT / "static", html=True))

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
