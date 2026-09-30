from datetime import date
from pathlib import Path

from rfnry_engine import Engine, State, Stop, Tool

from providers import AnthropicProvider
from tools import lookup_order

ROOT = Path(__file__).resolve().parent.parent

engine = Engine(
    ROOT / "agents", generation=AnthropicProvider(), tools=[Tool("lookup_order", body=lookup_order)]
)


async def run(question: State, history: list, on_event=None, stop: Stop | None = None):
    engine_run = await engine.run(
        "support",
        scope="acme-home",
        input=question,
        history=history,
        context=f"Today is {date.today().isoformat()}.",
        on_event=on_event,
        stop=stop,
    )
    return engine_run
