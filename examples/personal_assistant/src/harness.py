from datetime import date
from pathlib import Path

from rfnry_engine import Engine, State

from providers import AnthropicProvider

ROOT = Path(__file__).resolve().parent.parent

engine = Engine(ROOT / "agents", generation=AnthropicProvider())


async def run(question: State, history: list):
    engine_run = await engine.run(
        "assistant",
        scope="jordan",
        input=question,
        history=history,
        context=f"Today is {date.today().isoformat()}.",
    )
    return engine_run
