from pathlib import Path

from rfnry_engine import Engine

from providers import AnthropicProvider

ROOT = Path(__file__).resolve().parent.parent

engine = Engine(ROOT / "agents", generation=AnthropicProvider())


async def run(question: str, history: list):
    engine_run = await engine.run(
        "nurse_line", scope="clinic-northside", input=question, history=history
    )
    return engine_run
