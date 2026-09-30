import json
from pathlib import Path

from rfnry_engine import Engine

from providers import AnthropicProvider

ROOT = Path(__file__).resolve().parent.parent
PLANT = json.loads((ROOT / "data" / "plant.json").read_text())["plant"]
FACTS = json.dumps(PLANT, indent=2, ensure_ascii=False)

engine = Engine(ROOT / "agents", generation=AnthropicProvider())


async def run(question: str, line: str, tools):
    engine_run = await engine.run(
        "maintenance",
        scope="plant-reno",
        input=question,
        context=f"{FACTS}\n\nThis channel is {line}.",
        tools=tools,
    )
    return engine_run
