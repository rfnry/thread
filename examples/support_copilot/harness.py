import json

import rfnry_engine
from rfnry_engine import Engine, Refinery, ToolCallStarted
from rfnry_thread import Text

from providers import AnthropicProvider, TypeSafeProvider

TODAY = "2026-09-25"


def output(result):
    if isinstance(result, str):
        return [Text(text=result)]
    return result if isinstance(result, list) else [Text(text=json.dumps(result))]


async def build_refinery():
    storage = rfnry_engine.InMemoryStorage()
    refinery = Refinery(storage)
    await refinery.initialize()
    return refinery


def build_engine(refinery):
    return Engine(
        "agents", generation=AnthropicProvider(), decision=TypeSafeProvider(), refinery=refinery
    )


async def advise(engine, copilot, case):
    async def mirror(event):
        if isinstance(event, ToolCallStarted):
            await copilot.working(case.thread_id, label=event.tool)

    run = await engine.run(
        "copilot",
        scope=copilot.scope,
        input=case.content,
        context=f"Today is {TODAY}. The support agent asking is {case.author.name}.",
        on_event=mirror,
    )
    for step in run.steps:
        await copilot.step(
            case.thread_id,
            name=step.tool,
            input=step.args,
            output=output(step.result),
            is_error=step.is_error,
            duration_ms=step.duration_ms,
            in_reply_to=case.id,
        )
    reply = await copilot.send(case.thread_id, run.reply, in_reply_to=case.id)
    await copilot.idle(case.thread_id)
    return reply, run
