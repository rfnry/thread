import json

from rfnry_engine import Engine, ToolCallStarted
from rfnry_thread import Text, ThreadTools

from providers import AnthropicProvider


def output(result):
    if isinstance(result, str):
        return [Text(text=result)]
    if isinstance(result, dict):
        return [Text(text=json.dumps(result))]
    return result


def build_engine():
    return Engine("agents", generation=AnthropicProvider())


async def answer(engine, helper, thread_id, question):
    tools = ThreadTools(
        helper, thread_id=thread_id, triggered_by=question.author, in_reply_to=question.id
    )

    async def mirror(event):
        if isinstance(event, ToolCallStarted) and event.tool not in tools:
            await helper.working(thread_id, label=f"{event.tool}")

    run = await engine.run(
        "support",
        scope="demo",
        input="A new message is waiting in your thread.",
        tools=tools,
        on_event=mirror,
    )
    for step in run.steps:
        if step.tool not in tools:
            await helper.step(
                thread_id,
                name=step.tool,
                input=step.args,
                output=output(step.result),
                is_error=step.is_error,
                duration_ms=step.duration_ms,
                in_reply_to=question.id,
            )
    await helper.idle(thread_id)
