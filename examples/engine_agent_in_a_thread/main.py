import asyncio
import json

from rfnry_engine import Engine, ToolCallStarted
from rfnry_thread import Identity, InMemoryStorage, Message, Server, Text, ThreadTools, resolve

from providers import AnthropicProvider


def output(result):
    if isinstance(result, str):
        return [Text(text=result)]
    if isinstance(result, dict):
        return [Text(text=json.dumps(result))]
    return result


async def main() -> None:
    server = Server(InMemoryStorage())
    engine = Engine("agents", generation=AnthropicProvider())
    ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="demo")
    helper = await server.connect(
        identity=Identity(role="assistant", id="support", name="Support"), scope="demo"
    )
    thread = await ana.create_thread(thread_id="invoice")
    await ana.add_member(thread.id, helper.identity)
    await helper.join(thread.id)
    question = await ana.send(thread.id, "My invoice was charged twice. What happens now?")

    tools = ThreadTools(
        helper, thread_id=thread.id, triggered_by=question.author, in_reply_to=question.id
    )

    async def mirror(event):
        if isinstance(event, ToolCallStarted) and event.tool not in tools:
            await helper.working(thread.id, label=f"{event.tool}")

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
                thread.id,
                name=step.tool,
                input=step.args,
                output=output(step.result),
                is_error=step.is_error,
                duration_ms=step.duration_ms,
                in_reply_to=question.id,
            )
    await helper.idle(thread.id)

    for event in resolve(await ana.history(thread.id)):
        if isinstance(event, Message):
            print(f"{event.author.name}: {event.content[0].text}")
        else:
            print(f"  [{event.kind} {getattr(event, 'name', '')}]")
    await ana.close()
    await helper.close()


asyncio.run(main())
