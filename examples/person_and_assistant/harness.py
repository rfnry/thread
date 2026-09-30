from rfnry_engine import Engine, TextDelta
from rfnry_thread import Identity, Invited, Message, resolve

from providers import AnthropicProvider


def build_engine():
    return Engine("agents", generation=AnthropicProvider())


async def assistant(server, engine):
    me = await server.connect(
        identity=Identity(role="assistant", id="assistant", name="Assistant"), scope="demo"
    )
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        if not (isinstance(item, Message) and item.author.role == "user"):
            continue
        earlier = [
            e
            for e in resolve(await me.history(item.thread_id, before=item.seq))
            if isinstance(e, Message)
        ]
        history = [
            {"role": "user" if e.author.role == "user" else "assistant", "content": e.content}
            for e in earlier
        ]
        async with me.stream(item.thread_id, in_reply_to=item.id) as reply:

            async def show(event):
                if isinstance(event, TextDelta):
                    await reply.write(event.text)

            run = await engine.run(
                "assistant", scope="demo", input=item.content, history=history, on_event=show
            )
            await reply.finish(run.reply)
