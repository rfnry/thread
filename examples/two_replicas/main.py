import asyncio

import asyncpg
from rfnry_thread import Message, Working

from client import connect
from server import DSN, replica


async def main() -> None:
    one, two = await asyncpg.create_pool(DSN), await asyncpg.create_pool(DSN)
    (first, first_socket), (second, second_socket) = (
        await replica(one, 8771),
        await replica(two, 8772),
    )

    ana = await connect(8771, "ana")
    bruno = await connect(8772, "bruno")
    thread = await ana.create_thread(thread_id="across")
    await ana.add_member(thread.id, bruno.identity)
    await ana.join(thread.id)
    await bruno.join(thread.id)
    await ana.working(thread.id, label="typing")
    await ana.send(thread.id, "Hello from replica one.")

    async for item in bruno.events():
        if isinstance(item, Working):
            print(f"bruno sees ana {item.label} (a signal mirrored through NOTIFY)")
        if isinstance(item, Message):
            print(f"bruno, on replica two, reads seq {item.seq}: {item.content[0].text}")
            break

    await ana.close()
    await bruno.close()
    for server, socket in ((first, first_socket), (second, second_socket)):
        await server.stop()
        socket.close()
        await socket.wait_closed()
    await server.delete_scope("demo")
    await one.close()
    await two.close()


asyncio.run(main())
