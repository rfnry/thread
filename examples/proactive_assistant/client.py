from rfnry_thread import Client


async def connect_ana():
    return await Client.connect(
        "ws://127.0.0.1:8765", headers={"Authorization": "Bearer ana-token"}
    )
