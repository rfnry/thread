from rfnry_thread import Client


async def connect(port, token):
    return await Client.connect(
        f"ws://127.0.0.1:{port}", headers={"Authorization": f"Bearer {token}"}
    )
