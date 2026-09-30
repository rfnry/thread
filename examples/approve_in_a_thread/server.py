from rfnry_thread import InMemoryStorage, Server


def build_server():
    return Server(InMemoryStorage())
