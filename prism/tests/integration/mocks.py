class MockConsole:
    def __init__(self):
        self.messages: list[str] = []

    def print(self, msg, **kwargs):
        self.messages.append(msg)
