class AsyncDatabendDriver:
    def __init__(self, dsn: str): ...

    async def exec(self, sql: str) -> int: ...