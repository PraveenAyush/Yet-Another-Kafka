import asyncio
import json

import aioredis
import async_timeout

class Consumer:
    def __init__(self, *, topic: str, redis_url: str, from_beginning: bool, hostname: str = None, port: int = None):
        self.topic = topic

        self.from_beginning = from_beginning

        self.hostname = hostname
        self.port = port

        self.redis = aioredis.from_url(redis_url)
        self.pubsub = self.redis.pubsub()

    async def get_from_beginning(self):
        reader, writer = await asyncio.open_connection(self.hostname, self.port)

        from_beg_message = {
            "protocol": "from_beginning",
            "topic": self.topic
        }
        writer.write(json.dumps(from_beg_message).encode())
        await writer.drain()
        
        while True:
            data = await reader.read(1024)
            if not data:
                raise Exception("Socket Closed")

            print(data.decode().strip())


    async def reader(self):
        while True:
            try:
                async with async_timeout.timeout(1):
                    message = await self.pubsub.get_message(ignore_subscribe_messages=True)
                    if message is not None:
                        print(message["data"].decode())
                    await asyncio.sleep(0.01)
            except asyncio.TimeoutError:
                pass

    async def consume(self):
        await self.pubsub.subscribe(self.topic)

        future = asyncio.create_task(self.reader())
        await future

