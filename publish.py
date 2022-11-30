import asyncio

import async_timeout

import aioredis

from reader import reader 

STOPWORD = "STOP"
async def main():
    redis = aioredis.from_url("redis://localhost")

    pubsub = redis.pubsub()
    await pubsub.subscribe("channel:1", "channel:2")

    future = asyncio.create_task(reader(pubsub))

    await redis.publish("channel:1", "Hello")
    await redis.publish("channel:2", "World")
    await redis.publish("channel:1", STOPWORD)
    await future

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())