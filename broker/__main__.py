import asyncio
from broker import Broker


async def main():
    broker = Broker(port=9000, zk_hostname="localhost", zk_port=9999)

    await broker.listen()


if __name__ == "__main__":
    asyncio.run(main())