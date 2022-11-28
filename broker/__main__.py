import asyncio
from broker import Broker


async def main() -> None:
    """Run broker server."""
    broker = Broker(port=9000, zk_hostname="localhost", zk_port=9999)
    await broker.setup()

if __name__ == "__main__":
    asyncio.run(main())