import asyncio
from broker import Broker


async def main() -> None:
    """Run broker server."""
    broker = Broker(broker_id=0, port=9001)
    await broker.setup()

if __name__ == "__main__":
    asyncio.run(main())