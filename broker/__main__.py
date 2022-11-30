import asyncio
import sys

from broker import Broker


async def main() -> None:
    """Run broker server."""
    broker = Broker(broker_id=int(sys.argv[1]), port=int(sys.argv[2]))
    await broker.setup()

if __name__ == "__main__":
    asyncio.run(main())