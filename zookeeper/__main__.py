# maintain metadata
"""
- number of brokers (heartbeat)
- broker information
    - all partitions stored
    - partition leaders
    - topics

{
    brokers: {
        broker_id: {
            host:
            port:
        }
    }
    topics: {
        partitions: {
            partition_1: leader broker id
        }
    }
}
"""

import asyncio

from zookeeper import ZooKeeper


async def main():
    z = ZooKeeper()
    await z.run_server()

asyncio.run(main())