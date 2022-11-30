import asyncio
import json
from collections import defaultdict

import aio_pika


class ZooKeeper:

    def __init__(self, *, rmq_hostname="localhost", rmq_port=5672):

        self.rmq_hostname = rmq_hostname
        self.rmq_port = rmq_port

        self.broker_metadata = {}
        self.topic_metadata = {}

        self.broker_timeout = 10
        self.broker_availability = {}


    async def setup(self):
        pass

    async def broker_data(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        """
        {
            broker_id: int,
            hostname: str,
            port: int
        }
        """
        async with message.process():
            print(message)
            msg = message.body.decode()
            msg = json.loads(msg)

            if msg["broker_id"] not in self.broker_metadata:
                self.broker_metadata[msg["broker_id"]] = {
                    "hostname": msg["hostname"],
                    "port": msg["port"]
                }

                print(self.broker_metadata)

    async def start(self) -> None:
        # Perform connection
        connection = await aio_pika.connect(host=self.rmq_hostname, port=self.rmq_port)

        async with connection:
            # Creating a channel
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)

            broker_exchange = await channel.declare_exchange(
                "broker_data", aio_pika.ExchangeType.FANOUT,
            )

            # Declaring queue
            queue = await channel.declare_queue(exclusive=True)

            # Binding the queue to the exchange
            await queue.bind(broker_exchange)

            # Start listening the queue
            await queue.consume(self.broker_data)

            print("[*] Waiting for logs. To exit press CTRL+C")
            await asyncio.Future()

