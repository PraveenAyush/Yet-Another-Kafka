import asyncio
import json
from collections import defaultdict
from copy import deepcopy

import aio_pika
from async_timeout import timeout

from itertools import cycle


class ZooKeeper:

    def __init__(self, *, hostname="localhost", port=9000, rmq_hostname="localhost", rmq_port=5672):
        
        self.hostname = hostname
        self.port = port

        self.rmq_hostname = rmq_hostname
        self.rmq_port = rmq_port

        self.broker_metadata = {}
        self.topic_metadata = {}

        self.broker_timeout = 10
        self.broker_availability = {}


    async def handle_heartbeat(self):
        pass

    async def client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """
        Callback function to handle client connections from producer and consumers.
        
        Protocols:
        - New connection
        - New topic
        - metadata
        """
        data = None
        broker_id = None

        try:

            while True:
                data = await reader.read(1024)
                msg_d = data.decode()

                if msg_d == "":
                    raise asyncio.TimeoutError()

                msg = json.loads(msg_d)



                broker_id = msg["broker_id"]

                if msg["broker_id"] not in self.broker_metadata:
                    self.broker_metadata[msg["broker_id"]] = {
                        "hostname": msg["hostname"],
                        "port": msg["port"],
                    }

                print(msg["new_topics"])
                if msg["new_topics"]:
                    for topic in msg["new_topics"]:
                        self.topic_metadata[topic] = self.generate_partition_metadata(topic)
                        

                await asyncio.sleep(2)
                total_meta = {"brokers": self.broker_metadata} | {"topics": self.topic_metadata}
                writer.write(json.dumps(total_meta).encode())
                t2 = asyncio.create_task(writer.drain())
                await asyncio.wait_for(t2, timeout=5)

        except Exception as e:
            print(type(e))
            print(f"Broker {broker_id} died.")

            if len(self.broker_metadata) > 1:
                self.topic_metadata = self.elect_new_leader(broker_id)

    def elect_new_leader(self, dead_broker_id):
        print("electing")
        new = deepcopy(self.topic_metadata)
        remaining_brokers = cycle([broker for broker in self.broker_metadata if broker != dead_broker_id])
        for topic, partition in self.topic_metadata.items():
            for partition_id, value in partition.items():
                if value == dead_broker_id:
                    new[topic][partition_id] = next(remaining_brokers)

        return new
        

    def generate_partition_metadata(self, topic):
        metadata = {}

        for i, broker in enumerate(self.broker_metadata):
            metadata[i] = broker

            self.broker_metadata

        return metadata
        


    async def run_server(self) -> None:
        """Start socket server to accept producer and consumer connections."""
        server = await asyncio.start_server(self.client_connection, self.hostname, self.port)
        async with server:
            print(f"Started listening on port {self.port}")
            await server.serve_forever()

    # async def broker_data(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
    #     """
    #     {
    #         broker_id: int,
    #         hostname: str,
    #         port: int
    #     }
    #     """
    #     async with message.process():
    #         print(message)
    #         msg = message.body.decode()
    #         msg = json.loads(msg)

    #         if msg["broker_id"] not in self.broker_metadata:
    #             self.broker_metadata[msg["broker_id"]] = {
    #                 "hostname": msg["hostname"],
    #                 "port": msg["port"]
    #             }

    #             print(self.broker_metadata)

    # async def start(self) -> None:
    #     # Perform connection
    #     connection = await aio_pika.connect(host=self.rmq_hostname, port=self.rmq_port)

    #     async with connection:
    #         # Creating a channel
    #         channel = await connection.channel()
    #         await channel.set_qos(prefetch_count=1)

    #         broker_exchange = await channel.declare_exchange(
    #             "broker_data", aio_pika.ExchangeType.FANOUT,
    #         )

    #         # Declaring queue
    #         queue = await channel.declare_queue(exclusive=True)

    #         # Binding the queue to the exchange
    #         await queue.bind(broker_exchange)

    #         # Start listening the queue
    #         await queue.consume(self.broker_data)

    #         print("[*] Waiting for logs. To exit press CTRL+C")
    #         await asyncio.Future()

