import asyncio
import json
from copy import deepcopy
from itertools import cycle

from async_timeout import timeout


class ZooKeeper:

    def __init__(self, *, hostname="localhost", port=9000):
        
        self.hostname = hostname
        self.port = port

        self.broker_metadata = {}
        self.topic_metadata = {}

        self.broker_availability = {}


    def get_metadata(self) -> bytes:
        combined_metadata = {"brokers": self.broker_metadata} | {"topics": self.topic_metadata}
        return json.dumps(combined_metadata).encode()

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

                print(f"Received protocol `{msg['protocol']}` from broker {msg['broker_id']}")

                match msg["protocol"]:

                    case "register":
                        broker_id = msg["broker_id"]

                        if msg["broker_id"] not in self.broker_metadata:
                            self.broker_metadata[msg["broker_id"]] = {
                                "hostname": msg["hostname"],
                                "port": msg["port"],
                            }

                        writer.write(self.get_metadata())
                        await writer.drain()

                    case "new_topic":
                        if topic := msg["new_topic"]:
                            self.topic_metadata[topic] = self.generate_partition_metadata(topic)
                        
                        writer.write(self.get_metadata())
                        await writer.drain()

                    case "metadata":
                        writer.write(self.get_metadata())
                        await writer.drain()

                    case "heartbeat":
                        print(f"{msg['broker_id']} is alive.")

                        heartbeat_ack = {
                            "protocol": "heartbeat",
                            "status": 1
                        }
                        writer.write(json.dumps(heartbeat_ack).encode())
                        await writer.drain()

                    case _:
                        print("no message")

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

