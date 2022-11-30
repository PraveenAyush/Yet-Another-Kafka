import asyncio
import json
from pathlib import Path
from typing import Callable

import aioredis


class Broker:
    """A class to implement functionalities of a broker."""

    def __init__(self, *, broker_id, hostname: str ="localhost", port: int = 9001, zk_hostname: str = "localhost", zk_port: int = 9000) -> None:
        self.broker_id = broker_id
        self.hostname = hostname
        self.port = port

        self.broker_dict = {
            "broker_id": self.broker_id,
            "hostname": self.hostname,
            "port": self.port
        }

        self.zk_hostname = zk_hostname
        self.zk_port = zk_port
        
        self.redis = aioredis.from_url("redis://localhost")

        self.zk_reader = ...
        self.zk_writer = ...

        self.data_path = Path("data", str(self.broker_id))

        self.broker_connections = {}

        self.metadata = {}
        self.new_topics = set()


    async def setup(self) -> None: 
        """Start listening for clients and connect to zookeeper."""
        await asyncio.gather(self.run_client(), self.run_server())


    async def client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Callback function to handle client connections from producer and consumers."""
        data = None
        running = True

        while running:
            data = await reader.read(1024)
            if data == "":
                print("error")
                break
            msg_d = data.decode()
            message = json.loads(msg_d)
            print(message)
            await self.handle_message(message, reader, writer)

    async def run_server(self) -> None:
        """Start socket server to accept producer and consumer connections."""
        server = await asyncio.start_server(self.client_connection, self.hostname, self.port)
        async with server:
            print(f"Started listening on port {self.port}")
            await server.serve_forever()

    async def send_message_zk(self, message: bytes, callback: Callable | None = None) -> None:
        """Send message to zookeeper and handle immediately returned message."""
        print("Sending message to zookeeper...")

        self.zk_writer.write(message)
        await self.zk_writer.drain()

        data = await self.zk_reader.read(1024)
        if not data:
            raise Exception("Socket Closed")

        data_parsed = json.loads(data.decode())

        print("calling `Callback` function...")

        if not callback:
            return data_parsed

        callback(data_parsed)

    async def run_client(self)-> None:
        """Connect socket client to zookeeper"""
        try:
            self.zk_reader, self.zk_writer = await asyncio.open_connection(self.zk_hostname, self.zk_port)
        except Exception as e:
            print("Connnection to zookeeper failed.")
            return

        # Send broker server details to zookeeper.
        register_broker_data = self.broker_dict | {
            "protocol": "register"
        }

        data = await self.send_message_zk(
            json.dumps(register_broker_data).encode(),
        )

        self.metadata = data

    async def handle_message(self, message: dict, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        """
        Handle messages sent by producer/consumer.

        # Request Metadata
        ## Request
        {
            type: "metadata"
        }

        # Producer sending message
        ## Request
        {
            type: "producer",
            topic: ...,
            partition: ...,
            value: ...
        }

        # Consumer requesting for messages
        ## Request
        {
            type: "consumer",
            topic: ...,
            full: bool,
        }

        # Returns True or False
        - True: continue collecting messages.
        - False: close the socket connection and stop receiving messages.
        """
        match message["protocol"]:

            case "metadata":
                print("Sending metadata")
                print(self.metadata)
                msg = {
                    "protocol": "metadata",
                    "metadata": self.metadata
                }
                writer.write(json.dumps(msg).encode())
                await writer.drain()

            case "new_topic":
                register_new_topic = self.broker_dict | {
                    "protocol": "new_topic",
                    "new_topic": message["topic"]
                }
                data = await self.send_message_zk(
                    json.dumps(register_new_topic).encode()
                )
                self.metadata = data

                msg = {
                    "protocol": "metadata",
                    "metadata": self.metadata
                }
                writer.write(json.dumps(msg).encode())
                await writer.drain()
        
            case "produce":
                print("redis before")
                await self.redis.publish(message['topic'], message['value'])
                print("redis after")

                p = Path(self.data_path, message['topic'])
                if not p.exists():
                    self.new_topics.add(message["topic"])

                p.mkdir(exist_ok=True, parents=True)

                file = Path(p, str(message['partition'])+'.txt')
                if not file.exists():
                    file.touch()

                with file.open('a') as f:
                    f.write(message['value'] + '\n')

                print(file)
                
                ack_msg = {
                    "protocol": "ack",
                    "ack": 1
                }
                writer.write(json.dumps(ack_msg).encode())
                await writer.drain()

            case "from_beginning":
                p = Path(self.data_path, message['topic'])

                for child in p.iterdir():
                    for value in child.read_text().strip().split("\n"):
                        writer.write(f"{value}\n".encode())
                        await writer.drain()
