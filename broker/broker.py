import asyncio
import json
from pathlib import Path

import aio_pika    
            
                
class Broker:
    """A class to implement functionalities of a broker."""

    def __init__(self, *, broker_id, hostname: str ="localhost", port: int = 9001, rmq_hostname: str ="localhost", rmq_port: int = 5672) -> None:
        self.broker_id = broker_id
        self.hostname = hostname
        self.port = port

        self.zk_hostname = "localhost"
        self.zk_port = 9000

        self.zk_reader = None
        self.zk_writer = None

        self.redis_hostname = rmq_hostname
        self.redis_hostname = rmq_port

        self.brokers = {}
        self.topics = {}

        self.metadata = {}
        self.new_topics = set()


    async def setup(self) -> None: 
        """Start listening for clients and connect to zookeeper."""
        await asyncio.gather(self.run_client(), self.run_server())

    async def client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Callback function to handle client connections from producer and consumers."""
        data = None
        running = True
        print("got client")

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

    async def run_client(self)-> None:
        """Connect socket client to zookeeper"""
        try:
            self.zk_reader, self.zk_writer = await asyncio.open_connection(self.zk_hostname, self.zk_port)
        except Exception as e:
            print("Connnection to zookeeper failed.")
            return

        # Send broker server details to zookeeper.
        register_message = {
                "broker_id": self.broker_id,
                "hostname": self.hostname,
                "port": self.port,
                "new_topics": []
        }

        self.new_topics.clear()
        register_message_json = json.dumps(register_message).encode()
        
        self.zk_writer.write(register_message_json)
        await self.zk_writer.drain()

        while True:
            data = await self.zk_reader.read(1024)
            if not data:
                raise Exception("Socket Closed")

            self.metadata = json.loads(data.decode())
            
            print("sending heartbeat")
            print(self.metadata)

            register_message["new_topics"] = list(self.new_topics)
            register_message_json = json.dumps(register_message).encode()

            self.zk_writer.write(register_message_json)
            await self.zk_writer.drain()

            self.new_topics.clear()

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
        if message["type"] == "metadata":
            msg = {
                "type": "metadata",
                "metadata": self.metadata
            }
            writer.write(json.dumps(msg).encode())
            await writer.drain()

        elif message["type"] == "new_topic":

            p = Path(message['topic'])
            if not p.exists():
                self.new_topics.add(message["topic"])

            p.mkdir(exist_ok=True, parents=True)

        
        elif message['type'] == "producer":
            p = Path(message['topic'])
            if not p.exists():
                self.new_topics.add(message["topic"])

            p.mkdir(exist_ok=True, parents=True)

            file = Path(message['topic'], str(message['partition'])+'.txt')
            if not file.exists():
                file.touch()

            with file.open('a') as f:
                f.write(message['value'] + '\n')
            
            ack_msg = {
                "type": "ack",
                "ack": 1
            }
            writer.write(json.dumps(ack_msg).encode())
            await writer.drain()

        elif message['type'] == "consumer":
            if message['full']:
                p = Path(message['topic'])

                for child in p.iterdir():
                    writer.write(child.read_text())
                    await writer.drain()