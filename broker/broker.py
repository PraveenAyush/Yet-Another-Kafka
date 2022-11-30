import asyncio
import json
from pathlib import Path

import aio_pika    
            
                
class Broker:
    """A class to implement functionalities of a broker."""

    def __init__(self, *, hostname: str ="localhost", port: int, rmq_hostname: str ="localhost", rmq_port: int = 5672) -> None:
        self.hostname = hostname
        self.port = port

        self.rmq_hostname = rmq_hostname
        self.rmq_port = rmq_port

        self.brokers = {}
        self.topics = {}

        


    async def setup(self) -> None: 
        """Start listening for clients and connect to zookeeper."""
        await asyncio.gather(self.connect_rmq(), self.run_server())

    async def client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Callback function to handle client connections from producer and consumers."""
        data = None
        running = True
        while running:
            data = await reader.read(1024)
            msg_d = data.decode()
            addr, port = writer.get_extra_info("peername")
            # message = json.loads(msg_d)
            writer.write(b"Got message")
            await writer.drain()
            print(f"Message from {addr}:{port}: {msg_d!r}")

    async def connect_rmq(self):
        connection = await aio_pika.connect(hostname=self.rmq_hostname, port=self.rmq_port)
        print("In rmq")
        async with connection:
            # Creating a channel
            channel = await connection.channel()

            broker_exchange = await channel.declare_exchange(
                "broker_data", aio_pika.ExchangeType.FANOUT,
            )
            register_message = {
                "broker_id": 0,
                "hostname": self.hostname,
                "port": self.port,
            }
            
            register_message = json.dumps(register_message).encode()

            message = aio_pika.Message(
                register_message,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )

            # Sending the message
            await broker_exchange.publish(message, routing_key="info")

            print(f"[x] Sent {register_message}")


    async def run_server(self) -> None:
        """Start socket server to accept producer and consumer connections."""
        server = await asyncio.start_server(self.client_connection, self.hostname, self.port)
        async with server:
            print(f"Started listening on port {self.port}")
            await server.serve_forever()

    async def run_client(self)-> None:
        """Connect socket client to zookeeper"""
        try:
            reader, writer = await asyncio.open_connection(self.zk_hostname, self.zk_port)
        except ConnectionRefusedError:
            print("Connnection to zookeeper failed.")
            return

        # Send broker server details to zookeeper.
        message = {
            "broker_id": 1
        }
        
        writer.write(json.dumps(message,ensure_ascii=False).encode("gbk"))
        await writer.drain()

        while True:
            data = await reader.read(1024)
            if not data:
                raise Exception("Socket Closed")
            print(f"Received: {data.decode()!r}")

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
        if message['type'] == "producer":
            p = Path(message['topic'])
            p.mkdir(exist_ok=True, parents=True)

            file = Path(message['topic'], message['partition']+'.txt')

            if not file.exists():
                file.touch()
            file.open('a')
            with file.open('a') as f:
                f.write(message['value'] + '\n')
        
        elif message['type'] == "consumer":
            if message['full']:
                p = Path(message['topic'])

                for child in p.iterdir():
                    writer.write(child.read_text())
                    await writer.drain()