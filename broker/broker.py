import asyncio
import json



class Broker:
    """A class to implement functionalities of a broker."""

    def __init__(self, *, hostname: str ="localhost", port: int, zk_hostname: str ="localhost", zk_port: int) -> None:
        self.hostname = hostname
        self.port = port

        self.zk_hostname = zk_hostname
        self.zk_port = zk_port

        self.metadata = {}
        self.consumers = {}

        self.server = None
        self.client = None

    async def setup(self) -> None: 
        """Start listening for clients and connect to zookeeper."""
        async with asyncio.TaskGroup() as tg:
            task1 = tg.create_task(self.run_server())
            task2 = tg.create_task(self.run_client())

    async def client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Callback function to handle client connections from producer and consumers."""
        data = None
        running = True
        print("connection is here")
        while running:
            data = await reader.read(1024)
            msg_d = data.decode()
            addr, port = writer.get_extra_info("peername")
            # message = json.loads(msg_d)
            writer.write(b"Got message")
            await writer.drain()
            print(f"Message from {addr}:{port}: {msg_d!r}")

            # running = await self.handle_message(message, reader, writer)    


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
        ...
