import asyncio
import json



class Broker():

    def __init__(self, *, hostname="localhost", port, zk_hostname, zk_port):
        self.hostname = hostname
        self.port = port

        self.zk_hostname = zk_hostname
        self.zk_port = zk_port

        self.metadata = {}
        self.consumers = {}

        self.server = None
        self.client = None

    async def listen(self): 
        async with asyncio.TaskGroup() as tg:
            task1 = tg.create_task(self.setup_server())
            task2 = tg.create_task(self.run_client())

    async def handle_echo(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:

        print("Data received!")
        data = None
        running = True

        while running:
            data = await reader.read(1024)
            msg_d = data.decode()
            addr, port = writer.get_extra_info("peername")
            message = json.loads(msg_d)

            running = await self.handle_message(message, reader, writer)    


    async def setup_server(self):
        server = await asyncio.start_server(self.handle_echo, self.hostname, self.port)
        async with server:
            print(f"Started listening on port {self.port}")
            await server.serve_forever()

    async def run_client(self)-> None:
        reader, writer = await asyncio.open_connection(self.zk_hostname, self.zk_port)
        
        print("CONNECTED TO ZOOKEEPER!")

        message = {
            'topic':'BD',
            'key':'69',
            'value':'Working!'

        }
        
        writer.write(json.dumps(message,ensure_ascii=False).encode("gbk"))
        #writer.write(b"Hello World")
        await writer.drain()

        while True:
            data = await reader.read(1024)
            if not data:
                raise Exception("Socket Closed")
            print(f"Received: {data.decode()!r}")

    async def handle_message(self, message: dict, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
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
