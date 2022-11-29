import asyncio
import json
from itertools import cycle


class Producer:
    

    def __init__(self,hostname,port,topic,key=None) -> None:
        self.hostname = hostname
        self.port = port
        self.topic = topic
        self.key = key
        self.partition = None
        self.cycle = cycle([0, 1, 2])
        

    
    def get_partition_id(self):
        if not self.key:
            return next(self.cycle)
        self.partition = (len(self.key)%3)
        return self.partition
      
    async def run_client(self)-> None:
        reader, writer = await asyncio.open_connection(self.hostname, self.port)
        

        writer.write(b"Metadata")
        await writer.drain()
        
        while True:
            data = await reader.read(1024)



            if not data:
                raise Exception("Socket Closed")

            print(f"Received: {data.decode()!r}")

            x = input("Enter a message: ")
            message = {
                'topic' : self.topic,
                'partition' : self.partition or self.get_partition_id(),
                'value' : x 
            }
            writer.write(json.dumps(message).encode())
            await writer.drain()

    async def setup(self) -> None: 
        """Start sending for the server"""
        async with asyncio.TaskGroup() as tg:
            task1 = tg.create_task(self.run_client())


        