import asyncio
import json
from itertools import cycle
from typing import Callable


class Producer:
    

    def __init__(self,hostname,port,topic,key=None) -> None:
        self.hostname = hostname
        self.port = port
        self.topic = topic
        self.key = key
        self.partition = None
        self.cycle = cycle([0, 1, 2])

        self.broker_connections = {}

        self.metadata = {}

    def get_broker_url(self, broker_id: int | None = None):
        if not broker_id:
            return f"{self.hostname}:{self.port}"

        broker = self.metadata["brokers"][str(broker_id)]
        return f"{broker['hostname']}:{broker['port']}"

    def get_partition_id(self):
        if not self.key:
            return next(self.cycle)
        self.partition = (len(self.key)%3)
        return self.partition

    async def send_message_broker(self, broker_id: int, message: bytes, callback: Callable | None = None):
        broker_url = self.get_broker_url(broker_id)
        print(self.broker_connections)
        
        try:
            writer = self.broker_connections[broker_url]["writer"]
            reader = self.broker_connections[broker_url]["reader"]
        except KeyError:
            print("got key error, establishing connection.")
            reader, writer = await asyncio.open_connection(
                self.metadata["brokers"][str(broker_id)]["hostname"],
                self.metadata["brokers"][str(broker_id)]["port"]
            )

            self.broker_connections[broker_url] = {
                "reader": reader,
                "writer": writer
            }
            
        writer.write(message)
        await writer.drain()

        data = await reader.read(1024)
        if not data:
            raise Exception("Socket Closed")
        data_d = json.loads(data.decode())
        await self.handle_message(data_d)

    async def handle_message(self, msg) -> None:
        match msg["protocol"]:
            case 'ack':
                print("Ack receieved: ", msg["ack"])

            case "metadata":
                self.metadata = msg["metadata"]

                print("Metadata: ", self.metadata)

                if self.topic not in self.metadata['topics']:
                    new_topic_msg = {
                        "protocol": "new_topic",
                        "topic": self.topic
                    }
                    await self.send_message_broker(self.get_broker_url(), json.dumps(new_topic_msg).encode())

                    
      
    async def run_client(self)-> None:
        reader, writer = await asyncio.open_connection(self.hostname, self.port)

        self.broker_connections[f"{self.hostname}:{self.port}"] = {
            "reader": reader,
            "writer": writer
        }

        metadata_msg = {
            "protocol": "metadata"
        }
        writer.write(json.dumps(metadata_msg).encode())
        await writer.drain()
        
        data = await reader.read(1024)
        if not data:
            raise Exception("Socket Closed")
        data_d = json.loads(data.decode())
        await self.handle_message(data_d)

    async def input_messages(self):
        print("Enter q! to quit")
        while True:
            msg = input("Enter a message: ")

            if msg == "q!":
                break

            partition = self.partition or self.get_partition_id()
            message = {
                "protocol": "produce",
                'topic': self.topic,
                'partition' : partition,
                'value': msg
            }

            broker_id = self.metadata["topics"][self.topic][str(partition)]

            await self.send_message_broker(broker_id, json.dumps(message).encode())
        