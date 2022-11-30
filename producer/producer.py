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

        self.metadata = {}
    
    def get_partition_id(self):
        if not self.key:
            return next(self.cycle)
        self.partition = (len(self.key)%3)
        return self.partition
      
    async def run_client(self)-> None:
        reader, writer = await asyncio.open_connection(self.hostname, self.port)

        metadata_msg = {
            "protocol": "metadata"
        }
        writer.write(json.dumps(metadata_msg).encode())
        await writer.drain()
        
        while True:
            data = await reader.read(1024)

            if not data:
                raise Exception("Socket Closed")


            msg = json.loads(data.decode())

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

                        writer.write(json.dumps(new_topic_msg).encode())
                        await writer.drain()
                        continue

            msg = input("Enter a message: ")
            message = {
                "protocol": "produce",
                'topic': self.topic,
                'partition' : self.partition or self.get_partition_id(),
                'value': msg
            }
            writer.write(json.dumps(message).encode())
            await writer.drain()
        