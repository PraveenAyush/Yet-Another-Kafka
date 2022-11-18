import asyncio
import json

HOST = "127.0.0.1"
PORT = 9999

async def run_client()-> None:
    reader, writer = await asyncio.open_connection(HOST, PORT)

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

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop .run_until_complete(run_client())