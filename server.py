import asyncio
import json

HOST = "127.0.0.1"
PORT = 9999

async def handle_echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    data = None
    passed = False


    while data != b"quit":
        data = await reader.read(1024)
        msg = data.decode()
        addr, port = writer.get_extra_info("peername")
        print(json.loads(msg))
        #passed, error = func()
        if passed == True:
            writer.write(b"Acknowledgement from the server!")
            await writer.drain()
        else:
            writer.write(b"THERE IS A PROBLEM")
            await writer.drain()



        #Run manage topics
        

    writer.close()
    await writer.wait_closed()

async def run_server()-> None:
    server = await asyncio.start_server(handle_echo, HOST, PORT)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop .run_until_complete(run_server())

