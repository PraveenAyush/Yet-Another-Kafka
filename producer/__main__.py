import asyncio

from producer import Producer


async def main() -> None:
    """Run producer server."""
    topic = input("Enter a Topic: ")
    key = input("Enter a key: ")
   
    publisher = Producer('localhost',9999,topic,key)
    await publisher.setup()

if __name__ == "__main__":
    asyncio.run(main())