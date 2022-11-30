import asyncio

from producer import Producer


async def main() -> None:
    """Run producer server."""
    topic = input("Enter a Topic: ")
    key = input("Enter a key: ")
   
    publisher = Producer('localhost',9001,topic,key)
    await publisher.run_client()

if __name__ == "__main__":
    asyncio.run(main())