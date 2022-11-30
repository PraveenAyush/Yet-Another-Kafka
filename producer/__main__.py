import asyncio

from producer import Producer
import argparse

# Create the parser
producer_parse = argparse.ArgumentParser(
    prog='producer', description='Choose the producer topic')

# Add the arguments
producer_parse.add_argument('--topic', '-tp',
                            metavar="<topic>",
                            action='store',
                            type=str,
                            help='topic to publish',
                            required=True)

producer_parse.add_argument('--key', '-k',
                            metavar="<key>",
                            action='store',
                            type=str,
                            help='decides which partition to write to')

args = producer_parse.parse_args()

async def main() -> None:
    """Run producer server."""
    topic = args.topic
    key = args.key

    publisher = Producer('localhost',9999,topic,key)
    await publisher.setup()

if __name__ == "__main__":
    asyncio.run(main())