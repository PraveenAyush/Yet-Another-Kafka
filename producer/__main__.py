import argparse
import asyncio

from producer import Producer

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

producer_parse.add_argument('--input',
                            action='store_true',
                            help='gets all messages from topic creation')

args = producer_parse.parse_args()

async def main() -> None:
    """Run producer server."""

    topic = args.topic
    key = args.key
    has_input = args.input
    
    publisher = Producer('localhost',9001,topic,key, has_input=has_input)
    await publisher.run_client()
    await publisher.input_messages()

if __name__ == "__main__":
    asyncio.run(main())