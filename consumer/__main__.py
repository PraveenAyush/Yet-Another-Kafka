import argparse
import asyncio

from consumer import Consumer

# Create the parser
consumer_parse = argparse.ArgumentParser(
    prog='consumer', description='Choose the consumer topic'
)

# Add the arguments
consumer_parse.add_argument('--topic', '-tp',
                            metavar="topic",
                            action='store',
                            type=str,
                            help='topic to subscribe',
                            required=True)

consumer_parse.add_argument('--from-beginning',
                            action='store_true',
                            help='gets all messages from topic creation')

# Execute the parse_args() method
args = consumer_parse.parse_args()


async def main() -> None:
    topic: str = args.topic
    from_beginning: bool = args.from_beginning

    consumer = Consumer(
        topic=topic,
        redis_url="redis://localhost",
        from_beginning=from_beginning,
        hostname="localhost",
        port=9001
    )
    if from_beginning:
        await consumer.get_from_beginning()
    await consumer.consume()


if __name__ == "__main__":
    asyncio.run(main())
