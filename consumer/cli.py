import argparse

# Create the parser
consumer_parse = argparse.ArgumentParser(prog='cli',description='Choose the consumer topic')

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
print(args.topic)
print(args.from_beginning)

