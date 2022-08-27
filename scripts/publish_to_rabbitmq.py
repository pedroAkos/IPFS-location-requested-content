import pika
import argparse
import sys

parser = argparse.ArgumentParser()
#-t topic -h host
parser.add_argument('-t', '--topic', help="Topic to publish to", required=True)
parser.add_argument('--host', help="RabbitMQ host to connect", default='localhost')

args = parser.parse_args()

connection = pika.BlockingConnection(pika.ConnectionParameters(args.host))
channel = connection.channel()

channel.queue_declare(queue=args.topic)

print("Connected to Server!")

for line in sys.stdin:
    print('>', end=' ')
    line = line.rstrip()
    channel.basic_publish(exchange='',
                          routing_key=args.topic,
                          body=line)




