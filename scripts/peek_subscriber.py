"""Print messages arriving on the wiki-edits topic (to check the producer works).

Creates a subscription `wiki-edits-peek` if it does not exist.
Works with the emulator (PUBSUB_EMULATOR_HOST) or real Pub/Sub.
"""
import json
import os
import sys
import time

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import pubsub_v1

sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # titles in any script

PROJECT_ID = os.environ["PROJECT_ID"]
TOPIC_ID = os.getenv("TOPIC_ID", "wiki-edits")
SUB_ID = os.getenv("SUB_ID", "wiki-edits-peek")

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(PROJECT_ID, TOPIC_ID)
sub_path = subscriber.subscription_path(PROJECT_ID, SUB_ID)

# Wait until the producer has created the topic, then subscribe.
while True:
    try:
        subscriber.create_subscription(name=sub_path, topic=topic_path)
        break
    except AlreadyExists:
        break
    except NotFound:
        print("waiting for topic...")
        time.sleep(2)


def callback(message):
    event = json.loads(message.data)
    print(f"[{message.attributes.get('wiki')}] bot={message.attributes.get('bot')} {event.get('title')}")
    message.ack()


print(f"Listening on {sub_path}")
subscriber.subscribe(sub_path, callback=callback).result()
