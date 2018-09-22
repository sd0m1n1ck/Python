import os
from random import *
from google.cloud import pubsub_v1
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"

def publish_messages(project, topic_name):
    """Publishes multiple messages to a Pub/Sub topic."""

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)

    while True:
        import string
        min_char = 1
        max_char = 15
        allchar = string.ascii_letters + string.punctuation + string.digits
        data = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
        data = data.encode('utf-8')
        publisher.publish(topic_path, data=data)
        print(data)

    print('Published messages.')
