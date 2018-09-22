import time,os
from google.cloud import pubsub_v1
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"

def receive_messages(project, subscription_name):

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription_name)

    def callback(message):
        print('{}'.format(message.data))

        from google.cloud import bigquery
        client = bigquery.Client()
        dataset_ref = client.dataset('bqdataflow')
        table_ref = dataset_ref.table('df2pubsub')

        table = client.get_table(table_ref)

        errors = client.insert_rows(table, message.data)
        if not errors:
            print('Loaded {} row(s)'.format(len(message.data)))
        else:
            print('Errors:')
            for error in errors:
                print(error)

        message.ack()
        
    subscriber.subscribe(subscription_path, callback=callback)

    print('Listening for messages on {}'.format(subscription_path))
    while True:
        print('')
        time.sleep(60)
