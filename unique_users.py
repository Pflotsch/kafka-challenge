from confluent_kafka import Consumer, KafkaError, Producer
import pandas as pd
import json
import datetime

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'gid1',
    'default.topic.config': {
        'auto.offset.reset': 'earliest'
    }
})

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'cid1'})


# DataFrame to store data in memory
df_user = pd.DataFrame(columns=['ts', 'uid'])

# Function that counts unique user for a given time intervall [ts_start, ts_stop]
def count_user(df,ts_start, ts_stop):
    df_filtered = df.apply(lambda row: row[df['ts'] < ts_stop])
    df_new = df.apply(lambda row: row[df['ts'] >= ts_stop])
    number_of_unique_users = df_filtered['uid'].nunique()
    ts_start_unix = pd.Timestamp(ts_start.strftime('%Y-%m-%d %H:%M:%S')).timestamp()
    ts_stop_unix = pd.Timestamp(ts_stop.strftime('%Y-%m-%d %H:%M:%S')).timestamp()
    user_per_min = {'ts_start': ts_start_unix, 'ts_end': ts_stop_unix, 'number_of_users': number_of_unique_users}
    return df_new, user_per_min


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

running = True
first = True

consumer.subscribe(['website_visits'])

while running:
    msg = consumer.poll(1.0)

    if msg is None:
        print("No message")
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
        elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            sys.stderr.write('Topic unknown, creating website_visits topic\n')
        elif msg.error():
            raise KafkaException(msg.error())
    else:
        msg_dict = json.loads(msg.value().decode('utf-8'))
        if first:
            ts_start = datetime.datetime.fromtimestamp(int(msg_dict['ts']))
            #ts_start = datetime.datetime.now() #if we work with live streamed data (and not historical data)
            ts_stop = ts_start + datetime.timedelta(0, 60)
            ts_delta = ts_start + datetime.timedelta(0, 65) #to handle not strictly ordered data (and in live szenario to receive 99.9% of the data)
            first = False
        ts = datetime.datetime.fromtimestamp(int(msg_dict['ts']))
        uid = msg_dict['uid']
        # print('Received message: {}, {}'.format(ts, uid))
        if ts <= ts_delta:
            df_new_user = pd.DataFrame({'ts': ts,'uid': uid}, index=[0])
            df_user = pd.concat([df_user, df_new_user], ignore_index=True)  #could be a bottle neck and performance need to be tested
        else:
            #print(ts_start)
            #print(ts_stop)
            #print(ts_delta)
            df_user, user_count = count_user(df_user,ts_start, ts_stop)
            print(user_count) # print to stdout
            producer.produce('unique_website_visits_per_minute', value=json.dumps(user_count), callback=acked)
            producer.poll(1)
            ts_start = ts_stop
            ts_stop = ts_start + datetime.timedelta(0, 60)
            ts_delta = ts_start + datetime.timedelta(0, 65)
    if msg.value().decode('utf-8') == "STOP":
        running = False

consumer.close()