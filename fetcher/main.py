from kafka import KafkaConsumer
def get_stream(set):
    try:

        consumer = KafkaConsumer('test-topic',
                            bootstrap_servers=['kafka1:9092','kafka2:9092','kafka3:9092'])
        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value.decode('utf-8')))
    except Exception as e:
        print(e)

def main():
    get_stream(set)

if __name__ == "__main__":
    main()