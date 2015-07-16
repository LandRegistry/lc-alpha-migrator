from kombu import Connection


def write_error(message):
    with Connection('amqp://guest:guest@localhost:5672//') as conn:
        error_queue = conn.SimpleQueue('migration_error_queue')
        error_queue.put(message)
        print('Sent: %s' % message)
        error_queue.close()


def read_error():
    with Connection('amqp://guest:guest@localhost:5672//') as conn:
        error_queue = conn.SimpleQueue('migration_error_queue')
        message = error_queue.get(block=True, timeout=1)
        print("Received: %s" % message.payload)
        message.ack()
        error_queue.close()
        return message.payload


