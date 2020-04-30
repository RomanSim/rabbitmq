
import pika
import platform


class RabitMqConfigure(object):
    def __init__(self, queue='storage', host='localhost', routing_key='storage', exchange=''):
        self.queue = queue
        self.host = host
        self.routing_key = routing_key
        self.exchange = exchange


class RabbitMq(object):
    def __init__(self, server):
        self.server = server
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.server.queue)

    def publish(self, payload={}):
        self._channel.basic_publish(
            exchange=self.server.exchange,
            routing_key=self.server.routing_key,
            body=str(payload)
        )
        print("published message: {}".format(payload))
        self._connection.close()


if __name__ == "__main__":

    server = RabitMqConfigure(
        queue='storage', host='localhost', routing_key='storage', exchange='')
    rabbitmq = RabbitMq(server)
    path = './data/invoices_2009.json'
    check_format = path[-3:]
    if check_format == 'csv':
        file_format = 'csv'
    else:
        file_format = 'json'

    table_name = 'invoices'
    rabbitmq.publish(
        payload={"path": path, "file_format": file_format, "table_name": table_name})
