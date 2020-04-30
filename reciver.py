import pika
import dbHandler
import ast
import csv
import json


class RabbirMqServerConfigure(object):
    def __init__(self, queue='storage', sec_queue='confirmation', host='localhost', routing_key='confirmation', exchange=''):
        self.host = host
        self.queue = queue
        self.sec_queue = sec_queue
        self.routing_key = routing_key
        self.exchange = exchange


class RabbitMqServer(object):
    def __init__(self, server):
        self.server = server
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host)
        )
        self._channel = self._connection.channel()
        self._temp = self._channel.queue_declare(queue=self.server.queue)
        self._channel.queue_declare(queue=self.server.sec_queue)

    def callback(self, ch, method, properties, body):
        data = ast.literal_eval(body.decode("iso-8859-1"))
        file_path = data['path']
        db_name = data['table_name']
        file_format = data['file_format']

        y = db.connect()
        print(y)
        if(file_format == 'csv'):
            try:
                with open(file_path, 'r', encoding='utf8') as csv_file:
                    csv_reader = csv.reader(csv_file)
                    next(csv_reader)
                    for line in csv_reader:
                        db.csvDataEntry(line)
                    self.publish(payload="File Uploaded")
            except Exception as e:
                print("data was not sent to db {}".format(e))
            x = db.disconnect()
            print(x)
        else:
            try:
                with open(file_path, 'r', encoding='utf8') as json_file:
                    json_data = json.load(json_file)
                for d in json_data:
                    db.jsonDataEntry(d)
                self.publish(payload="File Uploaded")
            except Exception as e:
                print("data was not sent to db {}".format(e))
            x = db.disconnect()
            print(x)

    def publish(self, payload={}):
        self._channel.basic_publish(
            exchange=self.server.exchange,
            routing_key=self.server.routing_key,
            body=str(payload)
        )
        print("published message: {}".format(payload))

    def startServer(self):
        self._channel.basic_consume(
            queue=self.server.queue, on_message_callback=self.callback, auto_ack=True)
        print('[*] Waiting for messages. To exit press ctrl+c')
        self._channel.start_consuming()


if __name__ == "__main__":
    serverConfigure = RabbirMqServerConfigure(
        queue='storage', host='localhost', routing_key='confirmation', exchange='')
    server = RabbitMqServer(server=serverConfigure)

    db = dbHandler.dbHandler('./database.db')
    server.startServer()
