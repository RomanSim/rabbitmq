import pika
import dbHandler
import ast
import csv
import json
from plotter import plot


class RabbirMqPlotConfigure(object):
    def __init__(self, queue='confirmation', host='localhost'):
        self.host = host
        self.queue = queue


class RabbitMqPlot(object):
    def __init__(self, server):
        self.server = server
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.server.host)
        )
        self._channel = self._connection.channel()
        self._temp = self._channel.queue_declare(queue=self.server.queue)

    def callback(self, ch, method, properties, body):
        print(str(body))
        db.connect()
        sql = """SELECT strftime('%m-%Y',`InvoiceDate`), SUM(Total) FROM invoices GROUP BY strftime('%Y',`InvoiceDate`), strftime('%m',`InvoiceDate`) """
        x = db.select(sql)
        db.connect()
        sql = """SELECT strftime('%m-%Y',`InvoiceDate`), Count(DISTINCT CustomerId) FROM invoices GROUP BY strftime('%Y',`InvoiceDate`), strftime('%m',`InvoiceDate`) """
        y = db.select(sql)

        customers = []
        sales = []
        month = []

        for customer, sale in zip(x, y):
            month.append(customer[0])
            sales.append(sale[1])
            customers.append(customer[1])

        plot(month, sales, customers)

    def startServer(self):
        self._channel.basic_consume(
            queue=self.server.queue, on_message_callback=self.callback, auto_ack=True)

        print('[*] Waiting for messages. To exit press ctrl+c')
        self._channel.start_consuming()


if __name__ == "__main__":
    serverConfigure = RabbirMqPlotConfigure(
        queue='confirmation', host='localhost')
    server = RabbitMqPlot(server=serverConfigure)

    db = dbHandler.dbHandler('./database.db')
    server.startServer()
