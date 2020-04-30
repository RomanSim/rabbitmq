import sqlite3
import ast
import datetime


class dbHandler:
    def __init__(self, dbPath):
        self.path = dbPath
        self.conn = None
        self.cursor = None

    def getPath(self):
        return self.path

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.path)
            self.cursor = self.conn.cursor()
            return True
        except:
            print('connection error')
            return -1

    def disconnect(self):
        check = False
        if self.cursor:
            try:
                self.conn.commit()
                check = True
            except Exception as e:
                print("not commited {}".format(e))
            self.cursor.close()
            print("disconnected from database")
        return check

    def csvDataEntry(self, line):
        try:
            self.cursor.execute("""INSERT INTO invoices(InvoiceId, CustomerID, InvoiceDate, BillingAddress,
            BillingCity, BillingState, BillingCountry, BillingPostalCode,Total)
                            VALUES(?,?,?,?,?,?,?,?,?) """, (int(line[0]), int(line[1]), line[2], line[3], line[4], line[5], line[6], line[7], float(line[8])))
        except Exception as e:
            print("not uploaded {}".format(e))

    def jsonDataEntry(self, dicti):
        invoiceId = dicti.get('InvoiceId')
        customerId = dicti.get('CustomerId')
        total = dicti.get('Total')
        date = dicti.get('InvoiceDate')
        date_time_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        try:
            self.cursor.execute("""INSERT INTO invoices(InvoiceId, CustomerID, InvoiceDate, BillingAddress,
            BillingCity, BillingState, BillingCountry, BillingPostalCode, Total)
                            VALUES(?,?,?,?,?,?,?,?,?) """, (int(invoiceId), int(customerId), date_time_obj, dicti.get('BillingAddress'), dicti.get('BillingCity'), dicti.get('BillingState'), dicti.get('BillingCountry'), dicti.get('BillingPostalCode'), float(total)))

        except Exception as e:
            print("not uploaded {}".format(e))

    def select(self, query):
        cur = self.conn
        curs = self.cursor.execute(query)
        rs = curs.fetchall()
        self.disconnect()
        return rs
