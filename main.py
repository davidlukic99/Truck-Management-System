import psycopg2 as pg2
import csv
import time


# create a connection
conn = pg2.connect(database='TMS', user='postgres', password='*********')

# establish connection and start cursor
cur = conn.cursor()

print("Opened database successfully")

# FIRST

query1 = '''
        CREATE TABLE Driver (
            last_name varchar(50) PRIMARY KEY,
            first_name varchar(50) NOT NULL,
            status BOOLEAN NOT NULL
            
        );
        '''

cur.execute(query1)  # execute the query
cur.commit()  # commit the changes to the database

with open('drivers_seed.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Skip the header row
    for row in reader:
        cur.execute(
            "INSERT INTO Driver VALUES (%s, %s, %s, %s)",
            row)

conn.commit()

# SECOND

query2 = '''
        CREATE TABLE Truck (
            truck_number BIGINT PRIMARY KEY,
            status BOOLEAN NOT NULL
        );
        '''

cur.execute(query2)  # execute the query
cur.commit()  # commit the changes to the database

with open('truck_seed.csv', 'r') as f1:
    reader = csv.reader(f1)
    next(reader)  # Skip the header row.
    for row in reader:
        cur.execute(
            "INSERT INTO Truck VALUES (%s, %s)",
            row)

conn.commit()

# THIRD

query3 = '''
        CREATE TABLE Load (
            order_number BIGINT PRIMARY KEY,
            pickup timestamp NOT NULL,
            delivery timestamp NOT NULL 
        );
        '''

cur.execute(query3)  # execute the query
cur.commit()  # commit the changes to the database

with open('load_seed.csv', 'r') as f2:
    reader = csv.reader(f2)
    next(reader)  # Skip the header row.
    for row in reader:
        if abs(delivery - time.time()) == 10 and (pickup - delivery) == 5:
            cur.execute(
                "INSERT INTO Load VALUES (%s, %s, %s)",
                row)

conn.commit()

with open("report.csv", "r") as report_file:
    reader = csv.reader(report_file)
    for row in reader:
        print(row)
