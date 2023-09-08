"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os
import csv


path_dir = os.path.dirname(os.path.abspath(__file__))

customers_path = os.path.join(path_dir, "north_data", "customers_data.csv")
employees_path = os.path.join(path_dir, "north_data", "employees_data.csv")
orders_path = os.path.join(path_dir, "north_data", "orders_data.csv")

with psycopg2.connect(host='localhost', database='north', user='postgres', password='9501718367') as conn:
    with conn.cursor() as cur:
        with open(employees_path, encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    cur.execute('INSERT INTO employees VALUES(%s, %s, %s, %s,%s, %s)',
                                   (row['employee_id'], row['first_name'], row['last_name'], row['title'],
                                    row['birth_date'], row['notes']))

        with open(customers_path, encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    cur.execute('INSERT INTO customers VALUES(%s, %s, %s)',
                                   (row['customer_id'], row['company_name'], row['contact_name']))

        with open(orders_path, encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)

                for row in reader:
                    cur.execute('INSERT INTO orders VALUES(%s, %s, %s, %s, %s)',
                                   (row['order_id'], row['customer_id'], row['employee_id'], row['order_date'],
                                    row['ship_city']))
conn.close()