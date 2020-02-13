
import sqlite3
import os

from sqlite3 import Error


class MySqliteDatabase:

    conn = None
    cursor = None
    DATABASE_PATH = None

    def __init__(self):
        self.PROJECT_ROOT = os.path.dirname(os.path.realpath('./database/'))

    def close_connection(self):
        self.conn.close()

    def set_db_path(self, db):
        self.DATABASE_PATH = os.path.join(self.PROJECT_ROOT, 'database', db+'.db')

    def connect_db(self):
        self.conn = sqlite3.Connection(self.DATABASE_PATH)
        self.cursor = self.conn.cursor()

    def create_date_table(self):
        # creating the date
        try:
            self.cursor.execute(
                """
                    CREATE TABLE date_table (
                        id INTEGER PRIMARY KEY,
                        date_date TEXT UNIQUE NOT NULL
                    )
                """
            )
            return 'Created_Table'
        except Error:
            return 'date_table is already existed.'

    def create_base_currency_table(self):
        # creating the base_currency_table
        try:
            self.cursor.execute(
                """
                    CREATE TABLE base_currency_table (
                        id INTEGER PRIMARY KEY,
                        base_currency TEXT(3) NOT NULL UNIQUE
                    )
                """
            )
            return 'Created_Table'
        except Error:
            return 'base_currency_table is already existed.'

    def create_currency_exchange_table(self):
        # creating the currency_exchange_table
        try:
            self.cursor.execute(
                """
                    CREATE TABLE currency_exchange_table (
                        id INTEGER PRIMARY KEY,
                        date_id      INTEGER NOT NULL,
                        bc_id      INTEGER NOT NULL,
                        target_currency TEXT(3) NOT NULL,
                        exchange_rate REAL(13,4) NOT NULL,
                        FOREIGN KEY (date_id) REFERENCES date_table (id),
                        FOREIGN KEY (bc_id) REFERENCES base_currency_table (id)
                )
                """
            )
            return 'Created_Table'
        except Error:
            return 'currency_exchange_table is already existed.'

    def create_exchange_rate_view(self):
        try:
            self.cursor.execute(
                '''
                    CREATE VIEW exchange_rate
                    AS 
                        SELECT c.date_date AS date_date, b.base_currency AS Base, c.target_currency as Target,
                         c.exchange_rate as Rate FROM base_currency_table as b  INNER JOIN(
                            SELECT * FROM date_table  AS d INNER JOIN currency_exchange_table as c ON  d.id = c.date_id
                        ) AS c
                        ON c.bc_id = b.id;
                '''
            )
            return 'Created View.'
        except Error:
            return 'Exchange_Rate View is already existed.'

    def create_db_structure(self):
        self.connect_db()
        response_dt = self.create_date_table()
        response_bc = self.create_base_currency_table()
        response_ce = self.create_currency_exchange_table()
        response_ev = self.create_exchange_rate_view()
        self.close_connection()
        return {
            'date_table. ': response_dt,
            'base_currency_table. ': response_bc,
            'currency_exchange_table. ': response_ce,
            'Exchange_Rate_view': response_ev,
        }

    def insert_base_currency(self, currency):
        try:
            with self.conn:
                self.cursor.execute(
                    "INSERT INTO base_currency_table (base_currency) VALUES (:bc)",
                    {'bc': str(currency)}
                )
            return {
                'status': 200,
                'response': 'Data Inserted.'
            }
        except Error:
            return {
                'status': 202,
                'response': 'Data Already Existed.'
            }

    def insert_dates(self, date):
        try:
            with self.conn:
                self.cursor.execute(
                    "INSERT INTO date_table (date_date) VALUES (:dd)",
                    {'dd': str(date)}
                )
            return {
                'status': 200,
                'response': 'Data Inserted.'
            }
        except Error as err:
            return err

    def insert_exchange_rates(self, row):
        try:
            date = str(row.dates_data).split()[0]
            rates = row.rates
            base = str(row.base_currency)
            self.cursor.execute(
                '''
                    SELECT id
                    FROM base_currency_table where base_currency="{}"
                '''.format(base))
            base_id = self.cursor.fetchone()[0]
            self.cursor.execute(
                '''
                    SELECT id
                    FROM date_table where date_date='{}'
                '''.format(date))
            date_id = self.cursor.fetchone()[0]
            with self.conn:
                for currency, rate in rates.items():
                    self.cursor.execute(
                        """
                            INSERT INTO currency_exchange_table(
                            date_id, target_currency, exchange_rate, bc_id )
                            VALUES ({}, '{}', {}, {})
                        """.format(
                            date_id, currency, rate, base_id)
                    )
            return {
                'status': 200,
                'response': 'Data Inserted.'
            }
        except Error as err:
            return {
                'status': 200,
                'response': err
            }

    def exchange_rate_count(self, count):
        try:
            self.cursor.execute(
                "select * FROM exchange_rate LIMIT {}".format(count)
            )
            result = self.cursor.fetchall()
            return result if result else 'Empty'
        except Error as err:
            return err

    def conversion_rate(self, base, target, date):
        try:
            self.cursor.execute(
                '''
                    SELECT * FROM Exchange_Rate
                    where base = '{}' and target = '{}'
                    and date_date = '{}';
                '''.format(base, target, date)
            )
            result = self.cursor.fetchone()
            return result if result else 'Empty'
        except Error as err:
            return err

    def conversion_rates(self, base, target, date):
        try:
            self.cursor.execute(
                '''
                    SELECT * FROM Exchange_Rate
                    where base = '{}' and target in {}
                    and date_date = '{}';
                '''.format(base, target, date)
            )
            result = self.cursor.fetchall()
            return result if result else 'Empty'
        except Error as err:
            return err
