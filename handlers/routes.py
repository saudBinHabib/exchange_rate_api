import json
import pandas as pd
import requests

from .database import MySqliteDatabase
from flask import request, jsonify


def configure_routes(app):

    db = MySqliteDatabase()

    @app.route('/')
    def hello_world():
        return 'Hello Z Lab!'

    @app.route('/docs')
    def show_docs():
        return jsonify({
            'Application Welcome': 'Welcome to ExchangeIO api Implementation. There are multiple Api\'s for you.',
            'Call_backs': {
                '1': 'Api for creating Database with your own name. e.g. http://127.0.0.1:5000/create?db=databasename',
                '2': '''Api for inserting the base currency of your choice with starting and ending date
                        of your choice. e.g. http://127.0.0.1:5000/insert?start_at=2018-05-01&end_at=2019-01-3&base=EUR
                        ''',
                '3': '''Api for calling the first number of exchange rates from the view. e.g.
                http://127.0.0.1:5000/exchange?count=5''',
                '4': ''' Api for fetching the exchange rate of a base currency for a target currency on a particular
                date e.g. http://127.0.0.1:5000/rate?base=RUB&target=USD&date=2019-05-02''',
                '5': ''' Api for fetching the exchange rates of a base currency for multiple target currencies on a 
                particular date e.g. http://127.0.0.1:5000/rates?base=RUB&target=['USD','PLN', 'EUR']&date=2019-05-02'''
            }
        })

    @app.route('/create')
    def create_db_structure():
        db_path = request.args['db']
        db.set_db_path(db_path)
        db.connect_db()
        response = db.create_db_structure()
        db.close_connection()
        return jsonify({
            'status': 200,
            'response': response
        })

    @app.route('/insert')
    def insert_dat():
        start_at = request.args['start_at']
        end_at = request.args['end_at']
        base = request.args['base']
        response = requests.get('https://api.exchangeratesapi.io/history?start_at={}&end_at={}&base={}'.format(
            start_at,
            end_at,
            base
        ))

        df = pd.DataFrame(response.json())
        df['dates_data'] = df.index
        df['rates_length'] = df.rates.apply(lambda rate: len(rate))
        df.columns = ['rates', 'start_at', 'base_currency', 'end_at', 'dates_data', 'rates_length']

        db.connect_db()
        response_base_currency = db.insert_base_currency(base)
        dates_inserted = len([db.insert_dates(str(date).split()[0]) for date in df.dates_data.unique().tolist()])
        df.apply(lambda row: db.insert_exchange_rates(row), axis=1)
        db.close_connection()
        exchange_currency_inserted = sum(df.rates_length.tolist())
        return jsonify({
            'status': 200,
            'base_currency': response_base_currency,
            'dates': {
                'status': 200,
                'response': '{} dates inserted.'.format(dates_inserted)
            },
            'exchange_rate': {
                'status': 200,
                'response': '{} Exchange Currencies inserted.'.format(exchange_currency_inserted)
            },
        })

    @app.route('/exchange')
    def exchange_rate_fetch():
        count = request.args['count']
        db.connect_db()
        response = db.exchange_rate_count(count)
        db.close_connection()
        return jsonify({
            'status': 200,
            'response': response
        })

    @app.route('/rate')
    def conversion_rate():
        base = request.args['base']
        target = request.args['target']
        date = request.args['date']
        db.connect_db()
        response = db.conversion_rate(base, target, date)
        db.close_connection()
        return jsonify({
            'status': 200,
            'response': response
        })

    @app.route('/rates')
    def conversion_rates():
        base = request.args['base']
        target = request.args['target']
        date = request.args['date']
        target = '(' + target.strip('[]') + ')'
        db.connect_db()
        response = db.conversion_rates(base, target, date)
        db.close_connection()
        return jsonify({
            'status': 200,
            'response': response
        })
