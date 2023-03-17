import pytest
import pandas as pd
from sales.data_holder import DataHolder
from sales.feature_engineering.date_related_features import *
from sales.local_settings import *


@pytest.fixture
def test_db():
    conn = psycopg2.connect(
        host=TESTDBHOST,
        dbname=TESTDBNAME,
        user=TESTDBUSER,
        password=TESTDBPASS
    )
    create_query = '''
    CREATE TEMPORARY TABLE df_sale (
        sku VARCHAR,
        quantity INT,
        date DATE,
        warehouse VARCHAR
    );'''
    cur = conn.cursor()
    cur.execute(create_query)

    yield conn
    conn.close()


def create_datatable(conn, data):
    with conn.cursor() as cursor:
        cursor.execute('TRUNCATE df_sale;')
        for i in range(len(data[next(iter(data))])):
            query = f'''
            INSERT INTO df_sale (sku, quantity, date, warehouse)
            VALUES ('{data['sku'][i]}', {data['quantity'][i]},
            '{data['date'][i]}', '{data['warehouse'][i]}')
            '''
            cursor.execute(query)


def test_add_weekday_information(test_db):
    data = {'sku': ["1", "1", "1", "2"],
            'quantity': [5, 6, 7, 8],
            'date': [
                '08-02-2023', '26-02-2023',
                '07-02-2023', '09-02-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT'
            ]}
    """Create temporal database"""
    create_datatable(test_db, data)

    """Apply function"""
    add_weekday_information(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert df['weekday'].to_list() == [3, 0, 2, 4]


def test_add_week_information(test_db):
    data = {'sku': ["1", "1", "1", "2"],
            'quantity': [5, 6, 7, 8],
            'date': [
                '08-02-2021', '26-02-2023',
                '07-02-2023', '05-04-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT'
            ]}
    """Create temporal database"""
    create_datatable(test_db, data)

    """Apply function"""
    add_week_information(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert df['week'].to_list() == [6, 8, 6, 14]


def test_month_information(test_db):
    data = {'sku': ["1", "1", "1", "2"],
            'quantity': [5, 6, 7, 8],
            'date': [
                '08-02-2023', '26-05-2023',
                '07-07-2023', '09-12-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT'
            ]}
    """Create temporal database"""
    create_datatable(test_db, data)

    """Apply function"""
    add_month_information(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert df['month'].to_list() == [2, 5, 7, 12]


def test_year_information(test_db):
    data = {'sku': ["1", "1", "1", "2"],
            'quantity': [5, 6, 7, 8],
            'date': [
                '08-02-2018', '26-05-2023',
                '07-07-2022', '09-12-2019'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT'
            ]}
    """Create temporal database"""
    create_datatable(test_db, data)

    """Apply function"""
    add_year_information(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert df['year'].to_list() == [2018, 2023, 2022, 2019]


def test_add_offer_day_information(test_db):
    data_sale = {
                'sku': ["1", "1", "1", "1", "1", "1"],
                'quantity': [100, 200, 80, 50, 70, 120],
                'date': [
                    '05-02-2023', '06-02-2023',
                    '12-02-2023', '14-02-2023',
                    '08-02-2023', '09-02-2023'
                ],
                'warehouse': [
                    'LASCONDES', 'LASCONDES',
                    'LADEHESA', 'LADEHESA',
                    'MALLSPORT', 'MALLSPORT'
                ]}
    data_campaign = {
            'start_date': [
                '04-02-2023', '13-02-2023',
                '09-02-2023'
            ],
            'end_date': [
                '07-02-2023', '15-02-2023',
                '10-02-2023'
            ],
            'campaign_name': [
                'SALE1', 'SALE2',
                'SALE3'
            ]}
    """Create temporal campaign database"""
    with test_db.cursor() as cursor:
        cursor.execute('''
            CREATE TEMPORARY TABLE df_campaign (
                start_date DATE,
                end_date DATE,
                campaign_name VARCHAR
            );
            ''')
        for i in range(len(data_campaign[next(iter(data_campaign))])):
            query = f'''
            INSERT INTO df_campaign (start_date, end_date, campaign_name)
            VALUES ('{data_campaign['start_date'][i]}',
            '{data_campaign['end_date'][i]}',
            '{data_campaign['campaign_name'][i]}')
            '''
            cursor.execute(query)

    """Create temporal sale database"""
    create_datatable(test_db, data_sale)

    """Apply function"""
    add_offer_day_information(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert df.shape[0] == len(data_sale[next(iter(data_sale))])
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-05')
        ]['is_offer_day'].values[0] == 1
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-12')
        ]['is_offer_day'].values[0] == 0
