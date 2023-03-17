import pytest
import pandas as pd
from sales.data_holder import DataHolder
from sales.feature_engineering.warehouse_related_features import *
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


def test_add_warehouse_is_metropolitan_zone(test_db):
    data_sale = {
                'sku': ["1", "1", "1", "1", "1", "1"],
                'quantity': [100, 200, 80, 50, 70, 120],
                'date': [
                    '05-02-2023', '06-02-2023',
                    '12-02-2023', '14-02-2023',
                    '08-02-2023', '09-02-2023'
                ],
                'warehouse': [
                    'LASCONDES', 'PTOVARAS',
                    'COYHAIQUE', 'LADEHESA',
                    'MALLSPORT', 'CONCEPCION'
                ]}
    data_warehouse = {
                'warehouse': [
                    'COYHAIQUE', 'LADEHESA',
                    'MALLSPORT', 'CONCEPCION',
                    'LASCONDES', 'PTOVARAS'
                ],
                'is_metropolitan_zone': [0, 1, 1, 0, 1, 0]
                }
    """Create temporal warehouse database"""
    with test_db.cursor() as cursor:
        cursor.execute('''
            CREATE TEMPORARY TABLE df_warehouse (
                warehouse VARCHAR,
                is_metropolitan_zone INT
            );
            ''')
        for i in range(len(data_warehouse[next(iter(data_warehouse))])):
            query = f'''
            INSERT INTO df_warehouse (warehouse, is_metropolitan_zone)
            VALUES ('{data_warehouse['warehouse'][i]}',
            {data_warehouse['is_metropolitan_zone'][i]})
            '''
            cursor.execute(query)

    """Create temporal sale database"""
    create_datatable(test_db, data_sale)

    """Apply function"""
    add_warehouse_is_metropolitan_zone(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-05')
        ]['is_metropolitan_zone'].values[0] == 1
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'PTOVARAS') &
            (df['date'] == '2023-02-06')
        ]['is_metropolitan_zone'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'COYHAIQUE') &
            (df['date'] == '2023-02-12')
        ]['is_metropolitan_zone'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-14')
        ]['is_metropolitan_zone'].values[0] == 1


def test_add_warehouse_is_inside_mall(test_db):
    data_sale = {
                'sku': ["1", "1", "1", "1", "1", "1"],
                'quantity': [100, 200, 80, 50, 70, 120],
                'date': [
                    '05-02-2023', '06-02-2023',
                    '12-02-2023', '14-02-2023',
                    '08-02-2023', '09-02-2023'
                ],
                'warehouse': [
                    'LASCONDES', 'PTOVARAS',
                    'COYHAIQUE', 'LADEHESA',
                    'MALLSPORT', 'CONCEPCION'
                ]}
    data_warehouse = {
                'warehouse': [
                    'COYHAIQUE', 'LADEHESA',
                    'MALLSPORT', 'CONCEPCION',
                    'OTRAUBICACION', 'PTOVARAS'
                ],
                'is_inside_mall': [0, 1, 1, 1, 1, 0]
                }
    """Create temporal warehouse database"""
    with test_db.cursor() as cursor:
        cursor.execute('''
            CREATE TEMPORARY TABLE df_warehouse (
                warehouse VARCHAR,
                is_inside_mall INT
            );
            ''')
        for i in range(len(data_warehouse[next(iter(data_warehouse))])):
            query = f'''
            INSERT INTO df_warehouse (warehouse, is_inside_mall)
            VALUES ('{data_warehouse['warehouse'][i]}',
            {data_warehouse['is_inside_mall'][i]})
            '''
            cursor.execute(query)

    """Create temporal sale database"""
    create_datatable(test_db, data_sale)

    """Apply function"""
    add_warehouse_is_inside_mall(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-05')
        ]['is_inside_mall'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'PTOVARAS') &
            (df['date'] == '2023-02-06')
        ]['is_inside_mall'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'CONCEPCION') &
            (df['date'] == '2023-02-09')
        ]['is_inside_mall'].values[0] == 1
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-14')
        ]['is_inside_mall'].values[0] == 1


def test_add_warehouse_last_xdays_sales(test_db):
    data = {'sku': ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            'quantity': [5, 6, 7, 8, 4, 3, 5, 6, 3],
            'date': [
                '08-02-2023', '06-02-2023',
                '07-02-2023', '08-02-2023',
                '19-02-2023', '08-02-2023',
                '07-02-2023', '02-02-2023',
                '09-02-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT',
                'MALLSPORT', 'LADEHESA',
                'COSTANERA', 'MALLSPORT',
                'MALLSPORT'
            ]}
    """Create temporal sale database"""
    create_datatable(test_db, data)

    """Apply function"""
    days = 7
    add_warehouse_last_xdays_sales(test_db, days)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-08')
        ][f'warehouse_last_{days}days_sales'].values[0] == 13
    assert \
        df[
            (df['sku'] == '9') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-09')
        ][f'warehouse_last_{days}days_sales'].values[0] == 14
    assert \
        df[
            (df['sku'] == '5') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-19')
        ][f'warehouse_last_{days}days_sales'].values[0] == 0
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ][f'warehouse_last_{days}days_sales'].values[0] == 6


def test_add_warehouse_last_xdays_mean_sales(test_db):
    data = {'sku': ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            'quantity': [5, 6, 7, 8, 4, 3, 5, 6, 3],
            'date': [
                '08-02-2023', '06-02-2023',
                '07-02-2023', '08-02-2023',
                '19-02-2023', '08-02-2023',
                '07-02-2023', '02-02-2023',
                '09-02-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT',
                'MALLSPORT', 'LADEHESA',
                'COSTANERA', 'MALLSPORT',
                'MALLSPORT'
            ]}
    """Create temporal sale database"""
    create_datatable(test_db, data)

    """Apply function"""
    days = 7
    add_warehouse_last_xdays_mean_sales(test_db, days)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-08')
        ][f'warehouse_last_{days}days_mean_sales'
          ].values[0] == round(float(13/days), 4)
    assert \
        df[
            (df['sku'] == '9') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-09')
        ][f'warehouse_last_{days}days_mean_sales'
          ].values[0] == round(float(14/days), 4)
    assert \
        df[
            (df['sku'] == '5') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-19')
        ][f'warehouse_last_{days}days_mean_sales'
          ].values[0] == round(float(0/days), 4)
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ][f'warehouse_last_{days}days_mean_sales'
          ].values[0] == round(float(6/days), 4)


def test_add_warehouse_cumulative_sales_in_the_week(test_db):
    data = {'sku': ["1", "2", "3", "4", "5", "6", "7"],
            'quantity': [100, 200, 80, 50, 70, 120, 50],
            'date': [
                '05-02-2023', '06-02-2023',
                '12-02-2023', '14-02-2023',
                '08-02-2023', '09-02-2023',
                '16-02-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LADEHESA', 'LADEHESA',
                'MALLSPORT', 'MALLSPORT',
                'LADEHESA'
            ]}
    """Create temporal sale database"""
    create_datatable(test_db, data)

    """Apply function"""
    add_warehouse_cumulative_sales_in_the_week(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-06')
        ]['warehouse_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['sku'] == '3') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-12')
        ]['warehouse_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-14')
        ]['warehouse_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['sku'] == '7') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-16')
        ]['warehouse_cumulative_sales_in_the_week'].values[0] == 50
    assert \
        df[
            (df['sku'] == '5') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ]['warehouse_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['sku'] == '6') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-09')
        ]['warehouse_cumulative_sales_in_the_week'].values[0] == 70


def test_add_warehouse_cumulative_sales_in_the_month(test_db):
    data = {'sku': ["1", "2", "3", "4", "5", "6", "7", "8"],
            'quantity': [100, 200, 80, 50, 70, 120, 80, 25],
            'date': [
                '08-02-2018', '12-02-2023',
                '27-02-2023', '28-02-2023',
                '09-11-2022', '09-12-2023',
                '18-12-2023', '29-12-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LADEHESA', 'LADEHESA',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT'
            ]}
    """Create temporal sale database"""
    create_datatable(test_db, data)

    """Apply function"""
    add_warehouse_cumulative_sales_in_the_month(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-12')
        ]['warehouse_cumulative_sales_in_the_month'].values[0] == 0
    assert \
        df[
            (df['sku'] == '3') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-27')
        ]['warehouse_cumulative_sales_in_the_month'].values[0] == 0
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-28')
        ]['warehouse_cumulative_sales_in_the_month'].values[0] == 80
    assert \
        df[
            (df['sku'] == '5') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2022-11-09')
        ]['warehouse_cumulative_sales_in_the_month'].values[0] == 0
    assert \
        df[
            (df['sku'] == '6') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-09')
        ]['warehouse_cumulative_sales_in_the_month'].values[0] == 0
    assert \
        df[
            (df['sku'] == '7') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-18')
        ]['warehouse_cumulative_sales_in_the_month'].values[0] == 120
    assert \
        df[
            (df['sku'] == '8') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-29')
        ]['warehouse_cumulative_sales_in_the_month'].values[0] == 200


def test_add_warehouse_cumulative_sales_in_the_year(test_db):
    data = {'sku': ["1", "1", "1", "1", "1", "1", "2", "2"],
            'quantity': [100, 200, 80, 50, 70, 120, 50, 80],
            'date': [
                '08-02-2018', '12-02-2023',
                '27-04-2023', '28-05-2023',
                '09-11-2023', '09-12-2018',
                '19-12-2023', '29-12-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LADEHESA', 'LADEHESA',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT'
            ]}
    """Create temporal sale database"""
    create_datatable(test_db, data)

    """Apply function"""
    add_warehouse_cumulative_sales_in_the_year(test_db)

    """Check the results"""
    with test_db.cursor() as cursor:
        cursor.execute('SELECT * FROM df_sale;')
        df = pd.DataFrame(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        df['date'] = pd.to_datetime(df['date'])
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-12')
        ]['warehouse_cumulative_sales_in_the_year'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-04-27')
        ]['warehouse_cumulative_sales_in_the_year'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-05-28')
        ]['warehouse_cumulative_sales_in_the_year'].values[0] == 80
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-11-09')
        ]['warehouse_cumulative_sales_in_the_year'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2018-12-09')
        ]['warehouse_cumulative_sales_in_the_year'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2018-02-08')
        ]['warehouse_cumulative_sales_in_the_year'].values[0] == 0
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-19')
        ]['warehouse_cumulative_sales_in_the_year'].values[0] == 70
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-29')
        ]['warehouse_cumulative_sales_in_the_year'].values[0] == 120
