import pytest
import pandas as pd
from sales.data_holder import DataHolder
from sales.feature_engineering import *


def test_add_sku_warehouse_sales_last_xdays_sales():
    data = {'sku': ["1", "1", "1", "2", "2", "4", "4", "2", "2"],
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
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    days = 7

    df = add_sku_warehouse_sales_last_xdays_sales(df, days)
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-08')
        ][f'sku_warehouse_last_{days}days_sales'].values[0] == 13
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-09')
        ][f'sku_warehouse_last_{days}days_sales'].values[0] == 14
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-19')
        ][f'sku_warehouse_last_{days}days_sales'].values[0] == 0
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ][f'sku_warehouse_last_{days}days_sales'].values[0] == 6


def test_add_sku_historic_sales():
    data = {'sku': [
                "1", "1", "2", "2", "2", "2",
                "2", "4", "4", "4", "4", "4"
            ],
            'quantity': [5, 6, 7, 8, 4, 3,
                         5, 6, 3, 8, 10, 11],
            'date': [
                '08-02-2023', '15-03-2023',
                '09-02-2023', '23-02-2023',
                '02-05-2023', '09-08-2023',
                '16-02-2023', '03-02-2023',
                '10-02-2023', '17-02-2023',
                '24-02-2023', '03-03-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'LASCONDES',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_sku_historic_sales(df)
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-03-15')
        ][f'sku_historic_sales'].values[0] == 5
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-08-09')
        ][f'sku_historic_sales'].values[0] == 24
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-03-03')
        ][f'sku_historic_sales'].values[0] == 27


def test_add_sku_warehouse_historic_sales():
    data = {'sku': [
                "1", "1", "2", "2", "2", "2",
                "2", "4", "4", "4", "4", "4"
            ],
            'quantity': [5, 6, 7, 8, 4, 3, 5, 6, 3, 8, 10, 11],
            'date': [
                '08-02-2023', '15-03-2023',
                '09-02-2023', '23-02-2023',
                '02-05-2023', '09-08-2023',
                '16-02-2023', '03-02-2023',
                '10-02-2023', '17-02-2023',
                '24-02-2023', '03-03-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'LASCONDES',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_sku_warehouse_historic_sales(df)
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-03-15')
        ][f'sku_warehouse_historic_sales'].values[0] == 5
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-23')
        ][f'sku_warehouse_historic_sales'].values[0] == 7
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-08-09')
        ][f'sku_warehouse_historic_sales'].values[0] == 9
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-03-03')
        ][f'sku_warehouse_historic_sales'].values[0] == 27


def test_add_sku_warehouse_historic_sales_same_day_of_the_week():
    data = {'sku': [
                "1", "1", "2", "2", "2", "2",
                "2", "4", "4", "4", "4", "4"
            ],
            'quantity': [5, 6, 7, 8, 4, 3, 5, 6, 3, 8, 10, 11],
            'date': [
                '08-02-2023', '15-02-2023',
                '09-02-2023', '23-02-2023',
                '02-02-2023', '09-02-2023',
                '16-02-2023', '03-02-2023',
                '10-02-2023', '17-02-2023',
                '24-02-2023', '03-03-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'LASCONDES',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_sku_warehouse_historic_sales_same_day_of_the_week(df)
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-08')
        ][f'sku_warehouse_historic_sales_same_day_of_the_week'].values[0] == 0
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-15')
        ][f'sku_warehouse_historic_sales_same_day_of_the_week'].values[0] == 5
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-09')
        ][f'sku_warehouse_historic_sales_same_day_of_the_week'].values[0] == 0
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-17')
        ][f'sku_warehouse_historic_sales_same_day_of_the_week'].values[0] == 9
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-10')
        ][f'sku_warehouse_historic_sales_same_day_of_the_week'].values[0] == 6
    assert \
        df[
            (df['sku'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-03-03')
        ][f'sku_warehouse_historic_sales_same_day_of_the_week'].values[0] == 27


def test_add_sku_warehouse_historic_sales_same_month():
    data = {'sku': ["1", "1", "1", "2", "2", "2"],
            'quantity': [100, 200, 80, 50, 70, 120],
            'date': [
                '08-02-2018', '12-02-2023',
                '27-02-2023', '09-12-2019',
                '09-12-2022', '09-12-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_sku_warehouse_historic_sales_same_month(df)
    assert \
        df[
            (df['sku'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-27')
        ]['sku_warehouse_historic_sales_same_month'].values[0] == 100
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2022-12-09')
        ]['sku_warehouse_historic_sales_same_month'].values[0] == 50
    assert \
        df[
            (df['sku'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-09')
        ]['sku_warehouse_historic_sales_same_month'].values[0] == 120


def test_add_weekday_information():
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
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_weekday_information(df)
    assert df['weekday'].to_list() == [3, 0, 2, 4]


def test_month_information():
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
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_month_information(df)
    assert df['month'].to_list() == [2, 5, 7, 12]


def test_year_information():
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
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_year_information(df)
    assert df['year'].to_list() == [2018, 2023, 2022, 2019]
