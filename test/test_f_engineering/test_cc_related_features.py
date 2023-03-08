import pytest
import pandas as pd
from sales.data_holder import DataHolder
from sales.f_engineering.cc_related_features import *


def test_add_cc_warehouse_last_xdays_sales():
    data = {'cc': ["1", "1", "1", "2", "2", "4", "4", "2", "2"],
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

    df = add_cc_warehouse_last_xdays_sales(df, days)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-08')
        ][f'cc_warehouse_last_{days}days_sales'].values[0] == 13
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-09')
        ][f'cc_warehouse_last_{days}days_sales'].values[0] == 14
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-19')
        ][f'cc_warehouse_last_{days}days_sales'].values[0] == 0
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ][f'cc_warehouse_last_{days}days_sales'].values[0] == 6


def test_add_y_cc_warehouse_next_xdays_sales():
    data = {'cc': ["1", "1", "1", "2", "2", "4", "4", "2", "2", "2"],
            'quantity': [5, 6, 7, 8, 4, 3, 5, 6, 3, 5],
            'date': [
                '08-02-2023', '06-02-2023',
                '07-02-2023', '08-02-2023',
                '19-02-2023', '08-02-2023',
                '07-02-2023', '02-02-2023',
                '09-02-2023', '10-02-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT',
                'MALLSPORT', 'LADEHESA',
                'COSTANERA', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    days = 7

    df = add_y_cc_warehouse_next_xdays_sales(df, days)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-06')
        ][f'y_cc_warehouse_next_{days}days_sales'].values[0] == 12
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ][f'y_cc_warehouse_next_{days}days_sales'].values[0] == 8
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-02')
        ][f'y_cc_warehouse_next_{days}days_sales'].values[0] == 11
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-19')
        ][f'y_cc_warehouse_next_{days}days_sales'].values[0] == 0


def test_add_cc_warehouse_last_xdays_mean_sales():
    data = {'cc': ["1", "1", "1", "2", "2", "2", "2", "2"],
            'quantity': [5, 6, 7, 8, 4, 7, 2, 5],
            'date': [
                '08-02-2023', '06-02-2023',
                '07-02-2023', '08-02-2023',
                '19-02-2023', '22-02-2023',
                '23-02-2023', '24-02-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    days = 7

    df = add_cc_warehouse_last_xdays_mean_sales(df, days)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-08')
        ][f'cc_warehouse_last_{days}days_mean_sales'].values[0] == float(6.5)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-07')
        ][f'cc_warehouse_last_{days}days_mean_sales'].values[0] == float(6.0)
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-19')
        ][f'cc_warehouse_last_{days}days_mean_sales'].values[0] == float(0.0)
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ][f'cc_warehouse_last_{days}days_mean_sales'].values[0] == float(0.0)
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-24')
        ][f'cc_warehouse_last_{days}days_mean_sales'].values[0] == float(4.33)


def test_add_cc_historic_sales():
    data = {'cc': [
                "1", "1", "2", "2", "2", "2",
                "2", "4", "4", "4", "4", "4", "2"
            ],
            'quantity': [5, 6, 7, 8, 4, 3,
                         5, 6, 3, 8, 10, 11, 5],
            'date': [
                '08-02-2023', '15-03-2023',
                '09-02-2023', '23-02-2023',
                '02-05-2023', '09-08-2023',
                '16-02-2023', '03-02-2023',
                '10-02-2023', '17-02-2023',
                '24-02-2023', '03-03-2023',
                '23-02-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'LASCONDES',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'MALLSPORT',
                'COSTANERA'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_historic_sales(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-03-15')
        ][f'cc_historic_sales'].values[0] == 5
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-23')
        ][f'cc_historic_sales'].values[0] == 12
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'COSTANERA') &
            (df['date'] == '2023-02-23')
        ][f'cc_historic_sales'].values[0] == 12
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-08-09')
        ][f'cc_historic_sales'].values[0] == 29
    assert \
        df[
            (df['cc'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-03-03')
        ][f'cc_historic_sales'].values[0] == 27


def test_add_cc_historic_sales_same_day_of_the_week():
    data = {'cc': [
                "1", "1", "2", "2", "2", "2",
                "2", "4", "4", "4", "4", "4"
            ],
            'quantity': [5, 6, 7, 8, 4, 3,
                         5, 6, 3, 8, 10, 11],
            'date': [
                '08-02-2023', '15-02-2023',
                '09-02-2023', '23-02-2023',
                '02-02-2023', '09-02-2023',
                '16-02-2023', '03-02-2023',
                '10-02-2023', '17-02-2023',
                '24-02-2023', '03-03-2023'
            ],
            'warehouse': [
                'LASCONDES', 'COSTANERA',
                'LASCONDES', 'LASCONDES',
                'MALLSPORT', 'MALLSPORT',
                'MALLSPORT', 'COSTANERA',
                'LADEHESA', 'MALLSPORT',
                'TEMUCO', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_historic_sales_same_day_of_the_week(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-08')
        ][f'cc_historic_sales_same_day_of_the_week'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'COSTANERA') &
            (df['date'] == '2023-02-15')
        ][f'cc_historic_sales_same_day_of_the_week'].values[0] == 5
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-09')
        ][f'cc_historic_sales_same_day_of_the_week'].values[0] == 4
    assert \
        df[
            (df['cc'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-17')
        ][f'cc_historic_sales_same_day_of_the_week'].values[0] == 9
    assert \
        df[
            (df['cc'] == '4') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-10')
        ][f'cc_historic_sales_same_day_of_the_week'].values[0] == 6
    assert \
        df[
            (df['cc'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-03-03')
        ][f'cc_historic_sales_same_day_of_the_week'].values[0] == 27


def test_add_cc_historic_sales_same_month():
    data = {'cc': ["1", "1", "1", "2", "2", "2"],
            'quantity': [100, 200, 80, 50, 70, 120],
            'date': [
                '08-02-2018', '12-02-2023',
                '27-02-2023', '09-12-2019',
                '09-12-2022', '09-12-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LADHESA',
                'COSTANERA', 'TEMUCO',
                'MALLSPORT', 'PUCON'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_historic_sales_same_month(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'COSTANERA') &
            (df['date'] == '2023-02-27')
        ]['cc_historic_sales_same_month'].values[0] == 100
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2022-12-09')
        ]['cc_historic_sales_same_month'].values[0] == 50
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'PUCON') &
            (df['date'] == '2023-12-09')
        ]['cc_historic_sales_same_month'].values[0] == 120


def test_add_cc_warehouse_historic_sales():
    data = {'cc': [
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

    df = add_cc_warehouse_historic_sales(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-03-15')
        ][f'cc_warehouse_historic_sales'].values[0] == 5
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-23')
        ][f'cc_warehouse_historic_sales'].values[0] == 7
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-08-09')
        ][f'cc_warehouse_historic_sales'].values[0] == 9
    assert \
        df[
            (df['cc'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-03-03')
        ][f'cc_warehouse_historic_sales'].values[0] == 27


def test_add_cc_warehouse_historic_sales_same_day_of_the_week():
    data = {'cc': [
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

    df = add_cc_warehouse_historic_sales_same_day_of_the_week(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-08')
        ][f'cc_warehouse_historic_sales_same_day_of_the_week'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-15')
        ][f'cc_warehouse_historic_sales_same_day_of_the_week'].values[0] == 5
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-09')
        ][f'cc_warehouse_historic_sales_same_day_of_the_week'].values[0] == 0
    assert \
        df[
            (df['cc'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-17')
        ][f'cc_warehouse_historic_sales_same_day_of_the_week'].values[0] == 9
    assert \
        df[
            (df['cc'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-10')
        ][f'cc_warehouse_historic_sales_same_day_of_the_week'].values[0] == 6
    assert \
        df[
            (df['cc'] == '4') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-03-03')
        ][f'cc_warehouse_historic_sales_same_day_of_the_week'].values[0] == 27


def test_add_cc_warehouse_historic_sales_same_month():
    data = {'cc': ["1", "1", "1", "2", "2", "2"],
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

    df = add_cc_warehouse_historic_sales_same_month(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-27')
        ]['cc_warehouse_historic_sales_same_month'].values[0] == 100
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2022-12-09')
        ]['cc_warehouse_historic_sales_same_month'].values[0] == 50
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-09')
        ]['cc_warehouse_historic_sales_same_month'].values[0] == 120


def test_add_cc_cumulative_sales_in_the_week():
    data = {'cc': ["1", "1", "1", "1", "1", "1"],
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
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_cumulative_sales_in_the_week(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-06')
        ]['cc_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-12')
        ]['cc_cumulative_sales_in_the_week'].values[0] == 390
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-14')
        ]['cc_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ]['cc_cumulative_sales_in_the_week'].values[0] == 200
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-09')
        ]['cc_cumulative_sales_in_the_week'].values[0] == 270


def test_add_cc_cumulative_sales_in_the_month():
    data = {'cc': ["1", "1", "1", "1", "1", "1"],
            'quantity': [100, 200, 80, 50, 70, 120],
            'date': [
                '08-02-2018', '12-02-2023',
                '27-02-2023', '28-02-2023',
                '09-11-2022', '09-12-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LADEHESA', 'LADEHESA',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_cumulative_sales_in_the_month(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-12')
        ]['cc_cumulative_sales_in_the_month'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-27')
        ]['cc_cumulative_sales_in_the_month'].values[0] == 200
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-02-28')
        ]['cc_cumulative_sales_in_the_month'].values[0] == 280
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2022-11-09')
        ]['cc_cumulative_sales_in_the_month'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-09')
        ]['cc_cumulative_sales_in_the_month'].values[0] == 0


def test_add_cc_cumulative_sales_in_the_year():
    data = {'cc': ["1", "1", "1", "1", "1", "1"],
            'quantity': [100, 200, 80, 50, 70, 120],
            'date': [
                '08-02-2018', '12-02-2023',
                '27-04-2023', '28-05-2023',
                '09-11-2023', '09-12-2018'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LADEHESA', 'LADEHESA',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_cumulative_sales_in_the_year(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-12')
        ]['cc_cumulative_sales_in_the_year'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-04-27')
        ]['cc_cumulative_sales_in_the_year'].values[0] == 200
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LADEHESA') &
            (df['date'] == '2023-05-28')
        ]['cc_cumulative_sales_in_the_year'].values[0] == 280
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-11-09')
        ]['cc_cumulative_sales_in_the_year'].values[0] == 330
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2018-12-09')
        ]['cc_cumulative_sales_in_the_year'].values[0] == 100
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2018-02-08')
        ]['cc_cumulative_sales_in_the_year'].values[0] == 0


def test_add_cc_warehouse_cumulative_sales_in_the_week():
    data = {'cc': ["1", "1", "1", "1", "1", "1"],
            'quantity': [100, 200, 80, 50, 70, 120],
            'date': [
                '05-02-2023', '06-02-2023',
                '12-02-2023', '14-02-2023',
                '08-02-2023', '09-02-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'LASCONDES',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_warehouse_cumulative_sales_in_the_week(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-06')
        ]['cc_warehouse_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-12')
        ]['cc_warehouse_cumulative_sales_in_the_week'].values[0] == 200
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-14')
        ]['cc_warehouse_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-08')
        ]['cc_warehouse_cumulative_sales_in_the_week'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-02-09')
        ]['cc_warehouse_cumulative_sales_in_the_week'].values[0] == 70


def test_add_cc_warehouse_cumulative_sales_in_the_month():
    data = {'cc': ["1", "1", "1", "1", "2", "2"],
            'quantity': [100, 200, 80, 50, 70, 120],
            'date': [
                '08-02-2018', '12-02-2023',
                '27-02-2023', '28-02-2023',
                '09-11-2022', '09-12-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'LASCONDES',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_warehouse_cumulative_sales_in_the_month(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-12')
        ]['cc_warehouse_cumulative_sales_in_the_month'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-27')
        ]['cc_warehouse_cumulative_sales_in_the_month'].values[0] == 200
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-28')
        ]['cc_warehouse_cumulative_sales_in_the_month'].values[0] == 280
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2022-11-09')
        ]['cc_warehouse_cumulative_sales_in_the_month'].values[0] == 0
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-09')
        ]['cc_warehouse_cumulative_sales_in_the_month'].values[0] == 0


def test_add_cc_warehouse_cumulative_sales_in_the_year():
    data = {'cc': ["1", "1", "1", "1", "2", "2"],
            'quantity': [100, 200, 80, 50, 70, 120],
            'date': [
                '08-02-2018', '12-02-2023',
                '27-04-2023', '28-05-2023',
                '09-11-2023', '09-12-2023'
            ],
            'warehouse': [
                'LASCONDES', 'LASCONDES',
                'LASCONDES', 'LASCONDES',
                'MALLSPORT', 'MALLSPORT'
            ]}
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_cc_warehouse_cumulative_sales_in_the_year(df)
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-02-12')
        ]['cc_warehouse_cumulative_sales_in_the_year'].values[0] == 0
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-04-27')
        ]['cc_warehouse_cumulative_sales_in_the_year'].values[0] == 200
    assert \
        df[
            (df['cc'] == '1') &
            (df['warehouse'] == 'LASCONDES') &
            (df['date'] == '2023-05-28')
        ]['cc_warehouse_cumulative_sales_in_the_year'].values[0] == 280
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-11-09')
        ]['cc_warehouse_cumulative_sales_in_the_year'].values[0] == 0
    assert \
        df[
            (df['cc'] == '2') &
            (df['warehouse'] == 'MALLSPORT') &
            (df['date'] == '2023-12-09')
        ]['cc_warehouse_cumulative_sales_in_the_year'].values[0] == 70
