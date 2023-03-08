import pytest
import pandas as pd
from sales.data_holder import DataHolder
from sales.f_engineering.warehouse_related_features import *


def test_add_warehouse_is_metropolitan_zone():
    data_sales = {
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
    data_warehouses = {
                'warehouse': [
                    'COYHAIQUE', 'LADEHESA',
                    'MALLSPORT', 'CONCEPCION',
                    'LASCONDES', 'PTOVARAS'
                ],
                'is_metropolitan_zone': [0, 1, 1, 0, 1, 0]
                }
    df_sales = pd.DataFrame(data_sales)
    df_warehouses = pd.DataFrame(data_warehouses)
    # Convert the date column to datetime type
    df_sales['date'] = pd.to_datetime(df_sales['date'], format='%d-%m-%Y')
    df = add_warehouse_is_metropolitan_zone(df_sales, df_warehouses)
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


def test_add_warehouse_is_inside_mall():
    data_sales = {
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
    data_warehouses = {
                'warehouse': [
                    'COYHAIQUE', 'LADEHESA',
                    'MALLSPORT', 'CONCEPCION',
                    'OTRAUBICACION', 'PTOVARAS'
                ],
                'is_inside_mall': [0, 1, 1, 1, 1, 0]
                }
    df_sales = pd.DataFrame(data_sales)
    df_warehouses = pd.DataFrame(data_warehouses)
    # Convert the date column to datetime type
    df_sales['date'] = pd.to_datetime(df_sales['date'], format='%d-%m-%Y')
    df = add_warehouse_is_inside_mall(df_sales, df_warehouses)
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


def test_add_warehouse_cumulative_sales_in_the_week():
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
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_warehouse_cumulative_sales_in_the_week(df)
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


def test_add_warehouse_cumulative_sales_in_the_month():
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
    df = pd.DataFrame(data)
    # Convert the date column to datetime type
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

    df = add_warehouse_cumulative_sales_in_the_month(df)
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
