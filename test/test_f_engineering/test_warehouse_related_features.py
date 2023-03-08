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
