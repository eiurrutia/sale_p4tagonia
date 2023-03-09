import pytest
import pandas as pd
from sales.data_holder import DataHolder
from sales.feature_engineering.date_related_features import *


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


def test_add_offer_day_information():
    data_sales = {
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
    data_campaigns = {
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
    df_sales = pd.DataFrame(data_sales)
    df_campaigns = pd.DataFrame(data_campaigns)
    # Convert the date column to datetime type
    df_sales['date'] = pd.to_datetime(df_sales['date'], format='%d-%m-%Y')
    df_campaigns['start_date'] = \
        pd.to_datetime(df_campaigns['start_date'], format='%d-%m-%Y')
    df_campaigns['end_date'] = \
        pd.to_datetime(df_campaigns['end_date'], format='%d-%m-%Y')

    df = add_offer_day_information(df_sales, df_campaigns)
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
