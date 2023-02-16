import pytest
import pandas as pd
from sales.data_holder import DataHolder


@pytest.fixture(autouse=True)
def set_up():
    """Create df with sample data"""
    data = {'sku': ["1", "2", "3", "4"],
            'quantity': [5, 6, 7, 8],
            'date': [
                '08-02-2023', '06-02-2023',
                '07-02-2023', '08-02-2023'
            ]}
    df = pd.DataFrame(data)
    global data_holder
    data_holder = DataHolder()
    data_holder.set_data(df)


def test_remove_rows_by_values():
    data_holder.remove_rows_by_values(
        ['sku', 'date'],
        ['1', '06-02-2023']
    )
    assert data_holder.get_data()['sku'].tolist() == ['3', '4']
    assert data_holder.get_data()['date'].tolist() == [
        '07-02-2023', '08-02-2023']


def test_remove_columns_by_features():
    data_holder.remove_columns_by_features(['sku', 'date'])
    assert data_holder.get_data().columns.to_list() == ['sku', 'date']


def test_group_data():
    df = pd.DataFrame({
        'sku': ["1", "1", "1", "1", "2"],
        'quantity1': [5, 6, 7, 8, 2],
        'quantity2': [5, 6, 7, 8, 2],
        'date': [
            '06-02-2023', '06-02-2023',
            '07-02-2023', '08-02-2023',
            '06-02-2023']
    })
    data_holder.set_data(df)
    data_holder.group_data(['sku'], {'quantity1': 'max', 'quantity2': 'sum'})
    print(data_holder.get_data())
    assert data_holder.get_data()['sku'].tolist() == ['1', '2']
    assert data_holder.get_data()['quantity1'].tolist() == [8, 2]
    assert data_holder.get_data()['quantity2'].tolist() == [26, 2]


def test_delete_duplicates():
    df = pd.DataFrame({
        'sku': ["1", "1", "1", "1", "2"],
        'quantity': [5, 5, 5, 8, 2],
        'date': [
            '06-02-2023', '06-02-2023',
            '06-02-2023', '08-02-2023',
            '06-02-2023']
    })
    data_holder.set_data(df)
    data_holder.delete_duplicates()
    assert data_holder.get_data()['sku'].tolist() == ['1', '1', '2']
    assert data_holder.get_data()['date'].tolist() == [
        '06-02-2023', '08-02-2023', '06-02-2023']
    assert data_holder.get_data()['quantity'].tolist() == [5, 8, 2]


def test_rename_columns():
    df = pd.DataFrame({
        'SKU': ["1", "1", "1", "1", "2"],
        'cantidad': [5, 5, 5, 8, 2],
        'fecha': [
            '06-02-2023', '06-02-2023',
            '06-02-2023', '08-02-2023',
            '06-02-2023']
    })
    data_holder.set_data(df)
    data_holder.rename_columns({
        'SKU': 'sku',
        'fecha': 'date',
        'cantidad': 'quantity',
    })
    assert data_holder.get_data().columns.tolist() == [
        'sku', 'quantity', 'date']
