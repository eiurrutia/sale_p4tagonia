from sales import sns, norm, plt, stats, np, pd, ps, data_holder


def add_sku_warehouse_sales_last_xdays_sales(df, days):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the last week in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
        days: Int
            Number of the days that you want to look back
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format sku_warehouse_last_{days}days_sales
    """
    query = f'''
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0) AS sku_warehouse_last_{days}days_sales
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND b.date >= DATE(a.date, '-{7} day')
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'sku_warehouse_last_{days}days_sales'] = \
        result[f'sku_warehouse_last_{days}days_sales'].astype(int)
    return result


def add_sku_warehouse_historic_sales_same_day_of_the_week(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the last week in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format sku_warehouse_last_{days}days_sales
    """
    query = f'''
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_warehouse_historic_sales_same_day_of_the_week
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND strftime('%w', b.date) = strftime('%w', a.date)
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_historic_sales_same_day_of_the_week'] = \
        result['sku_warehouse_historic_sales_same_day_of_the_week'].astype(int)
    return result
