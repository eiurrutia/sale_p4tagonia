from sales import sns, norm, plt, stats, np, pd, ps, data_holder


def add_warehouse_is_metropolitan_zone(df_sales, df_warehouses):
    """
    Adds a new column to the Sales DataFrame with a column that show if
    the row data correspond to a sale in a warehouse located in RM.
    1 --> Sale in metropolitan zone, 0 --> Sale not in metropolitan zone
    Parameters:
        --------
        df_sales: DataFrame
            The input DataFrame with the sales data.
        df_warehouses: DataFrame
            The input DataFrame with the warehouses data. This dataframe
            follow the schema warehouse, is_metropolitan_zone
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            column is_metropolitan_zone
    """
    query = f'''
        SELECT df_s.*,
            COALESCE(df_w.is_metropolitan_zone, 0) AS is_metropolitan_zone
        FROM df_sales df_s
        LEFT JOIN df_warehouses df_w
            ON df_s.warehouse = df_w.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['is_metropolitan_zone'] = \
        result['is_metropolitan_zone'].astype('int64')
    return result


def add_warehouse_is_inside_mall(df_sales, df_warehouses):
    """
    Adds a new column to the Sales DataFrame with a column that show if
    the row data correspond to a sale in a warehouse located inside a mall.
    1 --> Sale inside a Mall, 0 --> Sale not inside a Mall
    Parameters:
        --------
        df_sales: DataFrame
            The input DataFrame with the sales data.
        df_warehouses: DataFrame
            The input DataFrame with the warehouses data. This dataframe
            follow the schema warehouse, is_inside_mall
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            column is_inside_mall
    """
    query = f'''
        SELECT df_s.*, COALESCE(df_w.is_inside_mall, 0) AS is_inside_mall
        FROM df_sales df_s
        LEFT JOIN df_warehouses df_w
            ON df_s.warehouse = df_w.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['is_inside_mall'] = result['is_inside_mall'].astype('int64')
    return result