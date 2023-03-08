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


def add_warehouse_cumulative_sales_in_the_week(df):
    """
    Adds a new column to the input DataFrame with the cumulative week
    sales for the warehouse considering all skus.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'warehouse_cumulative_sales_in_the_week'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY warehouse, strftime('%W-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS warehouse_cumulative_sales_in_the_week
            FROM (
                SELECT warehouse, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY warehouse, date
            )
        )
        SELECT df.*,
            cumulative_sales.warehouse_cumulative_sales_in_the_week
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.warehouse = cumulative_sales.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['warehouse_cumulative_sales_in_the_week'] = \
        result['warehouse_cumulative_sales_in_the_week'].astype('int64')
    return result


def add_warehouse_cumulative_sales_in_the_month(df):
    """
    Adds a new column to the input DataFrame with the cumulative month
    sales for the warehouse considering all skus.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'warehouse_cumulative_sales_in_the_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY warehouse, strftime('%m-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS warehouse_cumulative_sales_in_the_month
            FROM (
                SELECT warehouse, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY warehouse, date
            )
        )
        SELECT df.*,
            cumulative_sales.warehouse_cumulative_sales_in_the_month
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.warehouse = cumulative_sales.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['warehouse_cumulative_sales_in_the_month'] = \
        result['warehouse_cumulative_sales_in_the_month'].astype('int64')
    return result


def add_warehouse_cumulative_sales_in_the_year(df):
    """
    Adds a new column to the input DataFrame with the cumulative year
    sales for the warehouse considering all skus.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'warehouse_cumulative_sales_in_the_year'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY warehouse, strftime('%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS warehouse_cumulative_sales_in_the_year
            FROM (
                SELECT warehouse, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY warehouse, date
            )
        )
        SELECT df.*,
            cumulative_sales.warehouse_cumulative_sales_in_the_year
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.warehouse = cumulative_sales.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['warehouse_cumulative_sales_in_the_year'] = \
        result['warehouse_cumulative_sales_in_the_year'].astype('int64')
    return result