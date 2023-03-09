from sales import sns, norm, plt, stats, np, pd, ps, data_holder


def add_sku_warehouse_last_xdays_sales(df, days):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the last 'days' in the same warehouse.

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
        SELECT a.*,
           COALESCE(SUM(b.quantity), 0) AS sku_warehouse_last_{days}days_sales
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND b.date >= DATE(a.date, '-{days} day')
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'sku_warehouse_last_{days}days_sales'] = \
        result[f'sku_warehouse_last_{days}days_sales'].astype('int64')
    return result


def add_y_sku_warehouse_next_xdays_sales(df, days):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs to sale
    in the next 'days' in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
        days: Int
            Number of the days that you want to look ahead.
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format y_sku_warehouse_next_{days}days_sales
    """
    query = f'''
        SELECT a.*,
           COALESCE(SUM(b.quantity), 0)
            AS y_sku_warehouse_next_{days}days_sales
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND b.date < DATE(a.date, '{days + 1} day')
            AND b.date > a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'y_sku_warehouse_next_{days}days_sales'] = \
        result[f'y_sku_warehouse_next_{days}days_sales'].astype('int64')
    return result


def add_sku_warehouse_last_xdays_mean_sales(df, days):
    """
    Adds a new column to the input DataFrame with the mean of the
    quantity of SKUs sold in the last 'days' in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
        days: Int
            Number of the days that you want to look back
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format sku_warehouse_last_{days}days_mean_sales
    """
    query = f'''
        SELECT a.*,
           COALESCE(ROUND(SUM(b.quantity)/{days}.0, 4), 0)
            AS sku_warehouse_last_{days}days_mean_sales
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND b.date >= DATE(a.date, '-{days} day')
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'sku_warehouse_last_{days}days_mean_sales'] = \
        result[f'sku_warehouse_last_{days}days_mean_sales'].astype(float)
    return result


def add_sku_historic_sales(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format sku_historic_sales
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_historic_sales
            FROM (
                SELECT sku, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY sku, date
            )
        )
        SELECT df.*,
            cumulative_sales.sku_historic_sales
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.sku = cumulative_sales.sku
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_historic_sales'] = \
        result['sku_historic_sales'].astype('int64')
    return result


def add_sku_historic_sales_same_day_of_the_week(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the same day of the week historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_historic_sales_same_day_of_the_week'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, strftime('%w', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_historic_sales_same_day_of_the_week
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_historic_sales_same_day_of_the_week'] = \
        result['sku_historic_sales_same_day_of_the_week'].astype('int64')
    return result


def add_sku_historic_sales_same_month(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the same month of the year historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_historic_sales_same_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, month
                ORDER BY year, month
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_historic_sales_same_month
            FROM (
                SELECT sku,
                    strftime('%m', date) AS month,
                    strftime('%Y', date) AS year,
                    SUM(quantity) AS quantity
                FROM df
                GROUP BY month, year, sku
            )
        )
        SELECT df.*,
            cumulative_sales.sku_historic_sales_same_month
        FROM df, cumulative_sales
        WHERE strftime('%m', df.date) = cumulative_sales.month
            AND strftime('%Y', df.date) = cumulative_sales.year
            AND df.sku = cumulative_sales.sku
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_historic_sales_same_month'] = \
        result['sku_historic_sales_same_month'].astype('int64')
    return result


def add_sku_warehouse_historic_sales(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in historically in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format sku_warehouse_historic_sales
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_warehouse_historic_sales
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_historic_sales'] = \
        result['sku_warehouse_historic_sales'].astype('int64')
    return result


def add_sku_warehouse_historic_sales_same_day_of_the_week(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the same day of the week in the previous same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_warehouse_historic_sales_same_day_of_the_week'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, strftime('%w', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_warehouse_historic_sales_same_day_of_the_week
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_historic_sales_same_day_of_the_week'] = \
        result['sku_warehouse_historic_sales_same_day_of_the_week'
               ].astype('int64')
    return result


def add_sku_warehouse_historic_sales_same_month(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the same month of the previous year in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_warehouse_historic_sales_same_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, month
                ORDER BY year, month
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_warehouse_historic_sales_same_month
            FROM (
                SELECT sku, warehouse,
                    strftime('%m', date) AS month,
                    strftime('%Y', date) AS year,
                    SUM(quantity) AS quantity
                FROM df
                GROUP BY month, year, sku, warehouse
            )
        )
        SELECT df.*,
            cumulative_sales.sku_warehouse_historic_sales_same_month
        FROM df, cumulative_sales
        WHERE strftime('%m', df.date) = cumulative_sales.month
            AND strftime('%Y', df.date) = cumulative_sales.year
            AND df.sku = cumulative_sales.sku
            AND df.warehouse = cumulative_sales.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_historic_sales_same_month'] = \
        result['sku_warehouse_historic_sales_same_month'].astype('int64')
    return result


def add_unit_price_information(df):
    """
    Adds a new column to the input DataFrame with the unit price of the SKU for
    in the same month of the year in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'unit_price'
    """
    query = f'''
        SELECT df.*,
           COALESCE(ROUND(df.sale_amount/df.quantity, 0), 0)
            AS unit_price
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['unit_price'] = result['unit_price'].astype('int64')
    return result


def add_sku_cumulative_sales_in_the_week(df):
    """
    Adds a new column to the input DataFrame with the cumulative week
    sales for the sku across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_cumulative_sales_in_the_week'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, strftime('%W-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_cumulative_sales_in_the_week
            FROM (
                SELECT sku, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY sku, date
            )
        )
        SELECT df.*,
            cumulative_sales.sku_cumulative_sales_in_the_week
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.sku = cumulative_sales.sku
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_cumulative_sales_in_the_week'] = \
        result['sku_cumulative_sales_in_the_week'].astype('int64')
    return result


def add_sku_cumulative_sales_in_the_month(df):
    """
    Adds a new column to the input DataFrame with the cumulative month
    sales for the sku across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_cumulative_sales_in_the_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, strftime('%m-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_cumulative_sales_in_the_month
            FROM (
                SELECT sku, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY sku, date
            )
        )
        SELECT df.*,
            cumulative_sales.sku_cumulative_sales_in_the_month
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.sku = cumulative_sales.sku
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_cumulative_sales_in_the_month'] = \
        result['sku_cumulative_sales_in_the_month'].astype('int64')
    return result


def add_sku_cumulative_sales_in_the_year(df):
    """
    Adds a new column to the input DataFrame with the cumulative year
    sales for the sku across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_cumulative_sales_in_the_year'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, strftime('%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_cumulative_sales_in_the_year
            FROM (
                SELECT sku, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY sku, date
            )
        )
        SELECT df.*,
            cumulative_sales.sku_cumulative_sales_in_the_year
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.sku = cumulative_sales.sku
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_cumulative_sales_in_the_year'] = \
        result['sku_cumulative_sales_in_the_year'].astype('int64')
    return result


def add_sku_warehouse_cumulative_sales_in_the_week(df):
    """
    Adds a new column to the input DataFrame with the cumulative week
    sales for the sku in the warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_warehouse_cumulative_sales_in_the_week'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, strftime('%W-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_warehouse_cumulative_sales_in_the_week
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_cumulative_sales_in_the_week'] = \
        result['sku_warehouse_cumulative_sales_in_the_week'].astype('int64')
    return result


def add_sku_warehouse_cumulative_sales_in_the_month(df):
    """
    Adds a new column to the input DataFrame with the cumulative month
    sales for the sku in the warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_warehouse_cumulative_sales_in_the_month'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, strftime('%m-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_warehouse_cumulative_sales_in_the_month
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_cumulative_sales_in_the_month'] = \
        result['sku_warehouse_cumulative_sales_in_the_month'
               ].astype('int64')
    return result


def add_sku_warehouse_cumulative_sales_in_the_year(df):
    """
    Adds a new column to the input DataFrame with the cumulative year
    sales for the sku in the warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'sku_warehouse_cumulative_sales_in_the_year'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, strftime('%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS sku_warehouse_cumulative_sales_in_the_year
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_cumulative_sales_in_the_year'] = \
        result['sku_warehouse_cumulative_sales_in_the_year'].astype('int64')
    return result
