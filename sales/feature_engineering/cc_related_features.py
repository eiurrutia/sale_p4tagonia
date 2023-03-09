from sales import sns, norm, plt, stats, np, pd, ps, data_holder


def add_cc_warehouse_last_xdays_sales(df, days):
    """
    Adds a new column to the input DataFrame with the quantity of CCs sold
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
            format cc_warehouse_last_{days}days_sales
    """
    query = f'''
        SELECT a.*,
           COALESCE(SUM(b.quantity), 0) AS cc_warehouse_last_{days}days_sales
        FROM df a
        LEFT JOIN df b
            ON a.cc = b.cc
            AND a.warehouse = b.warehouse
            AND b.date >= DATE(a.date, '-{days} day')
            AND b.date < a.date
        GROUP BY a.date, a.cc, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'cc_warehouse_last_{days}days_sales'] = \
        result[f'cc_warehouse_last_{days}days_sales'].astype('int64')
    return result


def add_y_cc_warehouse_next_xdays_sales(df, days):
    """
    Adds a new column to the input DataFrame with the quantity of CCs to sale
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
            format y_cc_warehouse_next_{days}days_sales
    """
    query = f'''
        SELECT a.*,
           COALESCE(SUM(b.quantity), 0)
            AS y_cc_warehouse_next_{days}days_sales
        FROM df a
        LEFT JOIN df b
            ON a.cc = b.cc
            AND a.warehouse = b.warehouse
            AND b.date < DATE(a.date, '{days + 1} day')
            AND b.date > a.date
        GROUP BY a.date, a.cc, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'y_cc_warehouse_next_{days}days_sales'] = \
        result[f'y_cc_warehouse_next_{days}days_sales'].astype('int64')
    return result


def add_cc_warehouse_last_xdays_mean_sales(df, days):
    """
    Adds a new column to the input DataFrame with the mean of the
    quantity of CCs sold in the last 'days' in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
        days: Int
            Number of the days that you want to look back
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format cc_warehouse_last_{days}days_mean_sales
    """
    query = f'''
        SELECT a.*,
           COALESCE(ROUND(SUM(b.quantity)/{days}.0, 4), 0)
            AS cc_warehouse_last_{days}days_mean_sales
        FROM df a
        LEFT JOIN df b
            ON a.cc = b.cc
            AND a.warehouse = b.warehouse
            AND b.date >= DATE(a.date, '-{days} day')
            AND b.date < a.date
        GROUP BY a.date, a.cc, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'cc_warehouse_last_{days}days_mean_sales'] = \
        result[f'cc_warehouse_last_{days}days_mean_sales'].astype(float)
    return result


def add_cc_historic_sales(df):
    """
    Adds a new column to the input DataFrame with the quantity of CCs sold
    in historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format cc_historic_sales
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_historic_sales
            FROM (
                SELECT cc, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY cc, date
            )
        )
        SELECT df.*,
            cumulative_sales.cc_historic_sales
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.cc = cumulative_sales.cc
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_historic_sales'] = \
        result['cc_historic_sales'].astype('int64')
    return result


def add_cc_historic_sales_same_day_of_the_week(df):
    """
    Adds a new column to the input DataFrame with the quantity of CCs sold
    in the same day of the week historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_historic_sales_same_day_of_the_week'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, strftime('%w', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_historic_sales_same_day_of_the_week
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_historic_sales_same_day_of_the_week'] = \
        result['cc_historic_sales_same_day_of_the_week'].astype('int64')
    return result


def add_cc_historic_sales_same_month(df):
    """
    Adds a new column to the input DataFrame with the quantity of CCs sold
    in the same month of the year historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_historic_sales_same_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, month
                ORDER BY year, month
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_historic_sales_same_month
            FROM (
                SELECT cc,
                    strftime('%m', date) AS month,
                    strftime('%Y', date) AS year,
                    SUM(quantity) AS quantity
                FROM df
                GROUP BY month, year, cc
            )
        )
        SELECT df.*,
            cumulative_sales.cc_historic_sales_same_month
        FROM df, cumulative_sales
        WHERE strftime('%m', df.date) = cumulative_sales.month
            AND strftime('%Y', df.date) = cumulative_sales.year
            AND df.cc = cumulative_sales.cc
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_historic_sales_same_month'] = \
        result['cc_historic_sales_same_month'].astype('int64')
    return result


def add_cc_warehouse_historic_sales(df):
    """
    Adds a new column to the input DataFrame with the quantity of CCs sold
    in historically in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format cc_warehouse_historic_sales
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_warehouse_historic_sales
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_warehouse_historic_sales'] = \
        result['cc_warehouse_historic_sales'].astype('int64')
    return result


def add_cc_warehouse_historic_sales_same_day_of_the_week(df):
    """
    Adds a new column to the input DataFrame with the quantity of CCs sold
    in the same day of the week in the previous same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_warehouse_historic_sales_same_day_of_the_week'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, strftime('%w', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_warehouse_historic_sales_same_day_of_the_week
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_warehouse_historic_sales_same_day_of_the_week'] = \
        result['cc_warehouse_historic_sales_same_day_of_the_week'
               ].astype('int64')
    return result


def add_cc_warehouse_historic_sales_same_month(df):
    """
    Adds a new column to the input DataFrame with the quantity of CCs sold
    in the same month of the previous year in the same warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_warehouse_historic_sales_same_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, month
                ORDER BY year, month
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_warehouse_historic_sales_same_month
            FROM (
                SELECT cc, warehouse,
                    strftime('%m', date) AS month,
                    strftime('%Y', date) AS year,
                    SUM(quantity) AS quantity
                FROM df
                GROUP BY month, year, cc, warehouse
            )
        )
        SELECT df.*,
            cumulative_sales.cc_warehouse_historic_sales_same_month
        FROM df, cumulative_sales
        WHERE strftime('%m', df.date) = cumulative_sales.month
            AND strftime('%Y', df.date) = cumulative_sales.year
            AND df.cc = cumulative_sales.cc
            AND df.warehouse = cumulative_sales.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_warehouse_historic_sales_same_month'] = \
        result['cc_warehouse_historic_sales_same_month'].astype('int64')
    return result


def add_cc_cumulative_sales_in_the_week(df):
    """
    Adds a new column to the input DataFrame with the cumulative week
    sales for the cc across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_cumulative_sales_in_the_week'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, strftime('%W-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_cumulative_sales_in_the_week
            FROM (
                SELECT cc, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY cc, date
            )
        )
        SELECT df.*,
            cumulative_sales.cc_cumulative_sales_in_the_week
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.cc = cumulative_sales.cc
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_cumulative_sales_in_the_week'] = \
        result['cc_cumulative_sales_in_the_week'].astype('int64')
    return result


def add_cc_cumulative_sales_in_the_month(df):
    """
    Adds a new column to the input DataFrame with the cumulative month
    sales for the cc across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_cumulative_sales_in_the_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, strftime('%m-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_cumulative_sales_in_the_month
            FROM (
                SELECT cc, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY cc, date
            )
        )
        SELECT df.*,
            cumulative_sales.cc_cumulative_sales_in_the_month
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.cc = cumulative_sales.cc
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_cumulative_sales_in_the_month'] = \
        result['cc_cumulative_sales_in_the_month'].astype('int64')
    return result


def add_cc_cumulative_sales_in_the_year(df):
    """
    Adds a new column to the input DataFrame with the cumulative year
    sales for the cc across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_cumulative_sales_in_the_year'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, strftime('%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_cumulative_sales_in_the_year
            FROM (
                SELECT cc, date, SUM(quantity) AS quantity
                FROM df
                GROUP BY cc, date
            )
        )
        SELECT df.*,
            cumulative_sales.cc_cumulative_sales_in_the_year
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
            AND df.cc = cumulative_sales.cc
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_cumulative_sales_in_the_year'] = \
        result['cc_cumulative_sales_in_the_year'].astype('int64')
    return result


def add_cc_warehouse_cumulative_sales_in_the_week(df):
    """
    Adds a new column to the input DataFrame with the cumulative week
    sales for the cc in the warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_warehouse_cumulative_sales_in_the_week'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, strftime('%W-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_warehouse_cumulative_sales_in_the_week
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_warehouse_cumulative_sales_in_the_week'] = \
        result['cc_warehouse_cumulative_sales_in_the_week'].astype('int64')
    return result


def add_cc_warehouse_cumulative_sales_in_the_month(df):
    """
    Adds a new column to the input DataFrame with the cumulative month
    sales for the cc in the warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_warehouse_cumulative_sales_in_the_month'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, strftime('%m-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_warehouse_cumulative_sales_in_the_month
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_warehouse_cumulative_sales_in_the_month'] = \
        result['cc_warehouse_cumulative_sales_in_the_month'
               ].astype('int64')
    return result


def add_cc_warehouse_cumulative_sales_in_the_year(df):
    """
    Adds a new column to the input DataFrame with the cumulative year
    sales for the cc in the warehouse.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'cc_warehouse_cumulative_sales_in_the_year'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, strftime('%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS cc_warehouse_cumulative_sales_in_the_year
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['cc_warehouse_cumulative_sales_in_the_year'] = \
        result['cc_warehouse_cumulative_sales_in_the_year'].astype('int64')
    return result
