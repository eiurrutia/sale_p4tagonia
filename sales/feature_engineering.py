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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(ROUND(AVG(b.quantity), 2), 0)
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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_historic_sales
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_historic_sales'] = \
        result['sku_historic_sales'].astype(int)
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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_warehouse_historic_sales
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_historic_sales'] = \
        result['sku_warehouse_historic_sales'].astype(int)
    return result


def add_sku_warehouse_historic_sales_same_day_of_the_week(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the same day of the week in the same warehouse.

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


def add_sku_warehouse_historic_sales_same_month(df):
    """
    Adds a new column to the input DataFrame with the quantity of SKUs sold
    in the same month of the year in the same warehouse.

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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_warehouse_historic_sales_same_month
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND strftime('%m', b.date) = strftime('%m', a.date)
            AND strftime('%Y', b.date) < strftime('%Y', a.date)
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_historic_sales_same_month'] = \
        result['sku_warehouse_historic_sales_same_month'].astype(int)
    return result


def add_weekday_information(df):
    """
    Adds a new column to the input DataFrame with a column that explain which
    day of the week correspon the date:
        0 --> Sunday, 1 --> Monday, 6 --> Saturday
    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format weekday
    """
    query = f'''
        SELECT date, sku, warehouse, quantity,
            strftime('%w', date) AS weekday
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['weekday'] = result['weekday'].astype(int)
    return result


def add_month_information(df):
    """
    Adds a new column to the input DataFrame with a column that explain which
    month of the week correspond the date:
        1 --> Jaunary, 3 --> March, 9 --> September, 12 --> December

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format month in number
    """
    query = f'''
        SELECT date, sku, warehouse, quantity,
            strftime('%m', date) AS month
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['month'] = result['month'].astype(int)
    return result


def add_year_information(df):
    """
    Adds a new column to the input DataFrame with a column that explain which
    month of the week correspond the date: 2018, 2019, ..., 2023

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format month in number
    """
    query = f'''
        SELECT date, sku, warehouse, quantity,
            strftime('%Y', date) AS year
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['year'] = result['year'].astype(int)
    return result


def add_offer_day_information(df_sales, df_offer_campaigns):
    """
    Adds a new column to the Sales DataFrame with a column that show if
    the row data correspond to a sale in offer campaigns period.
    1 --> Sale during offer compaign, 0 --> Sale in normal days
    Parameters:
        --------
        df_sales: DataFrame
            The input DataFrame with the sales data.
        df_offer_campaigns: DataFrame
            The input DataFrame with the offer campaigns data. This dataframe
            follow the schema start_date, end_date, campaign_name
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            column is_offer_day
    """
    query = f'''
        SELECT df_s.*,
            CASE WHEN df_oc.start_date <= df_s.date
                    AND df_s.date <= df_oc.end_date
                THEN 1
                ELSE 0
            END AS is_offer_day
        FROM df_sales df_s
        LEFT JOIN df_offer_campaigns df_oc
            ON df_s.date BETWEEN dF_oc.start_date AND df_oc.end_date
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['is_offer_day'] = result['is_offer_day'].astype(int)
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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_cumulative_sales_in_the_week
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND strftime('%W-%Y', b.date) = strftime('%W-%Y', a.date)
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_cumulative_sales_in_the_week'] = \
        result['sku_cumulative_sales_in_the_week'].astype(int)
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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_cumulative_sales_in_the_month
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND strftime('%m-%Y', b.date) = strftime('%m-%Y', a.date)
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_cumulative_sales_in_the_month'] = \
        result['sku_cumulative_sales_in_the_month'].astype(int)
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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_cumulative_sales_in_the_year
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND strftime('%Y', b.date) = strftime('%Y', a.date)
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_cumulative_sales_in_the_year'] = \
        result['sku_cumulative_sales_in_the_year'].astype(int)
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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_warehouse_cumulative_sales_in_the_week
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND strftime('%W-%Y', b.date) = strftime('%W-%Y', a.date)
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_cumulative_sales_in_the_week'] = \
        result['sku_warehouse_cumulative_sales_in_the_week'].astype(int)
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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_warehouse_cumulative_sales_in_the_month
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND strftime('%m-%Y', b.date) = strftime('%m-%Y', a.date)
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_cumulative_sales_in_the_month'] = \
        result['sku_warehouse_cumulative_sales_in_the_month'].astype(int)
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
        SELECT a.date, a.sku, a.warehouse, a.quantity,
           COALESCE(SUM(b.quantity), 0)
            AS sku_warehouse_cumulative_sales_in_the_year
        FROM df a
        LEFT JOIN df b
            ON a.sku = b.sku
            AND a.warehouse = b.warehouse
            AND strftime('%Y', b.date) = strftime('%Y', a.date)
            AND b.date < a.date
        GROUP BY a.date, a.sku, a.warehouse
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['sku_warehouse_cumulative_sales_in_the_year'] = \
        result['sku_warehouse_cumulative_sales_in_the_year'].astype(int)
    return result


