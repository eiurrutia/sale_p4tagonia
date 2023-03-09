from sales import sns, norm, plt, stats, np, pd, ps, data_holder


def add_company_last_xdays_sales(df, days):
    """
    Adds a new column to the input DataFrame with the quantity of products
    sold in the last 'days' accross the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
        days: Int
            Number of the days that you want to look back
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format company_last_{days}days_sales
    """
    query = f'''
        WITH
        date_sale AS (
            SELECT date, SUM(quantity) AS quantity
            FROM df
            GROUP BY date
        ),
        company_date_last_days AS (
            SELECT a.date,
                COALESCE(SUM(b.quantity), 0) AS company_last_{days}days_sales
            FROM date_sale a
            LEFT JOIN date_sale b
                ON b.date >= DATE(a.date, '-{days} day')
                AND b.date < a.date
            GROUP BY a.date
        )
        SELECT df.*,
            company_date_last_days.company_last_{days}days_sales
                AS company_last_{days}days_sales
        FROM df, company_date_last_days
        WHERE df.date = company_date_last_days.date
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'company_last_{days}days_sales'] = \
        result[f'company_last_{days}days_sales'].astype('int64')
    return result


def add_company_last_xdays_mean_sales(df, days):
    """
    Adds a new column to the input DataFrame with the mean of products
    sold in the last 'days' accross the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
        days: Int
            Number of the days that you want to look back
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format company_last_{days}days_mean_sales
    """
    query = f'''
        WITH
        date_sale AS (
            SELECT date, SUM(quantity) AS quantity
            FROM df
            GROUP BY date
        ),
        company_date_last_days AS (
            SELECT a.date,
                COALESCE(SUM(b.quantity), 0) AS company_last_{days}days_sales
            FROM date_sale a
            LEFT JOIN date_sale b
                ON b.date >= DATE(a.date, '-{days} day')
                AND b.date < a.date
            GROUP BY a.date
        )
        SELECT df.*,
            COALESCE(
                ROUND(
                    company_date_last_days.company_last_{days}days_sales
                    / {days}.0,
                4),
            0) AS company_last_{days}days_mean_sales
        FROM df
        LEFT JOIN company_date_last_days
            ON df.date = company_date_last_days.date
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result[f'company_last_{days}days_mean_sales'] = \
        result[f'company_last_{days}days_mean_sales'].astype(float)
    return result


def add_company_historic_sales(df):
    """
    Adds a new column to the input DataFrame with the quantity of products sold
    in historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format company_historic_sales
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS company_historic_sales
            FROM (
                SELECT date, SUM(quantity) AS quantity
                FROM df
                GROUP BY date
            )
        )
        SELECT df.*,
            cumulative_sales.company_historic_sales
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['company_historic_sales'] = \
        result['company_historic_sales'].astype('int64')
    return result


def add_company_historic_sales_same_day_of_the_week(df):
    """
    Adds a new column to the input DataFrame with the quantity of products
    sold in the same day of the week historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'company_historic_sales_same_day_of_the_week'
    """
    query = f'''
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY strftime('%w', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS company_historic_sales_same_day_of_the_week
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['company_historic_sales_same_day_of_the_week'] = \
        result['company_historic_sales_same_day_of_the_week'].astype('int64')
    return result


def add_company_historic_sales_same_month(df):
    """
    Adds a new column to the input DataFrame with the quantity of products
    sold in the same month of the year historically in the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'company_historic_sales_same_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY month
                ORDER BY year, month
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS company_historic_sales_same_month
            FROM (
                SELECT
                    strftime('%m', date) AS month,
                    strftime('%Y', date) AS year,
                    SUM(quantity) AS quantity
                FROM df
                GROUP BY month, year
            )
        )
        SELECT df.*,
            cumulative_sales.company_historic_sales_same_month
        FROM df, cumulative_sales
        WHERE strftime('%m', df.date) = cumulative_sales.month
            AND strftime('%Y', df.date) = cumulative_sales.year
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['company_historic_sales_same_month'] = \
        result['company_historic_sales_same_month'].astype('int64')
    return result


def add_company_cumulative_sales_in_the_week(df):
    """
    Adds a new column to the input DataFrame with the cumulative week
    sales for the all products across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'company_cumulative_sales_in_the_week'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY strftime('%W-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS company_cumulative_sales_in_the_week
            FROM (
                SELECT date, SUM(quantity) AS quantity
                FROM df
                GROUP BY date
            )
        )
        SELECT df.*,
            cumulative_sales.company_cumulative_sales_in_the_week
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['company_cumulative_sales_in_the_week'] = \
        result['company_cumulative_sales_in_the_week'].astype('int64')
    return result


def add_company_cumulative_sales_in_the_month(df):
    """
    Adds a new column to the input DataFrame with the cumulative month
    sales for the all products across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'company_cumulative_sales_in_the_month'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY strftime('%m-%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS company_cumulative_sales_in_the_month
            FROM (
                SELECT date, SUM(quantity) AS quantity
                FROM df
                GROUP BY date
            )
        )
        SELECT df.*,
            cumulative_sales.company_cumulative_sales_in_the_month
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['company_cumulative_sales_in_the_month'] = \
        result['company_cumulative_sales_in_the_month'].astype('int64')
    return result


def add_company_cumulative_sales_in_the_year(df):
    """
    Adds a new column to the input DataFrame with the cumulative year
    sales for the all products across the company.

    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            name 'company_cumulative_sales_in_the_year'
    """
    query = f'''
        WITH cumulative_sales AS (
            SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY strftime('%Y', date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS company_cumulative_sales_in_the_year
            FROM (
                SELECT date, SUM(quantity) AS quantity
                FROM df
                GROUP BY date
            )
        )
        SELECT df.*,
            cumulative_sales.company_cumulative_sales_in_the_year
        FROM df, cumulative_sales
        WHERE df.date = cumulative_sales.date
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['company_cumulative_sales_in_the_year'] = \
        result['company_cumulative_sales_in_the_year'].astype('int64')
    return result
