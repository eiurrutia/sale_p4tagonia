from sales import sns, norm, plt, stats, np, pd, ps, data_holder, psycopg2


def add_company_last_xdays_sales(conn, days):
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
    ALTER TABLE df_sale
        ADD COLUMN company_last_{days}days_sales INTEGER DEFAULT 0;
    WITH date_sale AS (
        SELECT date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date
    ),
    company_date_last_days AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            ORDER BY date
            RANGE BETWEEN
                INTERVAL '{days} DAY' PRECEDING
                AND INTERVAL '1 DAY' PRECEDING
        ), 0) AS calculated
        FROM date_sale
    )
    UPDATE df_sale
    SET company_last_{days}days_sales =
        cdld.calculated
    FROM company_date_last_days cdld
    WHERE df_sale.date = cdld.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_company_last_xdays_mean_sales(conn, days):
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
    ALTER TABLE df_sale
        ADD COLUMN company_last_{days}days_mean_sales FLOAT DEFAULT 0.0;
    WITH date_sale AS (
        SELECT date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date
    ),
    company_date_last_days AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            ORDER BY date
            RANGE BETWEEN
                INTERVAL '{days} DAY' PRECEDING
                AND INTERVAL '1 DAY' PRECEDING
        ), 0) AS calculated
        FROM date_sale
    )
    UPDATE df_sale
    SET company_last_{days}days_mean_sales =
        COALESCE(ROUND(cdld.calculated/{days}.0, 4), 0)
    FROM company_date_last_days cdld
    WHERE df_sale.date = cdld.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_company_historic_sales(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN company_historic_sales INTEGER DEFAULT 0;
    WITH date_sale AS (
        SELECT date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM date_sale
    )
    UPDATE df_sale
    SET company_historic_sales =
        cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_company_historic_sales_same_day_of_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN company_historic_sales_same_day_of_the_week
            INTEGER DEFAULT 0;
    WITH date_sale AS (
        SELECT date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date
    ),
    cumulative_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY EXTRACT(DOW FROM date)
                ORDER BY date
                RANGE BETWEEN UNBOUNDED PRECEDING
                    AND INTERVAL '1 DAY' PRECEDING
            ), 0) AS calculated
        FROM date_sale
    )
    UPDATE df_sale
    SET company_historic_sales_same_day_of_the_week = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_company_historic_sales_same_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN company_historic_sales_same_month INTEGER DEFAULT 0;
    WITH month_year_sales AS (
        SELECT EXTRACT(MONTH FROM date) AS month,
            EXTRACT(YEAR FROM date) AS year,
            SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY EXTRACT(MONTH FROM date), EXTRACT(YEAR FROM date)
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY month
            ORDER BY year, month
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM month_year_sales
    )
    UPDATE df_sale
    SET company_historic_sales_same_month = cs.calculated
    FROM cumulative_sales cs
    WHERE EXTRACT(MONTH FROM date) = cs.month
        AND EXTRACT(YEAR FROM date) = cs.year
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_company_cumulative_sales_in_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN company_cumulative_sales_in_the_week INTEGER DEFAULT 0;
    WITH date_sales AS (
        SELECT date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY to_char(date, 'IW-IYYY')
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM date_sales
    )
    UPDATE df_sale
    SET company_cumulative_sales_in_the_week = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_company_cumulative_sales_in_the_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN company_cumulative_sales_in_the_month INTEGER DEFAULT 0;
    WITH date_sales AS (
        SELECT date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY to_char(date, 'MM-YYYY')
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM date_sales
    )
    UPDATE df_sale
    SET company_cumulative_sales_in_the_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_company_cumulative_sales_in_the_year(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN company_cumulative_sales_in_the_year INTEGER DEFAULT 0;
    WITH year_sales AS (
        SELECT date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY EXTRACT(YEAR FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM year_sales
    )
    UPDATE df_sale
    SET company_cumulative_sales_in_the_year = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
