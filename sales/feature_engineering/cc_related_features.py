from sales import sns, norm, plt, stats, np, pd, ps, data_holder, psycopg2


def add_cc_warehouse_last_xdays_sales(conn, days):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_warehouse_last_{days}days_sales INTEGER DEFAULT 0;
    WITH last_cc_warehouse_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY cc, warehouse
            ORDER BY date
            RANGE BETWEEN
                INTERVAL '{days} DAY' PRECEDING
                AND INTERVAL '1 DAY' PRECEDING
        ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET cc_warehouse_last_{days}days_sales =
        lcws.calculated
    FROM last_cc_warehouse_sales lcws
    WHERE df_sale.cc = lcws.cc
        AND df_sale.warehouse = lcws.warehouse
        AND df_sale.date = lcws.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_y_cc_warehouse_next_xdays_sales(conn, days):
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
    ALTER TABLE df_sale
        ADD COLUMN y_cc_warehouse_next_{days}days_sales INTEGER DEFAULT 0;
    WITH next_cc_warehouse_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY cc, warehouse
            ORDER BY date
            RANGE BETWEEN
                 INTERVAL '1 DAY' FOLLOWING
                 AND INTERVAL '{days} DAY' FOLLOWING
        ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET y_cc_warehouse_next_{days}days_sales =
        ncws.calculated
    FROM next_cc_warehouse_sales ncws
    WHERE df_sale.cc = ncws.cc
        AND df_sale.warehouse = ncws.warehouse
        AND df_sale.date = ncws.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_warehouse_last_xdays_mean_sales(conn, days):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_warehouse_last_{days}days_mean_sales FLOAT DEFAULT 0.0;
    WITH last_cc_warehouse_mean_sales AS (
        SELECT *,
            COALESCE(ROUND(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse
                ORDER BY date
                RANGE BETWEEN
                    INTERVAL '{days} DAY' PRECEDING
                    AND INTERVAL '1 DAY' PRECEDING
            )/{days}.0, 4), 0) AS calculated
        FROM df_sale
        )
    UPDATE df_sale
    SET cc_warehouse_last_{days}days_mean_sales =
        lcwms.calculated
    FROM last_cc_warehouse_mean_sales lcwms
    WHERE df_sale.cc = lcwms.cc
        AND df_sale.warehouse = lcwms.warehouse
        AND df_sale.date = lcwms.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_historic_sales(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_historic_sales INTEGER DEFAULT 0;
    WITH cc_date_sales AS (
        SELECT cc, date, SUM(quantity) AS quantity
            FROM df_sale
            GROUP BY cc, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY cc
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM cc_date_sales
    )
    UPDATE df_sale
    SET cc_historic_sales = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_historic_sales_same_day_of_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_historic_sales_same_day_of_the_week INTEGER DEFAULT 0;
    WITH cc_historic_sales_same_day AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, EXTRACT(DOW FROM date)
                ORDER BY date
                RANGE BETWEEN UNBOUNDED PRECEDING
                    AND INTERVAL '1 DAY' PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET cc_historic_sales_same_day_of_the_week =
        chssd.calculated
    FROM cc_historic_sales_same_day chssd
    WHERE df_sale.cc = chssd.cc
        AND df_sale.warehouse = chssd.warehouse
        AND df_sale.date = chssd.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_historic_sales_same_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_historic_sales_same_month INTEGER DEFAULT 0;
    WITH cc_month_year_sales AS (
        SELECT cc,
            EXTRACT(MONTH FROM date) AS month,
            EXTRACT(YEAR FROM date) AS year,
            SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY month, year, cc
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY cc, month
            ORDER BY year, month
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM cc_month_year_sales
    )
    UPDATE df_sale
    SET cc_historic_sales_same_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND EXTRACT(MONTH FROM date) = cs.month
        AND EXTRACT(YEAR FROM date) = cs.year
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_warehouse_historic_sales(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_warehouse_historic_sales INTEGER DEFAULT 0;
    WITH cc_warehouse_historic_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET cc_warehouse_historic_sales = cwhs.calculated
    FROM cc_warehouse_historic_sales cwhs
    WHERE df_sale.cc = cwhs.cc
        AND df_sale.warehouse = cwhs.warehouse
        AND df_sale.date = cwhs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_warehouse_historic_sales_same_day_of_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_warehouse_historic_sales_same_day_of_the_week
        INTEGER DEFAULT 0;
    WITH cc_warehouse_historic_sales_same_day AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, EXTRACT(DOW FROM date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET cc_warehouse_historic_sales_same_day_of_the_week = cwhssd.calculated
    FROM cc_warehouse_historic_sales_same_day cwhssd
    WHERE df_sale.cc = cwhssd.cc
        AND df_sale.warehouse = cwhssd.warehouse
        AND df_sale.date = cwhssd.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_warehouse_historic_sales_same_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_warehouse_historic_sales_same_month INTEGER DEFAULT 0;
    WITH cc_warehouse_historic_sales_same_month AS (
        SELECT cc, warehouse,
            EXTRACT(MONTH FROM date) AS month,
            EXTRACT(YEAR FROM date) AS year,
            SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY month, year, cc, warehouse
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY cc, warehouse, month
            ORDER BY year, month
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM cc_warehouse_historic_sales_same_month
    )
    UPDATE df_sale
    SET cc_warehouse_historic_sales_same_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND df_sale.warehouse = cs.warehouse
        AND EXTRACT(MONTH FROM date) = cs.month
        AND EXTRACT(YEAR FROM date) = cs.year
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_cumulative_sales_in_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_cumulative_sales_in_the_week INTEGER DEFAULT 0;
    WITH cc_date_sales AS (
        SELECT cc, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY cc, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY cc, to_char(date, 'IW-IYYY')
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM cc_date_sales
    )
    UPDATE df_sale
    SET cc_cumulative_sales_in_the_week = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_cumulative_sales_in_the_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_cumulative_sales_in_the_month INTEGER DEFAULT 0;
    WITH cc_date_sales AS (
        SELECT cc, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY cc, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY cc, to_char(date, 'MM-YYYY')
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM cc_date_sales
    )
    UPDATE df_sale
    SET cc_cumulative_sales_in_the_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_cumulative_sales_in_the_year(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_cumulative_sales_in_the_year INTEGER DEFAULT 0;
    WITH cc_year_sales AS (
        SELECT cc, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY cc, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY cc, EXTRACT(YEAR FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM cc_year_sales
    )
    UPDATE df_sale
    SET cc_cumulative_sales_in_the_year = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_warehouse_cumulative_sales_in_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_warehouse_cumulative_sales_in_the_week
        INTEGER DEFAULT 0;
    WITH cumulative_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, to_char(date, 'IW-IYYY')
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET cc_warehouse_cumulative_sales_in_the_week = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_warehouse_cumulative_sales_in_the_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_warehouse_cumulative_sales_in_the_month
        INTEGER DEFAULT 0;
    WITH cumulative_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, to_char(date, 'MM-YYYY')
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET cc_warehouse_cumulative_sales_in_the_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_cc_warehouse_cumulative_sales_in_the_year(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN cc_warehouse_cumulative_sales_in_the_year
        INTEGER DEFAULT 0;
    WITH cumulative_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY cc, warehouse, EXTRACT(YEAR FROM date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET cc_warehouse_cumulative_sales_in_the_year = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.cc = cs.cc
        AND df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
