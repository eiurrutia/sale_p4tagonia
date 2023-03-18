from sales import sns, norm, plt, stats, np, pd, ps, data_holder, psycopg2


def add_sku_warehouse_last_xdays_sales(conn, days):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_warehouse_last_{days}days_sales INTEGER DEFAULT 0;
    WITH last_sku_warehouse_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY sku, warehouse
            ORDER BY date
            RANGE BETWEEN
                INTERVAL '{days} DAY' PRECEDING
                AND INTERVAL '1 DAY' PRECEDING
        ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET sku_warehouse_last_{days}days_sales =
        lsws.calculated
    FROM last_sku_warehouse_sales lsws
    WHERE df_sale.sku = lsws.sku
        AND df_sale.warehouse = lsws.warehouse
        AND df_sale.date = lsws.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_y_sku_warehouse_next_xdays_sales(conn, days):
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
    ALTER TABLE df_sale
        ADD COLUMN y_sku_warehouse_next_{days}days_sales INTEGER DEFAULT 0;
    WITH next_sku_warehouse_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY sku, warehouse
            ORDER BY date
            RANGE BETWEEN
                 INTERVAL '1 DAY' FOLLOWING
                 AND INTERVAL '{days} DAY' FOLLOWING
        ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET y_sku_warehouse_next_{days}days_sales =
        nsws.calculated
    FROM next_sku_warehouse_sales nsws
    WHERE df_sale.sku = nsws.sku
        AND df_sale.warehouse = nsws.warehouse
        AND df_sale.date = nsws.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_warehouse_last_xdays_mean_sales(conn, days):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_warehouse_last_{days}days_mean_sales FLOAT DEFAULT 0.0;
    WITH last_sku_warehouse_mean_sales AS (
        SELECT *,
            COALESCE(ROUND(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse
                ORDER BY date
                RANGE BETWEEN
                    INTERVAL '{days} DAY' PRECEDING
                    AND INTERVAL '1 DAY' PRECEDING
            )/{days}.0, 4), 0) AS calculated
        FROM df_sale
        )
    UPDATE df_sale
    SET sku_warehouse_last_{days}days_mean_sales =
        lswms.calculated
    FROM last_sku_warehouse_mean_sales lswms
    WHERE df_sale.sku = lswms.sku
        AND df_sale.warehouse = lswms.warehouse
        AND df_sale.date = lswms.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_historic_sales(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_historic_sales INTEGER DEFAULT 0;
    WITH sku_date_sales AS (
        SELECT sku, date, SUM(quantity) AS quantity
            FROM df_sale
            GROUP BY sku, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY sku
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM sku_date_sales
    )
    UPDATE df_sale
    SET sku_historic_sales = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_historic_sales_same_day_of_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_historic_sales_same_day_of_the_week INTEGER DEFAULT 0;
    WITH sku_historic_sales_same_day AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, EXTRACT(DOW FROM date)
                ORDER BY date
                RANGE BETWEEN UNBOUNDED PRECEDING
                    AND INTERVAL '1 DAY' PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET sku_historic_sales_same_day_of_the_week =
        chssd.calculated
    FROM sku_historic_sales_same_day chssd
    WHERE df_sale.sku = chssd.sku
        AND df_sale.warehouse = chssd.warehouse
        AND df_sale.date = chssd.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_historic_sales_same_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_historic_sales_same_month INTEGER DEFAULT 0;
    WITH sku_month_year_sales AS (
        SELECT sku,
            EXTRACT(MONTH FROM date) AS month,
            EXTRACT(YEAR FROM date) AS year,
            SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY sku,
            EXTRACT(MONTH FROM date), EXTRACT(YEAR FROM date)
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY sku, month
            ORDER BY year, month
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM sku_month_year_sales
    )
    UPDATE df_sale
    SET sku_historic_sales_same_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND EXTRACT(MONTH FROM date) = cs.month
        AND EXTRACT(YEAR FROM date) = cs.year
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_warehouse_historic_sales(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_warehouse_historic_sales INTEGER DEFAULT 0;
    WITH sku_warehouse_historic_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET sku_warehouse_historic_sales = cwhs.calculated
    FROM sku_warehouse_historic_sales cwhs
    WHERE df_sale.sku = cwhs.sku
        AND df_sale.warehouse = cwhs.warehouse
        AND df_sale.date = cwhs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_warehouse_historic_sales_same_day_of_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_warehouse_historic_sales_same_day_of_the_week
        INTEGER DEFAULT 0;
    WITH sku_warehouse_historic_sales_same_day AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, EXTRACT(DOW FROM date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET sku_warehouse_historic_sales_same_day_of_the_week = cwhssd.calculated
    FROM sku_warehouse_historic_sales_same_day cwhssd
    WHERE df_sale.sku = cwhssd.sku
        AND df_sale.warehouse = cwhssd.warehouse
        AND df_sale.date = cwhssd.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_warehouse_historic_sales_same_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_warehouse_historic_sales_same_month INTEGER DEFAULT 0;
    WITH sku_warehouse_historic_sales_same_month AS (
        SELECT sku, warehouse,
            EXTRACT(MONTH FROM date) AS month,
            EXTRACT(YEAR FROM date) AS year,
            SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY sku, warehouse,
            EXTRACT(MONTH FROM date), EXTRACT(YEAR FROM date)
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY sku, warehouse, month
            ORDER BY year, month
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM sku_warehouse_historic_sales_same_month
    )
    UPDATE df_sale
    SET sku_warehouse_historic_sales_same_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND df_sale.warehouse = cs.warehouse
        AND EXTRACT(MONTH FROM date) = cs.month
        AND EXTRACT(YEAR FROM date) = cs.year
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_unit_price_information(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN unit_price INTEGER DEFAULT 0;
    UPDATE df_sale
    SET unit_price = COALESCE(ROUND(sale_amount/quantity, 0), 0)
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_cumulative_sales_in_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_cumulative_sales_in_the_week INTEGER DEFAULT 0;
    WITH sku_date_sales AS (
        SELECT sku, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY sku, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY sku, to_char(date, 'IW-IYYY')
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM sku_date_sales
    )
    UPDATE df_sale
    SET sku_cumulative_sales_in_the_week = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_cumulative_sales_in_the_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_cumulative_sales_in_the_month INTEGER DEFAULT 0;
    WITH sku_date_sales AS (
        SELECT sku, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY sku, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY sku, to_char(date, 'MM-YYYY')
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM sku_date_sales
    )
    UPDATE df_sale
    SET sku_cumulative_sales_in_the_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_cumulative_sales_in_the_year(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_cumulative_sales_in_the_year INTEGER DEFAULT 0;
    WITH sku_year_sales AS (
        SELECT sku, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY sku, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY sku, EXTRACT(YEAR FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM sku_year_sales
    )
    UPDATE df_sale
    SET sku_cumulative_sales_in_the_year = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_warehouse_cumulative_sales_in_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_warehouse_cumulative_sales_in_the_week
        INTEGER DEFAULT 0;
    WITH cumulative_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, to_char(date, 'IW-IYYY')
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET sku_warehouse_cumulative_sales_in_the_week = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_warehouse_cumulative_sales_in_the_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_warehouse_cumulative_sales_in_the_month
        INTEGER DEFAULT 0;
    WITH cumulative_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, to_char(date, 'MM-YYYY')
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET sku_warehouse_cumulative_sales_in_the_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_sku_warehouse_cumulative_sales_in_the_year(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN sku_warehouse_cumulative_sales_in_the_year
        INTEGER DEFAULT 0;
    WITH cumulative_sales AS (
        SELECT *,
            COALESCE(SUM(quantity)
            OVER (
                PARTITION BY sku, warehouse, EXTRACT(YEAR FROM date)
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS calculated
        FROM df_sale
    )
    UPDATE df_sale
    SET sku_warehouse_cumulative_sales_in_the_year = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.sku = cs.sku
        AND df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
