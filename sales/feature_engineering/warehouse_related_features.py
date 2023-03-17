from sales import sns, norm, plt, stats, np, pd, ps, data_holder, psycopg2


def add_warehouse_is_metropolitan_zone(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN is_metropolitan_zone INTEGER DEFAULT 0;
    UPDATE df_sale
    SET is_metropolitan_zone = COALESCE(df_w.is_metropolitan_zone, 0)
    FROM df_warehouse df_w
    WHERE df_sale.warehouse = df_w.warehouse
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_warehouse_is_inside_mall(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN is_inside_mall INTEGER DEFAULT 0;
    UPDATE df_sale
    SET is_inside_mall = COALESCE(df_w.is_inside_mall, 0)
    FROM df_warehouse df_w
    WHERE df_sale.warehouse = df_w.warehouse
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_warehouse_last_xdays_sales(conn, days):
    """
    Adds a new column to the input DataFrame with the quantity products sold
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
            format warehouse_last_{days}days_sales
    """
    query = f'''
    ALTER TABLE df_sale
        ADD COLUMN warehouse_last_{days}days_sales INTEGER DEFAULT 0;
    WITH date_warehouse_sale AS (
        SELECT date, warehouse, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date, warehouse
    ),
    last_warehouse_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY warehouse
            ORDER BY date
            RANGE BETWEEN
                INTERVAL '{days} DAY' PRECEDING
                AND INTERVAL '1 DAY' PRECEDING
        ), 0) AS calculated
        FROM date_warehouse_sale
    )
    UPDATE df_sale
    SET warehouse_last_{days}days_sales =
        lws.calculated
    FROM last_warehouse_sales lws
    WHERE df_sale.warehouse = lws.warehouse
        AND df_sale.date = lws.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_warehouse_last_xdays_mean_sales(conn, days):
    """
    Adds a new column to the input DataFrame with the mean of products sold
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
            format warehouse_last_{days}days_mean_sales
    """
    query = f'''
    ALTER TABLE df_sale
        ADD COLUMN warehouse_last_{days}days_mean_sales FLOAT DEFAULT 0.0;
    WITH date_warehouse_sale AS (
        SELECT date, warehouse, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY date, warehouse
    ),
    last_warehouse_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY warehouse
            ORDER BY date
            RANGE BETWEEN
                INTERVAL '{days} DAY' PRECEDING
                AND INTERVAL '1 DAY' PRECEDING
        ), 0) AS calculated
        FROM date_warehouse_sale
    )
    UPDATE df_sale
    SET warehouse_last_{days}days_mean_sales =
        COALESCE(ROUND(lws.calculated/{days}.0, 4), 0)
    FROM last_warehouse_sales lws
    WHERE df_sale.warehouse = lws.warehouse
        AND df_sale.date = lws.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_warehouse_cumulative_sales_in_the_week(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN warehouse_cumulative_sales_in_the_week INTEGER DEFAULT 0;
    WITH date_warehouse_sale AS (
        SELECT warehouse, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY warehouse, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY warehouse, to_char(date, 'IW-IYYY')
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM date_warehouse_sale
    )
    UPDATE df_sale
    SET warehouse_cumulative_sales_in_the_week = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_warehouse_cumulative_sales_in_the_month(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN warehouse_cumulative_sales_in_the_month INTEGER DEFAULT 0;
    WITH date_warehouse_sale AS (
        SELECT warehouse, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY warehouse, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY warehouse, to_char(date, 'MM-YYYY')
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM date_warehouse_sale
    )
    UPDATE df_sale
    SET warehouse_cumulative_sales_in_the_month = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_warehouse_cumulative_sales_in_the_year(conn):
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
    ALTER TABLE df_sale
        ADD COLUMN warehouse_cumulative_sales_in_the_year INTEGER DEFAULT 0;
    WITH date_warehouse_sale AS (
        SELECT warehouse, date, SUM(quantity) AS quantity
        FROM df_sale
        GROUP BY warehouse, date
    ),
    cumulative_sales AS (
        SELECT *,
        COALESCE(SUM(quantity)
        OVER (
            PARTITION BY warehouse, EXTRACT(YEAR FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS calculated
        FROM date_warehouse_sale
    )
    UPDATE df_sale
    SET warehouse_cumulative_sales_in_the_year = cs.calculated
    FROM cumulative_sales cs
    WHERE df_sale.warehouse = cs.warehouse
        AND df_sale.date = cs.date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
