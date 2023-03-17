from sales import sns, norm, plt, stats, np, pd, ps, data_holder, psycopg2


def add_weekday_information(conn):
    """
    Adds a new column to the input DataFrame with a column that explain which
    day of the week correspond the date:
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
    ALTER TABLE df_sale ADD COLUMN weekday INTEGER DEFAULT 0;
    UPDATE df_sale
    SET weekday = EXTRACT(DOW FROM date)
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_week_information(conn):
    """
    Adds a new column to the input DataFrame with a column that explain which
    week of the year correspond the date:
        1, 2, ... 52
    Parameters:
        --------
        df: DataFrame
            The input DataFrame with the sales data.
        --------
    Returns:
        DataFrame: The input DataFrame with the new column added with the
            format week
    """
    query = f'''
    ALTER TABLE df_sale ADD COLUMN week INTEGER DEFAULT 0;
    UPDATE df_sale
    SET week = EXTRACT(WEEK FROM date)
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_month_information(conn):
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
    ALTER TABLE df_sale ADD COLUMN month INTEGER DEFAULT 0;
    UPDATE df_sale
    SET month = EXTRACT(MONTH FROM date)
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_year_information(conn):
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
            format year in number
    """
    query = f'''
    ALTER TABLE df_sale ADD COLUMN year INTEGER DEFAULT 0;
    UPDATE df_sale
    SET year = EXTRACT(YEAR FROM date)
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def add_offer_day_information(conn):
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
    ALTER TABLE df_sale ADD COLUMN is_offer_day INTEGER DEFAULT 0;
    UPDATE df_sale
    SET is_offer_day =
        CASE WHEN df_c.start_date <= df_sale.date
                    AND df_sale.date <= df_c.end_date
                THEN 1
                ELSE 0
        END
    FROM df_campaign df_c
    WHERE df_sale.date BETWEEN df_c.start_date AND df_c.end_date
    '''
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
