from sales import sns, norm, plt, stats, np, pd, ps, data_holder


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
        SELECT *,
            strftime('%w', date) AS weekday
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['weekday'] = result['weekday'].astype('int64')
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
        SELECT *,
            strftime('%m', date) AS month
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['month'] = result['month'].astype('int64')
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
        SELECT *,
            strftime('%Y', date) AS year
        FROM df
    '''
    result = ps.sqldf(query)
    result['date'] = pd.to_datetime(result['date'])
    result['year'] = result['year'].astype('int64')
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
    result['is_offer_day'] = result['is_offer_day'].astype('int64')
    return result
