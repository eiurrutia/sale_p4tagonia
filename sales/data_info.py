from sales import pd, np, data_holder


def preview():
    """Shows Data shape and Data first 5 rows
    """
    DATA = data_holder.get_data()
    print('Data shape: ', DATA.shape)
    return DATA.head()


def missing_values():
    """Shows missing values for each column of Data
    """
    DATA = data_holder.get_data()
    print('Data missing values: ')
    print(DATA.isna().sum())


def duplicates():
    """Gets the number of rows duplicated in Data and the percentage of total
    """
    DATA = data_holder.get_data()
    print(
        f'Duplicates in Data set: {DATA.duplicated().sum()},'
        f'({np.round(100 * DATA.duplicated().sum() / len(DATA), 2)}%)')


def cardinality():
    """Show Data type for each column and number of distinct values
    """
    DATA = data_holder.get_data()
    get_types()
    print('')
    print('')
    print('Data unique values: ')
    print('')
    print(DATA.nunique())


def get_types(features=None):
    """Gets the type of every feature, or the specified ones
    Parameters:
          --------
          features: [str]
            Array with the name of the features
    """
    DATA = data_holder.get_data()
    print('Data types: ')
    print('')
    if not features:
        print(DATA.dtypes)
    else:
        print(DATA[features].dtypes)
