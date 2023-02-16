import pandas as pd


class DataHolder:

    def __init__(self):
        self._data = None
        self._original_data = None

    def set_data(self, data):
        if self._original_data is None:
            self._original_data = data.copy()
        self._data = data

    def get_data(self) -> pd.DataFrame:
        return self._data

    def remove_rows_by_values(self, features, values):
        """Remove rows by indicated features and values
        Parameters:
            --------
            features: [str]
                Array with the name of the features
            --------
            values: [str]
                Array with the value where the position indicate
                which feature you have to compare it
        """
        df = self.get_data()
        for col, val in zip(features, values):
            df = df[df[col] != val]
        self.set_data(df)

    def remove_columns_by_features(self, features):
        """Remove columns and just keep indicated features
        Parameters:
            --------
            features: [str]
                Array with the name of the features
        """
        df = self.get_data()[features]
        self.set_data(df)

    def group_data(self, features, aggregation):
        """Group data by indicated features, or the specified ones
        Parameters:
            --------
            features: [str]
                Array with the name of the features
            --------
            aggregation: dictionary
                dictionary to pass the aggrations in groupby
        """
        df = self.get_data()
        self.set_data(df.groupby(features).agg(aggregation).reset_index())

    def delete_duplicates(self):
        """Delete duplicated rows
        """
        return self.get_data().drop_duplicates(inplace=True)

    def rename_columns(self, rename_dict):
        """Rename data columns
        Parameters:
            --------
            rename_dict: dictionary
                Disctionary with pairs old_name: new_name
        """
        df = self.get_data()
        return self.set_data(df.rename(columns=rename_dict))
