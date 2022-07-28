import pandas as pd


def _float_to_int(column: pd.Series) -> pd.Series:
    """ Converts a float column to an int one. """
    column[column.isna()] = 0
    return column.astype(int)


class DataCleaner:
    def clean_airport_data(df: pd.DataFrame) -> pd.DataFrame:
        """ Cleans the airport dataframe. """
        # Convert elevation float to int
        df['elevation_ft'] = _float_to_int(df['elevation_ft'])

        # Only gather the USA airports
        df_usa = df[df['iso_country'] == 'US']

        # Split the coordinates in latitude and longitude
        df_usa[['latitude', 'longitude']] = df_usa['coordinates'].str.split(', ', expand=True)
        df_usa.drop('coordinates', axis=1, inplace=True)

        # Split state from iso_region
        df_usa['state'] = df_usa['iso_region'].str.split('-', expand=True)[1]

        return df_usa


    def clean_cities_data(df: pd.DataFrame) -> pd.DataFrame:
        """ Cleans the cities dataframe. """
        # Convert population floats to ints
        df['Male Population'] = _float_to_int(df['Male Population'])
        df['Female Population'] = _float_to_int(df['Female Population'])
        df['Total Population'] = _float_to_int(df['Total Population'])
        df['Number of Veterans'] = _float_to_int(df['Number of Veterans'])
        df['Foreign-born'] = _float_to_int(df['Foreign-born'])
        df['Count'] = _float_to_int(df['Count'])

        return df


    def clean_i94prtl_data(df: pd.DataFrame) -> pd.DataFrame:
        """ Cleans the i94prtl dataframe. """
        # Split state from name
        df[['airport_name', 'state_code']] = df['airport_name'].str.split(', ', expand=True)[[0, 1]]

        # Strip additional whitespace
        df['state_code'] = df['state_code'].str.strip()

        return df