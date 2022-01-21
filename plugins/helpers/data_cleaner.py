import pandas as pd


def _float_to_int(column: pd.Series) -> pd.Series:
        column[column.isna()] = 0
        return column.astype(int)


class DataCleaner:
    def clean_airport_data(df: pd.DataFrame) -> pd.DataFrame:
        # Convert elevation float to int
        df['elevation_ft'] = _float_to_int(df['elevation_ft'])

        # Split the coordinates in latitude and longitude
        df[['latitude', 'longitude']] = df['coordinates'].str.split(', ', expand=True)
        df.drop('coordinates', axis=1, inplace=True)

        return df


    def clean_temperature_data(df: pd.DataFrame) -> pd.DataFrame:
        # Filter out the NaN values for temperature
        df_no_nan = df[~df['AverageTemperature'].isna()]

        # Only gather the USA data
        df_usa = df_no_nan[df_no_nan['Country'] == 'United States']

        return df_usa


    def clean_cities_data(df: pd.DataFrame) -> pd.DataFrame:
        # Convert population floats to ints
        df['Male Population'] = _float_to_int(df['Male Population'])
        df['Female Population'] = _float_to_int(df['Female Population'])
        df['Total Population'] = _float_to_int(df['Total Population'])
        df['Number of Veterans'] = _float_to_int(df['Number of Veterans'])
        df['Count'] = _float_to_int(df['Count'])

        return df