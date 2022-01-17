class DataCleaner:
    def clean_airport_data(df):
        # Split the coordinates in latitude and longitude
        df[['latitude', 'longitude']] = df['coordinates'].str.split(', ', expand=True)
        df.drop('coordinates', axis=1, inplace=True)

        return df


    def clean_temperature_data(df):
        # Filter out the NaN values for temperature
        df_no_nan = df[~df['AverageTemperature'].isna()]

        # Only gather the USA data
        df_usa = df_no_nan[df_no_nan['Country'] == 'United States']

        return df_usa