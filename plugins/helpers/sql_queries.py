class SqlQueries:
    def drop_table(table_name):
        """ Returns a drop table query statement with the table name argument. """
        return f"DROP TABLE IF EXISTS {table_name};"


    def create_table(table_name, table_columns):
        """ Returns a create table query statement with the table name and columns arguments. """
        # Convert the DataFrame to a string
        columns_string = ''
        for index, row in table_columns.iterrows():
            columns_string += f"{row['name']} {row['attributes']} {row['constraints']}"
            
            # If it is not the last row, add a comma
            if (index != table_columns.shape[0] - 1):
                columns_string += ","
        
        return f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_string});"


    def insert_records(table_name, column_names, table_data):
        """ Returns a insert into query statement with the table name, columns and data arguments. """
        # Convert the column names list to a string
        column_names_string = ",".join([column for column in column_names])
        
        return f"INSERT INTO {table_name} ({column_names_string}) {table_data};"


    staging_airport_table_drop = drop_table('')
    
    drop_table_queries = []
    create_table_queries = []