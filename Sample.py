import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def update_postgres_table_if_rows_not_exist(dataframe, table_name, conn_string, unique_key_columns):
    try:
        # Establish a database connection
        engine = create_engine(conn_string)

        # Create a list of column names as a comma-separated string
        column_names = ", ".join(dataframe.columns)

        # Create a list of placeholders for SQL parameter binding
        placeholders = ", ".join(["%s"] * len(dataframe.columns))

        # Create a SQL INSERT statement with ON CONFLICT DO NOTHING clause
        insert_sql = f"""
        INSERT INTO {table_name} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT ({", ".join(unique_key_columns)}) DO NOTHING;
        """

        # Convert the DataFrame to a list of tuples for insertion
        data_to_insert = [tuple(row) for row in dataframe.values]

        # Execute the SQL statement with parameter binding
        with engine.connect() as conn:
            conn.execute(insert_sql, data_to_insert)

        return "Table updated successfully."

    except Exception as e:
        return f"Error: {str(e)}"


# Example usage:
if __name__ == "__main__":
    # Define your PostgreSQL connection string
    conn_string = "postgresql://username:password@localhost:5432/database_name"

    # Load your DataFrame with the data to update
    data_to_update = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
    })

    # Define the table name in the database
    table_name = 'your_table_name'

    # Define the columns that make up the unique key --all columns
    unique_key_columns = data_to_update.columns.tolist()

    result = update_postgres_table_if_rows_not_exist(data_to_update, table_name, conn_string, unique_key_columns)
    print(result)
