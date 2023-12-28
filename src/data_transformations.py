import psycopg2


def convert_column_to_array(conn, table_name, column_name):
    try:
        curr = conn.curson
        print(f"Converting {column_name} in table {table_name} to array...")
        transform_query = f"""
        ALTER TABLE {table_name}
        ADD COLUMN {column_name}_array TEXT[]

        UPDATE {table_name}
        SET {column_name}_array = string_to_array("{column_name}")
        """

        curr.execute(transform_query)
        conn.commit()
    except Exception as e:
        print(f"An error occured during transformation convert_column_to_array: {e}")