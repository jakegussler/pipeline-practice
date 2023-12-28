import psycopg2


def convert_column_to_array(conn, table_name, column_name):
    try:
        curr = conn.cursor()
        print(f"Converting {column_name} in table {table_name} to array...")
        transform_query = f"""
        ALTER TABLE {table_name}
        ADD COLUMN {column_name}_array TEXT[];

        UPDATE {table_name}
        SET {column_name}_array = STRING_TO_ARRAY("{column_name}", ', ')
        """

        curr.execute(transform_query)
        conn.commit()
    except Exception as e:
        print(f"An error occured during transformation convert_column_to_array: {e}")

def explode_column(conn, table_name, column_name, new_table_name):
    try:
        curr = conn.cursor()
        print(f"Exploding {column_name} into seperate rows")
        query = f"""
        CREATE TABLE {new_table_name} AS
        SELECT
            n."index",
            UNNEST({column_name}),
            n."Title",
            n."Release Date",
            n."Hours Viewed",
            n."Number of Ratings",
            n."Rating",
            n."Key Words",
            n."Description"
        FROM
                {table_name} AS n;
            """
        
        curr.execute(query)
        conn.commit()
    except Exception as e:
        print(f"An error occured while exploding {column_name} in {table_name}: {e}")