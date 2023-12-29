import psycopg2


def convert_column_to_array(conn, table_name, column_name):
    try:
        #Create cursor object
        curr = conn.cursor()
        print(f"Converting {column_name} in table {table_name} to array...")
        transform_query = f"""
        ALTER TABLE {table_name}
        ADD COLUMN {column_name}_array TEXT[];

        UPDATE {table_name}
        SET {column_name}_array = STRING_TO_ARRAY("{column_name}", ', ')
        """
        #Execute and commit transform query
        curr.execute(transform_query)
        conn.commit()
    except Exception as e:
        print(f"An error occured during transformation convert_column_to_array: {e}")

def explode_column(conn, table_name, column_name, new_table_name):
    try:
        #Get all column names then remove column name that needs to be exploded
        other_columns = get_table_columns(conn, table_name)
        other_columns.remove(column_name)
        #Convert to format t."column_name"
        other_columns = ["t.\"" + name + "\"" for name in other_columns]

        #Create cursor object
        cur = conn.cursor()
        print(f"Exploding {column_name} into seperate rows")
        query = f"""
        CREATE TABLE {new_table_name} AS
        SELECT
            UNNEST({column_name}),
            {', '.join(other_columns)}
        FROM
            {table_name} AS t;
            """
        #Execute and commit transform query
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(f"An error occured while exploding {column_name} in {table_name}: {e}")


def get_table_columns(conn, table_name):
    try:
        cur = conn.cursor()
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{table_name}';
        """
        #Ececute qury and fetch results
        cur.execute(query)
        columns = cur.fetchall()

        #Extract column names from the query result
        column_names = [col[0] for col in columns]
        return column_names
    except Exception as e:
        print(f"An error occured: {e}")
        return[]
    
def remove_characters(conn,table_name,column_name,char):
        #Create cursor object
        curr = conn.cursor()
        


def generate_sql_replace_query(table_name,column_name,characters_to_remove, replacement_characters):
    """
    Generates an SQL query to remove specific characters or strings from a column in a table.

    :param table_name: Name of the table.
    :param column_name: Name of the column from which characters/strings are to be removed.
    :param characters_to_remove: List of characters or strings to remove.
    :param replacement_characters: List or value to replace removed characters with
    :return: SQL query string.
    """
    base_query = f"UPDATE {table_name} SET {column_name} = "
    replace_query = f"{column_name}"

    for char in characters_to_remove:
         replace_query = f"REPLACE('{char}','{replacement_characters}')"
    final_query = base_query + replace_query + ";"
    return final_query

