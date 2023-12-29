import psycopg2


def convert_column_to_array(conn, table_name, column_name):
    """
    Converts a column containing strings in an array format into an actual array

    :param conn: database connection object
    :param table_name: Name of the table
    :param column_name: Column to convert
    """
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

def explode_column(conn, table_name, column_name, new_table_name, exploded_column_name):
    """
    Explodes a column containing an array into separate rows where each value in the array it placed in it's own row.

    :param conn: database connection object
    :param table_name: name of table in database
    :param column_name: name of the column containing the array which needs to be exploded
    :param new_table_name: Name of the new table created created to hold the exploded data
    :param exploded_column_name: What the column being exploded should be named in the new table
    """
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
            UNNEST({column_name}) AS {exploded_column_name},
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
    """
    Retrieves a list of table columns from a specified table.

    :param conn: database connection
    :param table_name: Name of the table
    :return: list of table columns
    """

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
    
def remove_characters(conn,table_name,column_name,characters_to_remove):
    """
    Connects to a database and generates a SQL query to remove characters for cleaning

    :param conn: database connection.
    :param table_name: Name of the table to edit
    :param column_name: Name of the column to use in the REPLACE statement
    :characters_to_remove: List of the characters that should be removed from the column
    """
        
    try:
        #Create cursor object
        cur = conn.cursor()
        #Generate a sql replace query
        print("Generating SQL replace query")
        query = generate_sql_replace_query(table_name,column_name, characters_to_remove,"")

        #Execute and commit query
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(f"An error occured {e}")



def generate_sql_replace_query(table_name,column_name,characters_to_remove, replacement_characters):
    """
    Generates an SQL query to remove specific characters or strings from a column in a table.

    :param table_name: Name of the table.
    :param column_name: Name of the column from which characters/strings are to be removed.
    :param characters_to_remove: List of characters or strings to remove.
    :param replacement_characters: List or value to replace removed characters with
    :return: SQL query string.
    """
    #Set up components for starting query
    base_query = f"UPDATE {table_name} AS t SET {column_name} = "
    replace_query = f"{column_name}"

    
    #Iterate through characters dynamically generating a SQL replace query
    for char in characters_to_remove:
         escaped_char = char.replace("'", "''")
         replace_query = f"REPLACE({replace_query},'{escaped_char}','{replacement_characters}')"

    #Generate and return final query
    final_query = base_query + replace_query + ";"
    return final_query

