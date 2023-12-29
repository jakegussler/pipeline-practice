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

        #Drop the table if it already exists
        drop_query = f"DROP TABLE IF EXISTS {new_table_name}"
        cur.execute(drop_query)

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
        #Generate a query replace query
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

def rename_columns(conn, table_name):
    try:
        cur = conn.cursor()
        #Select current column names
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")    
        
        #Retrieve column names
        columns = cur.fetchall()

        #Iterate through the columns
        for col in columns:
            original_column_name = col[0]
            #Set new column name
            new_column_name = original_column_name.lower().replace(' ', '_')
            
            #Check if new column name and old column name are equal
            if (original_column_name == new_column_name):
                #Skip if no change is needed
                continue
            
            #SQL Query to rename column
            rename_query = f"ALTER TABLE {table_name} RENAME COLUMN \"{original_column_name}\" TO \"{new_column_name}\";"
            cur.execute(rename_query)

        #Commit changes to database
        conn.commit()
    except Exception as e:
        print(f"An error occured while renaming columns in {table_name}: {e}")


def create_aggregated_view(conn, view_name, table_name,group_by_columns,aggregate_column,aggregate_function,additional_columns=None,where_column=None, where_condition=None, order_by=None,order_direction='DESC',limit = None):
    try:
        cur = conn.cursor()
        #Base select statement
        select_clause = ', '.join(group_by_columns + additional_columns if additional_columns else group_by_columns)
        aggregation = f"{aggregate_function}({aggregate_column}) AS {aggregate_column}"
        query = f"CREATE OR REPLACE VIEW {view_name} AS SELECT {select_clause}, {aggregation} FROM {table_name}"

        #Add where column if applicable
        if where_column:
            query+= f" WHERE {where_column} {where_condition}"

        #Add group by clause to statement
        query+= f" GROUP BY {', '.join(group_by_columns)}"

        #Check if there is order by clause, add if there is
        if order_by:
            query+= f" ORDER BY {order_by} {order_direction}"
        if limit:
            query+= f" LIMIT {limit}"

        #Execute and commit query
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(f"An error occured while creating view {view_name}: {e}")
        conn.rollback()


        