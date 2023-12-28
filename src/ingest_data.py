import pandas as pandas
import psycopg2
from sqlalchemy import create_engine

def main(params):
    print('Running Main Function...Assigning Parameters...')
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    db_config = {
        'host': host,
        'dbname': db,
        'user':user,
        'password': password
    }







if __name__ == '__main__':

    print('Initializing parser...')
    parser = argparse.ArgumentParser(description='Ingest CSV Data to Postgres')

    print('Adding Parser Arguments...')    
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write data to')
    parser.add_argument('--url', help='url of the csv file')
    parser.add_argument('--network', help='name of the network')

    print('Parsing args...')
    args = parser.parse_args()

    print('Running main with args...')
    main(args)

