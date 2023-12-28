import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
import argparse

def ingest_data(params):
    print('Running Main Function...Assigning Parameters...')
    user = params['user']
    password = params['password']
    host = params['host']
    port = params['port']
    db = params['db']
    table_name = params['table_name']
    raw_data_folder = params['raw_data_folder'] 
    #url = params.url
    csv_name = params['csv_name']
    path = os.path.join(raw_data_folder,csv_name)

    print("Creating connection string")
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    print("creating engine")
    engine = create_engine(connection_string)
    print("engine created")

    chunksize = 1000
    print(f"Reading CSV at {path}")
    for i, chunk in enumerate(pd.read_csv(path,chunksize=chunksize)):
        print(f"adding chunk {i}")
        chunk.to_sql(table_name, engine, if_exists='append'  if (i > 0) else 'replace')
    print("All chunks processed")



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
    parser.add_argument('--file_path', help='file path where raw data is being stored')
    #parser.add_argument('--url', help='url of the csv file')
    #parser.add_argument('--network', help='name of the network')

    print('Parsing args...')
    args = parser.parse_args()

    print('Running ingest_data with args...')
    ingest_data(args)

