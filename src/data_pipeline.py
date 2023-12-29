
import os
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.api_client import ApiClient
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from download_dataset import download_kaggle_dataset
from ingest_data import ingest_data
import data_transformations as dt
import database_connection as dc



if __name__ == "__main__":


    kaggle_dataset = 'vassyesboy/netflix-engagement-jan-jun-23'
    raw_data_target_path = "C:/Users/jakeg/git/netflix-engagement/pipeline-practice/data/raw"
    raw_data_file_name = 'netflix_engagement_raw_data.csv'
    postgres_params = {
        'user':"postgres",
        'password':"root",
        'host':"localhost",
        'port':5432,
        'dbname':"netflix_engagement_db",
        'table_name':"netflix_engagement",
        'raw_data_folder':raw_data_target_path,
        'csv_name':raw_data_file_name
    }

    #Download data from Kaggle and ingest into postgres
    try:
        download_kaggle_dataset(kaggle_dataset,raw_data_target_path, raw_data_file_name)
    except Exception as e:
        print(f"Error occured while downloading dataset: {e}")
    try:
        ingest_data(postgres_params)
    except Exception as e:
        print(f"Error occured while ingesting data: {e}")

    #Set up connection to database and begin transormations
    conn = dc.create_db_connection(postgres_params)
    try:
        #Rename columns to make lowercase and remove spaces
        print('Renaming columns...')
        dt.rename_columns(conn,postgres_params['table_name'])
        #Convert Genre column to array datatype
        print('Converting columns to array...')
        dt.convert_column_to_array(conn, postgres_params['table_name'], "genre")
        #Create new table with exploded genre column
        print('Exploding column...')
        dt.explode_column(conn, postgres_params['table_name'], "genre_array", "exploded_genre","exploded_genre")
        print('Cleaning genre column...')
        #Remove special characters from exploded genre column
        dt.remove_characters(conn,"exploded_genre","exploded_genre",["'","[","]"])
        #Create aggregated view for most viewed genres
        dt.create_aggregated_view(
            conn,
            view_name='most_viewed_genres',
            table_name='exploded_genre',
            group_by_columns='exploded_genre',
            aggregate_column='hours_viewed',
            aggregate_function='SUM',
            order_by='exploded_genre',
            order_direction='Desc'
        )


        print('Closing connection...')
        dc.close_db_connection(conn)
    except Exception as e: 
        print(f"Error occured while applying transformations: {e}")


