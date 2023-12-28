
import os
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.api_client import ApiClient
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import argparse
from download_dataset import download_kaggle_dataset
from ingest_data import ingest_data



if __name__ == "__main__":

    kaggle_dataset = 'vassyesboy/netflix-engagement-jan-jun-23'
    raw_data_target_path = "C:/Users/jakeg/git/netflix-engagement/pipeline-practice/data/raw"
    raw_data_file_name = 'netflix_engagement_raw_data.csv'
    postgres_params = {
        'user':"postgres",
        'password':"root",
        'host':"localhost",
        'port':5432,
        'db':"netflix_engagement_db",
        'table_name':"netflix_engagement",
        'raw_data_folder':raw_data_target_path,
        'csv_name':raw_data_file_name
    }
    #postgres_url - add this later, not sure if needed yet
    #Change file path to parameter later


    download_kaggle_dataset(kaggle_dataset,raw_data_target_path, raw_data_file_name)
    ingest_data(postgres_params)


