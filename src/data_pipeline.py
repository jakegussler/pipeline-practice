
import download_dataset
import ingest_data

if __name__ == "__main__":

    kaggle_dataset = 'vassyesboy/netflix-engagement-jan-jun-23'
    raw_data_target_path = "C:/Users/jakeg/git/netflix-engagement/pipeline-practice/data/raw"
    postgres_user = "postgres"
    postgres_password = "admin"
    postgres_host = "localhost"
    postgres_port = 5432
    postgres_db = "netflix_engagement_db"
    postgres_table_name = "netflix_engagement"
    #postgres_url - add this later, not sure if needed yet
    #Change file path to parameter later


    download_dataset(kaggle_dataset,raw_data_target_path)
    ingest_data(user=postgres_user,password=postgres_password,host=postgres_host,port = postgres_port,db = postgres_db, table_name = postgres_table_name)


