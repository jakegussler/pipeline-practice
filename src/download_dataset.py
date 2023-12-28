import os
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.api_client import ApiClient
import zipfile


def download_kaggle_dataset(dataset_name, path, output_file_name):
    try: 
        #Initialize the api
        print("Initalizing the API")
        api = KaggleApi()
        print("Authenticating API")
        api.authenticate()
        print("Authenticated")
        

        #Create the directory if it does not exist
        print("Checking for directory")
        if not os.path.exists(path):
            print("Creating directory")
            os.makedirs(path)

        #Download the dataset
        print("Downloading dataset")
        api.dataset_download_files(dataset_name, path=path, unzip=True)

        print(f"Dataset {dataset_name} downloaded successfully to {path}")

        # Post-Download Processing
        print("Renaming file")
        rename_files(path, output_file_name)

    except Exception as e:
        print(f"An unexpected error occured: {e}")



def unzip_dataset(zip_path, extract_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    os.remove(zip_path)  # Remove the zip file after extraction

def rename_files(directory, new_name):
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            os.rename(os.path.join(directory, filename), os.path.join(directory, new_name))


if __name__ == "__main__":

    dataset = 'vassyesboy/netflix-engagement-jan-jun-23'
    target_path = "C:/Users/jakeg/git/netflix-engagement/pipeline-practice/data/raw"
    output_file_name = 'netflix_engagement_raw_data.csv'

    download_kaggle_dataset(dataset, target_path, output_file_name)
    #kaggle datasets download -d vassyesboy/netflix-engagement-jan-jun-23