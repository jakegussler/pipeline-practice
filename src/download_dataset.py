import os
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.api_client import ApiClient


def download_dataset(dataset_name, path):
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
    except Exception as e:
        print(f"An unexpected error occured: {e}")

if __name__ == "__main__":

    dataset = 'vassyesboy/netflix-engagement-jan-jun-23'
    target_path = "C:/Users/jakeg/git/netflix-engagement/pipeline-practice/data/raw"

    download_dataset(dataset, target_path)
    #kaggle datasets download -d vassyesboy/netflix-engagement-jan-jun-23