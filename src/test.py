import os
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.api_client import ApiClient


def download_dataset(dataset_name, path):
    try: 
        #Initialize the api
        api = KaggleApi()
        api.authenticate()

        #Create the directory if it does not exist
        if not os.path.exists(path):
            os.makedirs(path)
        
        #Download the dataset
        api.dataset_download_files(dataset_name, path=path, unzip=True)

        print(f"Dataset {dataset_name} downloaded successfully to {path}")
    except Exception as e:
        print(f"An unexpected error occured: {e}")

if __name__ == "__main__":

    dataset = 'vassyesboy/netflix-engagement-jan-jun-23'
    target_path = "C:/Users/jakeg/git/projects/netflix-engagement/data"

    download_dataset(dataset, target_path)
    #kaggle datasets download -d vassyesboy/netflix-engagement-jan-jun-23