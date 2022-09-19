from datetime import datetime
import pandas as pd
import requests
import random
import json

"""
Anime
    ETL
      * Extract data from Animechan API and csv file from pc then merge them
      * Transform extracted data
      * Load transformed data
      * Logging
"""

class ETLAnime:
    def __init__(self):
        self.extracted_data   = None
        self.transformed_data = None
        self.list_quotes  = list()
        self.numeric_cols = list()
        self.object_cols  = list()
        self.url_all_anime        = "https://animechan.vercel.app/api/available/anime"
        self.url_anime_with_title = "https://animechan.vercel.app/api/quotes/anime?title="
        self.data_path_file       = "data/Anime.csv"
        self.target_destination   = "data/anime_details.csv"

    def extract(self):
        self.log("Extract phase Started")

        def extract_from(file_to_process, file_type):
            if file_type == "csv": dataframe = pd.read_csv(file_to_process)
            elif file_type == "json": dataframe = pd.read_json(file_to_process)
            return dataframe

        extracted_dataframe_file = extract_from(self.data_path_file, "csv")

        def extract_from_api(url):
            title_anime = list(requests.get(url).json())

            def fetch_data(url, titles, start, end):
                for title in titles[start:end]:
                    response = requests.get(url + title)
                    self.list_quotes.append(response.json()[random.randint(0, len(response.json()) - 1)])

            fetch_data(self.url_anime_with_title, title_anime, 0, 98)
            dataframe_quotes = pd.DataFrame(self.list_quotes)
            dataframe_quotes.columns = ["Name", "Character", "Quote"]
            return dataframe_quotes

        extracted_dataframe_api = extract_from_api(self.url_all_anime)
        self.extracted_data = pd.merge(extracted_dataframe_file, extracted_dataframe_api, on="Name")
        self.log("Extract phase Ended")

    def transform(self):
        self.log("Transform phase Started")
        self.transformed_data = self.extracted_data.copy()

        def grab_columms(dataframe):
            self.numeric_cols = [col for col in dataframe.columns if dataframe[col].dtype != "object"]
            self.object_cols  = [col for col in dataframe.columns if dataframe[col].dtype == "object"]

        def fill_na_numeric(dataframe, list_col):
            for col in list_col:
                dataframe[col].fillna(-1, inplace=True)

        def fill_na_object(dataframe, list_col):
            for col in list_col:
                dataframe[col].fillna("-", inplace=True)

        def convert_float_to_int(dataframe, list_col):
            for col in list_col:
                dataframe[col] = dataframe[col].astype("int")

        def drop_columns(dataframe, drop_cols):
            dataframe.drop(drop_cols, axis=1, inplace=True)

        grab_columms(self.transformed_data)
        fill_na_numeric(self.transformed_data, self.numeric_cols)
        fill_na_object(self.transformed_data, self.object_cols)
        convert_float_to_int(self.transformed_data, self.numeric_cols)

        drop_cols = ["Rank", "Content_Warning", "Related_Mange", "Related_anime", "Voice_actors", "staff"]
        drop_columns(self.transformed_data, drop_cols)

        self.log("Transform phase Ended")

    def load(self):
        self.log("Load phase Started")
        self.transformed_data.to_csv(self.target_destination, index=False)
        self.log("Load phase Ended")

    def log(self, message):
        timestamp_format = '%Y-%h-%d-%H:%M:%S'
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        with open("logfile.txt", "a") as f:
            f.write(timestamp + ',' + message + '\n')
