import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import mysql
from decouple import config
import os


class GetData:

    def __init__(self, number_users):
        self.number_users = number_users

    def __save_csv_file_locally(self, response):
        """
        Saving data locally so we use always the same file for the ETL
        :param data: json data
        :return:
        """
        # Write to .CSV
        f = open('./data/data.csv', "w")
        f.write(response.text)
        f.close()

    def get_users_data_from_api(self):
        session = requests.Session()

        # tries 3 times to get the data from the api, only if errors from server side
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        url = f'https://randomuser.me/api/?results={self.number_users}&format=csv'
        request = session.get(url)

        self.__save_csv_file_locally(request)


class Etl:
    HOST = config('HOST')
    USER = config('DB_USER')
    PASSWORD = config('DB_PASSWORD')

    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD
    )

    def open_data(self):
        try:
            return pd.read_csv("./data/data.csv")
        except FileNotFoundError:
            GetData(20).get_users_data_from_api()
            return pd.read_csv("./data/data.csv")

    def rename_columns(self, data):
        cols = data.columns
        cols_renamed = [w.replace('.', '_') for w in cols]
        data.columns = cols_renamed

        return data

    def split_dataset(self):
        """
        Splits dataset into male and females
        """
        data = self.open_data()
        results = self.rename_columns(data)

        return results[results.gender == 'male'], results[results.gender == 'female']

    def create_male_female_tabel(self):
        pass


if __name__ == "__main__":
    a = Etl().split_dataset()
