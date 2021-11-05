import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import mysql.connector
from decouple import config
import os
from sqlalchemy import create_engine, exc
import urllib.parse
import numpy as np

script_dir = os.path.dirname(__file__)


class GetData:
    GET_USER_RETRIES_NUMBER = 3

    def __init__(self, number_users):
        self.number_users = number_users

    def __save_csv_file_locally(self, text_to_write):
        """
        Saving data locally so we use always the same file for the ETL
        :param text_to_write: csv data
        :return:
        """
        f = open('./data/data.csv', "w")
        f.write(text_to_write)
        f.close()

    def get_users_data_from_api(self):
        session = requests.Session()

# todo is there any enumeration of http status codes in python
        retries = Retry(total=self.GET_USER_RETRIES_NUMBER, backoff_factor=1, status_forcelist=[502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        url = f'https://randomuser.me/api/?results={self.number_users}&format=csv'
        response = session.get(url)
        self.__save_csv_file_locally(response.text)


class Etl:
    HOST = config('HOST')
    USER = config('DB_USER')
    PASSWORD = urllib.parse.quote_plus(config('DB_PASSWORD'))
    DB = config('DB')
    PORT = config('PORT')

    engine = create_engine(f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}?charset=utf8", echo=False)

    def open_data(self):
        """Open data from csv file, if it exists.
        P.S.: If dealing with big data, best approach would be to load data with
        pyspark"""
        try:
            # TODO store as class level constant filepath and encoding
            return pd.read_csv("./data/data.csv", na_values=['null', None],
                               encoding='utf-8')
        except FileNotFoundError:
            # TODO 4500 IS A CONSTANT
            GetData(4500).get_users_data_from_api()
            # todo can utf8 be a constant everywhere and also na values
            return pd.read_csv("./data/data.csv", na_values=['null', None], encoding='utf-8', )

    def rename_columns(self, data):
        """Rename columns removing '.' and replacing by '_'"""
        cols = data.columns
        cols_renamed = [w.replace('.', '_') for w in cols]
        data.columns = cols_renamed

    def transform_datetimes(self, data):
        # TODO extract to function
        data['registered_date'] = (pd.to_datetime(data.registered_date) - pd.Timestamp("1970-01-01",
                                                                                       tz="UTC")) // pd.Timedelta('1s')
        data['dob_date'] = (pd.to_datetime(data.dob_date) - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta('1s')

    def transform_dataset(self):
        data = self.open_data()
        self.rename_columns(data)
        self.transform_datetimes(data)
        return data

# TODO include what kind of splitting this is should include the word data frame
    def split_dataset(self):
        """
        Splits dataset into male and females
        """
        data = self.transform_dataset()

        return data[data.gender == 'male'], data[data.gender == 'female']

    def create_table_male_female(self):
        """Creates a table for females and another for males"""

        rel_path = 'sql_queries/create_male_female_tbl.sql'
        abs_file_path = os.path.join(script_dir, rel_path)
        # TODO rename query to query_file
        query = open(abs_file_path, 'r')

        with self.engine.connect() as connection:
            connection.execute(query.read())

    def load_data_table_male_female(self):
        try:
            with self.engine.connect() as connection:
                self.create_table_male_female()
                df_male, df_female = self.split_dataset()
                #todo michell prexif should be a constant or a function or both
                df_male.to_sql(con=connection, schema='interview', name='MICHELL_test_male', if_exists='append', index=False)
                df_female.to_sql(con=connection, schema='interview', name='MICHELL_test_female', if_exists='append', index=False)
        except exc.IntegrityError:
            # todo missing information which table and which data
            raise Exception('This data has already been uploaded to this table. There are repeated users.')

    def slice_dataframe(self, df):
        df_sliced_dict = {}
        for age_group in df['age_group'].unique():
            df_sliced_dict[int(age_group)] = df[df['age_group'] == age_group].drop('age_group', axis=1)
        return df_sliced_dict

    def split_dataset_by_age_group(self) -> dict:
        df = self.transform_dataset().sort_values('dob_age')
        # todo name constants
        bins = np.linspace(10, 100, num=10)
        labels = bins[1:]
        df['age_group'] = pd.cut(df['dob_age'], bins=bins, labels=labels)

        return self.slice_dataframe(df)

    def create_subset_tables_by_age_group(self):
        # todo sql queries should be a function called get query by name
        rel_path = 'sql_queries/create_age_groups_tbl.sql'
        abs_file_path = os.path.join(script_dir, rel_path)

        for age_group in self.split_dataset_by_age_group().keys():
            connection = self.engine.connect()
            query_file = open(abs_file_path, 'r')
            age_group_str = str(age_group)
            connection.execute(query_file.read().format(age_group_str))
            connection.close()

    def load_table_by_age_group(self):
        self.create_subset_tables_by_age_group()
        try:
            with self.engine.connect() as connection:
                for age_group, df in self.split_dataset_by_age_group().items():
                    df.to_sql(con=connection, schema='interview', name=f'MICHELL_test_{age_group}', if_exists='append', index=False)
        except exc.IntegrityError:
            # TODO not informative
            raise Exception('This data has already been uploaded to this table. There are repeated users.')


if __name__ == "__main__":
    # Etl().load_data_table_male_female()
    Etl().load_table_by_age_group()