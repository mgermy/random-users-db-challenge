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
from utils.http_errors import HttpStatusCode

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

        retries = Retry(total=self.GET_USER_RETRIES_NUMBER, backoff_factor=1,
                        status_forcelist=[HttpStatusCode.BAD_GATEWAY.value, HttpStatusCode.SERVICE_UNAVAILABLE.value,
                                          HttpStatusCode.GATEWAY_TIMEOUT.value])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        url = f'https://randomuser.me/api/?results={self.number_users}&format=csv'
        response = session.get(url)
        self.__save_csv_file_locally(response.text)


class Etl:
    TABLES_PREFIX = 'MICHELL_test'
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
        """
        Convert to unix time for easier manipulation of datetimes
        :param data:
        :return:
        """
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

    def create_db_table(self, query_name: str):
        """Creates table in the db"""

        rel_path = f'sql_queries/{query_name}'
        abs_file_path = os.path.join(script_dir, rel_path)
        query_file = open(abs_file_path, 'r')

        with self.engine.connect() as connection:
            connection.execute(query_file.read())

    def load_data_table_male_female(self):
        try:
            with self.engine.connect() as connection:
                self.create_db_table('create_male_female_tbl.sql')
                df_male, df_female = self.split_dataset()
                # todo michell prexif should be a constant or a function or both
                df_male.to_sql(con=connection, schema='interview', name=f'{self.TABLES_PREFIX}_male', if_exists='append',
                               index=False)
                df_female.to_sql(con=connection, schema='interview', name=f'{self.TABLES_PREFIX}_female', if_exists='append',
                                 index=False)
        except exc.IntegrityError:
            # todo missing information which table and which data
            raise Exception('This data has already been uploaded to this table. There are repeated users.')

    def slice_dataframe(self, df):
        DECREASE_DECIMAL = 10

        df_sliced_dict = {}
        for age_group in df['age_group'].unique():
            df_sliced_dict[int(age_group / DECREASE_DECIMAL)] = df[df['age_group'] == age_group].drop('age_group',
                                                                                                      axis=1)
        return df_sliced_dict

    def split_dataset_by_age_group(self) -> dict:
        df = self.transform_dataset().sort_values('dob_age')
        # todo name constants
        # bins: 0-9, 10-19, 20-29,..., 90-100
        bins = np.linspace(9, 100, num=10)
        # labels: 0s, 10s, 20s, 30s,..., 90s
        labels = bins[:len(bins)-1]+1
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
        try:
            with self.engine.connect() as connection:
                self.create_subset_tables_by_age_group()
                for age_group, df in self.split_dataset_by_age_group().items():
                    table_name = f'{self.TABLES_PREFIX}_{age_group}'
                    df.to_sql(con=connection, schema='interview', name=table_name, if_exists='append',
                              index=False)
        except exc.IntegrityError:
            raise Exception(f'The same data has already been uploaded to table {table_name}.')

    def get_query_absolute_path(self, query_name):
        rel_path = f'sql_queries/{query_name}'
        abs_file_path = os.path.join(script_dir, rel_path)

        return abs_file_path

    def load_top20_users_table(self):
        try:
            with self.engine.connect() as connection:
                table_name = f'{self.TABLES_PREFIX}_20'
                self.create_db_table('create_top_20_male_female_tbl.sql')
                query_file = open(self.get_query_absolute_path('get_top_20_male_female_tbl.sql'))
                query_data = pd.read_sql(query_file.read(), connection)
                query_data.to_sql(con=connection, schema='interview', name=table_name, if_exists='append',
                                  index=False)
        except exc.IntegrityError:
            raise Exception(f'The same data has already been uploaded to table {table_name}.')

    def combine_tables_20_and_5(self):
        with self.engine.connect() as connection:
            query_file = open(self.get_query_absolute_path('get_combined_tables_20_and_5.sql'))
            query_data = pd.read_sql(query_file.read(), connection)
            query_data.to_json(r'./data/first.json')

    def combine_tables_20_and_2(self):
        # Todo test function
        with self.engine.connect() as connection:
            query_file = open(self.get_query_absolute_path('get_combined_tables_20_and_2.sql'))
            query_data = pd.read_sql(query_file.read(), connection)
            query_data.to_json(r'./data/second.json')


if __name__ == "__main__":
    # Etl().load_data_table_male_female()
    # Etl().load_table_by_age_group()
    # Etl().load_top20_users_table()
    # Etl().combine_tables_20_and_5()
    Etl().combine_tables_20_and_2()
