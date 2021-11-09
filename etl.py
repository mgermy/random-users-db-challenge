import pandas as pd
from decouple import config
import os
from sqlalchemy import create_engine, exc
import urllib.parse
import numpy as np
from get_data import GetData
import pathlib
from datetime import datetime
import logger as logger


script_dir = os.path.dirname(__file__)

# creates logs folder if it does not exist
pathlib.Path("logs").mkdir(parents=True, exist_ok=True)
log = logger.setup_applevel_logger(
    file_name='logs/app_debug_{}.log'.format(str(datetime.now().strftime("%m-%d-%Y-%H-%M-%S"))))


class Etl:
    TABLES_PREFIX = 'MICHELL_test'
    ENCODING = 'utf8'
    NUMBER_USERS = 4500
    HOST = config('HOST')
    USER = config('DB_USER')
    PASSWORD = urllib.parse.quote_plus(config('DB_PASSWORD'))
    DB = config('DB')
    PORT = config('PORT')

    ENGINE = create_engine(f"mysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}?charset={ENCODING}", echo=False)

    @staticmethod
    def get_query_absolute_path(query_name) -> os.path:
        rel_path = f'sql_queries/{query_name}'
        abs_file_path = os.path.join(script_dir, rel_path)

        return abs_file_path

    def open_data(self):
        """Open data from csv file, if it exists.
        P.S.: If dealing with big data, best approach would be to load data with
        pyspark"""
        try:
            log.debug('Opening file')
            return pd.read_csv("./data/data.csv", na_values=['null', None],
                               encoding=self.ENCODING)
        except FileNotFoundError:
            log.debug('File not found. Creating file')
            GetData(self.NUMBER_USERS).get_users_data_from_api()
            return pd.read_csv("./data/data.csv", na_values=['null', None], encoding='utf-8')

    def rename_columns(self, data):
        """Rename columns removing '.' and replacing by '_'"""
        cols = data.columns
        cols_renamed = [w.replace('.', '_') for w in cols]
        data.columns = cols_renamed

    def convert_to_unixtime(self, df, column):
        df[f'{column}'] = (pd.to_datetime(df[f'{column}']) - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta('1s')

    def transform_datetimes_columns(self, data):
        """
        Convert each datetime column to unix time for easier manipulation
        :param data:
        :return:
        """

        for columns_to_convert in ['registered_date', 'dob_date']:
            self.convert_to_unixtime(data, columns_to_convert)

    def transform_dataset(self) -> pd.DataFrame:
        data = self.open_data()
        self.rename_columns(data)
        self.transform_datetimes_columns(data)
        return data

    def split_dataframe_male_female(self):
        """
        Splits dataset into male and females
        """
        data = self.transform_dataset()

        return data[data.gender == 'male'], data[data.gender == 'female']

    def create_db_table(self, query_name: str):
        """Creates table in the db"""

        query_file = open(self.get_query_absolute_path(query_name))

        with self.ENGINE.connect() as connection:
            connection.execute(query_file.read())

    def load_data_table_male_female(self):
        try:
            with self.ENGINE.connect() as connection:
                log.debug('Creating male/female tables')
                self.create_db_table('create_male_female_tbl.sql')
                df_male, df_female = self.split_dataframe_male_female()
                df_male.to_sql(con=connection, schema='interview', name=f'{self.TABLES_PREFIX}_male',
                               if_exists='append',
                               index=False)
                df_female.to_sql(con=connection, schema='interview', name=f'{self.TABLES_PREFIX}_female',
                                 if_exists='append',
                                 index=False)
        except exc.IntegrityError:
            log.error('Duplicated data')
            raise Exception(f'The same data has already been uploaded to table.')

    def slice_dataframe(self, df) -> dict:
        decrease_decimal = 10

        df_sliced_dict = {}
        for age_group in df['age_group'].unique():
            df_sliced_dict[int(age_group / decrease_decimal)] = df[df['age_group'] == age_group].drop('age_group',
                                                                                                      axis=1)
        return df_sliced_dict

    def split_dataset_by_age_group(self) -> dict:
        lower_boundary_bins = 10
        upper_boundary_bins = 100

        df = self.transform_dataset().sort_values('dob_age')
        # bins: 10-19, 20-29,..., 90-100
        bins = np.linspace(lower_boundary_bins, upper_boundary_bins, num=10)
        # labels: 10s, 20s, 30s,..., 90s
        labels = bins[:len(bins) - 1]
        df['age_group'] = pd.cut(df['dob_age'], bins=bins, labels=labels, right=False)

        return self.slice_dataframe(df)

    def create_subset_tables_by_age_group(self):
        """For each age group creates a table in the database. The table is only created if there's data for the
        specific age group."""

        for age_group in self.split_dataset_by_age_group().keys():
            connection = self.ENGINE.connect()
            query_file = open(self.get_query_absolute_path('create_age_groups_tbl.sql'))
            age_group_str = str(age_group)
            connection.execute(query_file.read().format(age_group_str))
            connection.close()

    def load_table_by_age_group(self):
        """Loads data into the tables created in create_subset_tables_by_age_group() according to its
        age group"""
        try:
            with self.ENGINE.connect() as connection:
                log.debug('Loading table age groups')
                self.create_subset_tables_by_age_group()
                for age_group, df in self.split_dataset_by_age_group().items():
                    table_name = f'{self.TABLES_PREFIX}_{age_group}'
                    df.to_sql(con=connection, schema='interview', name=table_name, if_exists='append',
                              index=False)
        except exc.IntegrityError:
            log.error('Repeated data')
            raise Exception(f'The same data has already been uploaded to table {table_name}.')

    def load_top20_users_table(self):
        try:
            with self.ENGINE.connect() as connection:
                log.debug('Loading table Top 20')
                table_name = f'{self.TABLES_PREFIX}_20'
                self.create_db_table('create_top_20_male_female_tbl.sql')
                query_file = open(self.get_query_absolute_path('get_top_20_male_female_tbl.sql'))
                query_data = pd.read_sql(query_file.read(), connection)
                query_data.to_sql(con=connection, schema='interview', name=table_name, if_exists='append',
                                  index=False)
        except exc.IntegrityError:
            log.error('Repeated data')
            raise Exception(f'The same data has already been uploaded to table {table_name}.')

    def combine_tables_20_and_5(self):
        with self.ENGINE.connect() as connection:
            log.debug('Combining tables 20 & 5')
            query_file = open(self.get_query_absolute_path('get_combined_tables_20_and_5.sql'))
            query_data = pd.read_sql(query_file.read(), connection)
            query_data.to_json(r'./data/first.json')

    def combine_tables_20_and_2(self):
        with self.ENGINE.connect() as connection:
            log.debug('Combining tables 20 & 2')
            query_file = open(self.get_query_absolute_path('get_combined_tables_20_and_2.sql'))
            query_data = pd.read_sql(query_file.read(), connection)
            query_data.to_json(r'./data/second.json')


if __name__ == "__main__":
    Etl().load_data_table_male_female()
    Etl().load_table_by_age_group()
    Etl().load_top20_users_table()
    Etl().combine_tables_20_and_5()
    Etl().combine_tables_20_and_2()
