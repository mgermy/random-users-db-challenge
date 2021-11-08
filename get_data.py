import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from utils.http_errors import HttpStatusCode


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
        with open('./data/data.csv', "w") as file:
            file.write(text_to_write)

    def get_users_data_from_api(self):
        session = requests.Session()

        retries = Retry(total=self.GET_USER_RETRIES_NUMBER, backoff_factor=1,
                        status_forcelist=[HttpStatusCode.BAD_GATEWAY.value, HttpStatusCode.SERVICE_UNAVAILABLE.value,
                                          HttpStatusCode.GATEWAY_TIMEOUT.value])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        url = f'https://randomuser.me/api/?results={self.number_users}&format=csv'
        response = session.get(url)
        self.__save_csv_file_locally(response.text)