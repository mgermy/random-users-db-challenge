import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json


class GetData:

    def __init__(self, number_users):
        self.number_users = number_users

    def __save_json_file_locally(self, data):
        with open('./data/data.json', 'w') as f:
            json.dump(data, f)

    def get_users_data_from_api(self):
        session = requests.Session()

        # tries 3 times to get the data from the api, only if errors from server side
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        url = f'https://randomuser.me/api/?results={self.number_users}'
        request = session.get(url)

        data = request.json()
        self.__save_json_file_locally(data)


if __name__ == "__main__":
    GetData(4500).get_users_data_from_api()
