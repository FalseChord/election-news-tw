import json
import gzip
import os
import requests
import time
import shutil

from datetime import timedelta
from datetime import datetime
from dateutil.rrule import rrule, DAILY

class DataFetcher:
    def __init__(self, base_url: str, start_date: datetime, target_dir: str, result_dir: str):
        self.__base_url = base_url
        self.__start_date = start_date
        self.__end_date = datetime.now() - timedelta(days=1)
        self.__target_dir = target_dir
        self.__result_dir = result_dir

    def execute(self, post_action=None):
        self.__update_latest_date(self.__end_date.strftime("%Y/%m/%d"))

        for date in self.__get_missing_dates():
            target_url = self.__base_url.format(date)
            http_response = self.__fetch_data_from_target_url_through_date(target_url)
            file_path = self.__write_http_content_to_file(http_response=http_response, target_url=target_url)
            print("{} saved.".format(file_path))
            if post_action is not None:
                post_action(file_path)

            time.sleep(2)


    def __get_missing_dates(self):
        missing_files = []
        for date in list(rrule(freq=DAILY, dtstart=self.__start_date, until=self.__end_date, interval=1)):
            date_in_string = date.strftime("%Y%m%d")
            file_name = "articles-{}.jsonl".format(date_in_string)
            file_path = os.path.join(self.__target_dir, file_name)
            if not os.path.isfile(file_path):
                missing_files.append(date_in_string)
        return missing_files

    def __get_url(self, date):
        date_in_string = date.strftime("%Y%m%d")
        target_url = self.__base_url.format(date_in_string)

        return target_url

    def __fetch_data_from_target_url_through_date(self, target_url: str):
        print("download: {}".format(target_url))
        response = requests.get(target_url)
        print("done")

        return response

    def __write_http_content_to_file(self, http_response, target_url: str):
        file_name = target_url.rsplit("/", 1)[1]
        file_path = os.path.join(self.__target_dir, file_name)

        with open(file_path, 'wb') as file:
            file.write(http_response.content)

        return file_path

    def __update_latest_date(self, date_string):
        interval = {}
        with open('./{}/date.json'.format(self.__result_dir), 'r') as f:
            interval = json.loads(f.read())
            interval["until"] = date_string

        with open('./{}/date.json'.format(self.__result_dir), 'w') as f:
            f.write(json.dumps(interval, indent=4))

class DataExtractor:
    def execute(self, source_file_path):
        target_file_path = self.__get_target_file_path(source_file_path)

        with gzip.open(source_file_path, 'rb') as gzip_file:
            with open(target_file_path, 'wb') as unzipped_file:
                shutil.copyfileobj(gzip_file, unzipped_file)

        print("extract {} to {}.".format(source_file_path, target_file_path))

    def __get_target_file_path(self, source_file_path):
        source_file_name = os.path.basename(source_file_path)
        split_source_file_name = os.path.splitext(source_file_name)

        filename_without_extension = split_source_file_name[0]
        extracted_extension = split_source_file_name[1]

        target_file_name = filename_without_extension
        target_file_path = os.path.join(os.path.dirname(source_file_path), target_file_name)

        return target_file_path

#if __name__ == '__main__':
def main():
    RESUTL_DIR = "NC_Result"

    download_dir = os.path.join("NC_Origin")
    os.makedirs(download_dir, exist_ok=True)

    zipped_base_url = "http://g0v-data.gugod.org/people-in-news/db/articles-{}.jsonl.gz"
    zipped_start_date = datetime(2018, 10, 19)

    DataFetcher(
        base_url=zipped_base_url, start_date=zipped_start_date, target_dir=download_dir, result_dir=RESUTL_DIR
    ).execute(post_action=DataExtractor().execute)
    
