import requests
import asyncio
import glob
import json
import os
import six
import time
import sys

from time import sleep
from google.oauth2 import service_account
import subprocess

# gcp access token
#SCOPES = ['https://www.googleapis.com/auth/cloud-language']
#SERVICE_ACCOUNT_FILE = '/Users/mac/Documents/Projects/cofacts/gcskeyfile.json'


#if __name__ == '__main__':
def main():
    API_REQUEST_DIR = "NC_APIRequest"
    API_RESULT_DIR = "NC_APIResult"

    # limit reset time
    req_batch_limit = 590

    req_time = time.monotonic()
    req_reset_window = 60

    def refresh_token():
        result = subprocess.run(["gcloud", "auth", "application-default", "print-access-token"], stdout=subprocess.PIPE)
        return result.stdout.decode("utf-8").strip()

    @asyncio.coroutine
    def do_analyze(text, req_token):
        r = yield from loop.run_in_executor(None, lambda: requests.post('https://language.googleapis.com/v1/documents:analyzeSentiment', 
            json = {
                'encodingType': 'UTF8',
                'document': {
                    'type': 'PLAIN_TEXT',
                    'content': text
                }
            },
            headers = {
                'Authorization': 'Bearer {}'.format(req_token),
                'Content-Type': 'application/json; charset=utf-8'
            })
        )
        result = {}
        try:
            result = r.json()
        except Exception as e:
            print(e)
        finally:
            return result

    @asyncio.coroutine
    def request(article_id, text, req_token, write_file):
        if isinstance(text, six.binary_type):
            text = text.decode('utf-8')

        sentiment = {}
        try:
            sentiment = yield from do_analyze(text, req_token)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

        write_file.write(json.dumps({
            "article_id": article_id,
            "sentiment": sentiment}) + '\n')
        write_file.flush()
    req_token = refresh_token()
    req_token_refresh_time = time.monotonic()
    req_token_refresh_window = 1800
    # asyncio event loop
    loop = asyncio.get_event_loop()


    print("start calling gcp NL API, rate limit: {} requests per {} second".format(req_batch_limit, req_reset_window))
    for file_path in glob.glob('./{}/*-patch.json'.format(API_REQUEST_DIR)):

        date = file_path.split("-")[0].split("/")[-1]
        write_file_path = './{}/{}-sentiment.json'.format(API_RESULT_DIR, date)
        
        with open(file_path, 'rb') as f:

            write_file = open(write_file_path, "a+")

            news_list = [json.loads(x.decode('utf-8')) for x in f.readlines()]
            while len(news_list) > 0:
                now = time.monotonic()
                if not now - req_time > req_reset_window:
                    continue
                else:
                    req_time = now

                if now - req_token_refresh_time > req_token_refresh_window:
                    req_token = refresh_token()

                req_list = []
                if len(news_list) >= req_batch_limit:
                    req_list = news_list[0:req_batch_limit]
                    news_list = news_list[req_batch_limit + 1:]
                elif len(news_list) > 0:
                    req_list = news_list
                    news_list = []
                else:
                    break

                tasks = [asyncio.ensure_future(request(x["article_id"], "{} \n {}".format(x["title"], x["content_text"]), req_token, write_file)) for x in req_list]
                print('start calling NLAPI for req file {}'.format(file_path))
                loop.run_until_complete(asyncio.wait(tasks))
        os.remove(file_path)
    loop.close()
