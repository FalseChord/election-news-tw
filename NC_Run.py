import NC_Downloader
import NC_Tokenizer
import NC_IndexFiles
import NC_NLAPI
import NC_ResultFiles

import configparser
import json
import requests
import traceback

def post(payload):
    config = configparser.ConfigParser()
    config.read('related-news-engine.conf')
    url = config.get('MONITOR','SLACK_WEBHOOK_URL')
    headers = {
        "Content-type": "application/json"
    }

    r = requests.post(url, data=json.dumps(payload), headers=headers)
    print("Send message to slack...", r.content)

def main():
    NC_Downloader.main()
    NC_Tokenizer.main() # NC_Tokenizer.main(True) for Keywords updated
    NC_IndexFiles.main() # NC_IndexFiles.main(True) for Keywords updated
    NC_NLAPI.main()
    NC_ResultFiles.main()

if __name__=="__main__":
    try:
        post({
            "text": "Start to run: {}".format(__file__)
            })
        main()
    except Exception as e:
        post({
            "text": "Execution fail: {}\nerror: {} \nerror_stack: {}".format(__file__, e, traceback.format_exc())
            })
        pass
