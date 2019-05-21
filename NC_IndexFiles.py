import glob
import json
import os
import re

# 取得所有 *-keywords 檔案路徑
def fetch_news_file(tokenized_dir: str):
    news_file_list = glob.glob('./{}/*-keyword.json'.format(tokenized_dir))
    return news_file_list

def intermediate_created(file_date, intermediate_dir):
    file_path = os.path.join(intermediate_dir, "{}-formated.json".format(file_date))
    return os.path.isfile(file_path)

def has_NLAPI_result(api_result_dir, file_date):
    file_path = os.path.join(api_result_dir, "{}-sentiment.json".format(file_date))
    print(file_path)
    return os.path.isfile(file_path)

# 產生所有新聞列表，以前端格式排列
class ResultFormatter():
    def __init__(self, tokenized_dir:str, result_dir: str, indexfile_dir:str, intermediate_dir:str, REDO:bool):
        print("... init ...")
        self.__REDO = REDO
        self.__result_dir = result_dir
        self.__indexfile_dir = indexfile_dir
        self.__intermediate_dir = intermediate_dir
        self.__tokenized_dir = tokenized_dir
        self.__keyword_mapping = {}
        self.__keywords = {}
        self.__domains = {}
        
        with open("./{}/keyword_mapping.json".format(self.__indexfile_dir), "r") as f:
            mapping = json.loads(f.read())
            for v in mapping:
                if type(mapping[v]) is list:
                    for k in mapping[v]:
                        self.__keyword_mapping[k] = v
                else:
                    self.__keyword_mapping[mapping[v]] = v
        with open("./{}/keywords.json".format(self.__result_dir), "r") as f:
            k = json.loads(f.read())
            for v in k:
                self.__keywords[k[v]] = v

        with open("./{}/domains.json".format(self.__result_dir), "r") as f:
            d = json.loads(f.read())
            for v in d:
                self.__domains[d[v]] = v

    def execute(self):
        for file_path in glob.glob('./{}/*-keyword.json'.format(self.__tokenized_dir)):
            date = re.findall(r"[0-9]{8}", file_path)[0]
            if not intermediate_created(date, self.__intermediate_dir) or self.__REDO:
                print("start formatting result for", file_path, "...")
                result = {}
                news_id_list = {}
                titles = {}
                with open(file_path, 'rb') as f:
                    all_news_keywords = json.loads(f.read())
                    for news_id in all_news_keywords:
                        domain = all_news_keywords[news_id]["domain"]
                        trimed_title = self.__trim_title(all_news_keywords[news_id]["title"])
                        if not domain in titles:
                            titles[domain] = {}
                        if not trimed_title in titles[domain]:
                            titles[domain][trimed_title] = 0
                            news_id_list[news_id] = 0
                            for kw_id in self.__extract_keyword(all_news_keywords[news_id]["keywords"]):
                                if not kw_id in result:
                                    result[kw_id] = {}
                                if not domain in result[kw_id]:
                                    result[kw_id][domain] = []
                                result[kw_id][domain].append(news_id)

                self.__write_id_list_to_file(date, news_id_list)
                self.__write_struct_to_file(date, result)

    # 將群組的 keyword mapping 到主要 keyword, 再 mapping 到 keyword 編號
    def __extract_keyword(self, keyword_arr):
        result = []
        for keyword in keyword_arr:
            if keyword in self.__keyword_mapping:
                meta_keyword = self.__keywords[self.__keyword_mapping[keyword]]
                if keyword in self.__keyword_mapping and not meta_keyword in result:
                    result.append(meta_keyword)
        return result

    def __trim_title(self, title):
        sep = ["｜", " | ", " :: ", " - ", " – ", " -- "]
        for s in sep:
            splited = title.split(s)
            if s == " - " and splited[0].strip() == "無綫新聞":
                title = splited[2]
            elif s == " - " and splited[0].strip() == "自立晚報":
                title = splited[1]
            elif s == "｜" and splited[0].strip() == "放言Fount Media":
                title = splited[1]
            else:
                title = splited[0]
        return title.strip()

    def __write_id_list_to_file(self, date, news_id_list):
        out = "./{}/{}-newsids.json".format(self.__intermediate_dir, date)
        with open(out, "w") as f:
            f.write(json.dumps(news_id_list, indent=4))

    def __write_struct_to_file(self, date, result_format):
        out = "./{}/{}-formated.json".format(self.__intermediate_dir, date)
        with open(out, "w") as f:
            f.write(json.dumps(result_format, indent=4))

# 產生呼叫情緒分數所需之新聞資料，只產生沒分析過的日數 (以 API_RESULT_DIR 含有的檔案名稱判斷)
class APIReqGen():
    def __init__(self, tokenized_dir: str, api_req_dir: str, api_result_dir: str, intermediate_dir: str, REDO: bool):
        self.__tokenized_dir = tokenized_dir
        self.__api_req_dir = api_req_dir
        self.__api_res_dir = api_result_dir
        self.__intermediate_dir = intermediate_dir
        self.__analysed_dates = [ x.split("/")[-1].split("-")[0] for x in glob.glob('./{}/*-sentiment.json'.format(api_result_dir)) ]
        self.__collected_dates = [ x.split("/")[-1].split("-")[0] for x in glob.glob('./{}/*-newsids.json'.format(intermediate_dir)) ]

    def execute(self):
        for date in self.__collected_dates:
            if not date in self.__analysed_dates or REDO:
                collectable_id_list = []
                sentiment_id_list = []
                with open('./{}/{}-patch.json'.format(self.__api_req_dir, date), 'w') as fw:
                    with open('./{}/{}-newsids.json'.format(self.__intermediate_dir, date), 'rb') as f:
                        collectable_id_list = json.loads(f.read()).keys()

                    if has_NLAPI_result(self.__api_res_dir, date):
                        with open('./{}/{}-sentiment.json'.format(self.__api_res_dir, date), 'rb') as f:
                            sentiment_id_list = [json.loads(x)["article_id"] for x in f.readlines()]

                    news_id_list = list(set(collectable_id_list) - set(sentiment_id_list))
                    news_info = {}
                    with open('./{}/{}-keyword.json'.format(self.__tokenized_dir, date), 'rb') as f:
                        news_info = json.loads(f.read())

                    for news_id in news_id_list:
                        if news_id in news_info:
                            fw.write(json.dumps(news_info[news_id]) + '\n')

#if __name__ == '__main__':
    #REDO=True
def main(REDO=False):
    INDEX_ROOT = "NC_Indexes"
    INTERMEDIATES_ROOT = "NC_Intermediates"
    TOKENIZER_ROOT = "NC_Tokenizer"
    RESULT_ROOT = "NC_Result"

    API_REQUEST_DIR = "NC_APIRequest"
    API_RESULT_DIR = "NC_APIResult"

    intermediates_dir = os.path.join(INTERMEDIATES_ROOT)
    os.makedirs(intermediates_dir, exist_ok=True)

    print("Start filtering keywords, domains and titles ...")
    ResultFormatter(tokenized_dir=TOKENIZER_ROOT, result_dir=RESULT_ROOT, indexfile_dir=INDEX_ROOT, intermediate_dir=INTERMEDIATES_ROOT, REDO=REDO).execute()
    print("Start generate request data for NLAPI ...")
    APIReqGen(tokenized_dir=TOKENIZER_ROOT, api_req_dir=API_REQUEST_DIR, api_result_dir=API_RESULT_DIR, intermediate_dir=INTERMEDIATES_ROOT, REDO=REDO).execute()
