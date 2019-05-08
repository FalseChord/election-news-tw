import glob
import json
import os
import re

# 完整新聞關鍵字整理
class KeywordAggregater():
    def __init__(self, tokenized_dir: str, indexfile_dir:str, intermediate_dir: str):
        print("... init ...")
        self.__tokenized_dir = tokenized_dir
        self.__indexfile_dir = indexfile_dir
        self.__intermediate_dir = intermediate_dir
        self.__news_list = {}

        with open('./{}/news_domain_list.json'.format(self.__indexfile_dir), 'rbsudo apt-get install python3.6') as f:
            self.__domain_list = json.loads(f.read())

    def execute(self):
        print("start to aggregating keywords ...")
        regex = re.compile(r'^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\?\/\n]+)',re.I)

        for file_path in glob.glob('./{}/*-article.json'.format(self.__tokenized_dir)):
            with open(file_path, 'rb') as f:
                for news in json.loads(f.read()):
                    domain = regex.search(news["url"]).group(0)
                    if domain in self.__domain_list:
                        self.__news_list[news["article_id"]] = {
                            "domain": domain,
                            "title": news["title"],
                            "keywords": []
                        }
                        if "substrings" in news:
                            subs = news["substrings"]
                            self.__news_list[news["article_id"]]["keywords"] = self.__check_and_get_keywords(
                                subs, ["countries", "taiwan-subdivisions", "powerful-people", "political-people", "events"]
                            )
                            
        for file_path in glob.glob('./{}/*-keyword.json'.format(self.__tokenized_dir)):
            with open(file_path, 'rb') as f:
                for news in json.loads(f.read()):
                    if news["article_id"] in self.__news_list:
                        self.__news_list[news["article_id"]]["keywords"] += [x["keyword"] for x in news["keyword_list"]]
                        self.__news_list[news["article_id"]]["keywords"] = list(set(self.__news_list[news["article_id"]]["keywords"]))

        self.__write_to_file()

    def __check_and_get_keywords(self, kw_obj, kw_attr):
        kws = []
        for attr in kw_attr:
            if attr in kw_obj:
                kws += kw_obj[attr]
        return kws

    def __write_to_file(self):

        out = "./{}/all_news_keywords.json".format(self.__intermediate_dir)
        with open(out, "w") as f:
            f.write(json.dumps(self.__news_list, indent=4))


# 產生所有新聞列表，以前端格式排列
class NewsCollecting():
    def __init__(self, result_dir: str, indexfile_dir:str, intermediate_dir:str):
        print("... init ...")
        self.__result_dir = result_dir
        self.__indexfile_dir = indexfile_dir
        self.__intermediate_dir = intermediate_dir
        self.__result = {}
        self.__keyword_mapping = {}
        self.__keywords = {}
        self.__domains = {}
        self.__collectable_keywords = []
        
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
                self.__result = {}

        with open("./{}/domains.json".format(self.__result_dir), "r") as f:
            d = json.loads(f.read())
            for v in d:
                self.__domains[d[v]] = v

    def execute(self):
        print("start filtering news ...")
        with open('./{}/all_news_keywords.json'.format(self.__intermediate_dir), 'rb') as f:
            all_news_keywords = json.loads(f.read())
            for news_id in all_news_keywords:
                date = self.__date_format(news_id)
                domain = all_news_keywords[news_id]["domain"]
                for kw_id in self.__extract_keyword(all_news_keywords[news_id]["keywords"]):
                    if not kw_id in self.__result:
                        self.__result[kw_id] = {}
                    if not date in self.__result[kw_id]:
                        self.__result[kw_id][date] = {}
                    if not domain in self.__result[kw_id][date]:
                        self.__result[kw_id][date][domain] = []

                    trimed_title = self.__trim_title(all_news_keywords[news_id]["title"])
                    if not trimed_title in [ x["t"] for x in self.__result[kw_id][date][domain]]:
                        self.__result[kw_id][date][domain].append({
                            "i": news_id,
                            "t": trimed_title
                        })

        self.__write_id_list_to_file()
        self.__write_struct_to_file()

    def __extract_keyword(self, keyword_arr):
        result = []
        for keyword in keyword_arr:
            if keyword in self.__keyword_mapping:
                meta_keyword = self.__keywords[self.__keyword_mapping[keyword]]
                if keyword in self.__keyword_mapping and not meta_keyword in result:
                    result.append(meta_keyword)
        return result

    def __date_format(self, news_id):
        date_string = news_id.split("-")[0]
        return "{}/{}/{}".format(date_string[0:4], date_string[4:6], date_string[6:8])

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

    def __write_id_list_to_file(self):
        id_dict = {}
        for kw in self.__result:
            for date in self.__result[kw]:
                for domain in self.__result[kw][date]:
                    for news in self.__result[kw][date][domain]:
                        if not news["i"] in id_dict:
                            id_dict[news["i"]] = 0

        out = "./{}/collectable_news_all_ids.json".format(self.__intermediate_dir)
        with open(out, "w") as f:
            f.write(json.dumps(id_dict, indent=4))

    def __write_struct_to_file(self):

        out = "./{}/result_formated.json".format(self.__intermediate_dir)
        with open(out, "w") as f:
            f.write(json.dumps(self.__result, indent=4))

# 產生呼叫情緒分數所需之新聞資料，只產生沒分析過的日數 (以 API_RESULT_DIR 含有的檔案名稱判斷)
class APIReqGen():
    def __init__(self, tokenized_dir: str, api_req_dir: str, api_result_dir: str, intermediate_dir: str):
        self.__tokenized_dir = tokenized_dir
        self.__api_req_dir = api_req_dir
        self.__analysed_dates = [ x.split("/")[-1].split("-")[0] for x in glob.glob('./{}/*-sentiment.json'.format(api_result_dir)) ]
        self.__tokenized_dates = [ x.split("/")[-1].split("-")[0] for x in glob.glob('./{}/*-article.json'.format(tokenized_dir)) ]
        with open("./{}/collectable_news_all_ids.json".format(intermediate_dir), 'rb') as f:
            self.__collectable_news_id_dict = json.loads(f.read())

    def execute(self):
        for date in self.__tokenized_dates:
            if not date in self.__analysed_dates:
                with open('./{}/{}-article.json'.format(self.__tokenized_dir, date), 'rb') as f:
                    with open('./{}/{}-patch.json'.format(self.__api_req_dir, date), 'w') as fw:
                        for news in json.loads(f.read()):
                            if news["article_id"] in self.__collectable_news_id_dict:
                                fw.write(json.dumps(news) + '\n')

#if __name__ == '__main__':
def main():
    INDEX_ROOT = "NC_Indexes"
    INTERMEDIATES_ROOT = "NC_Intermediates"
    TOKENIZER_ROOT = "NC_Tokenizer"
    RESULT_ROOT = "NC_Result"

    API_REQUEST_DIR = "NC_APIRequest"
    API_RESULT_DIR = "NC_APIResult"

    intermediates_dir = os.path.join(INTERMEDIATES_ROOT)
    os.makedirs(intermediates_dir, exist_ok=True)

    print("Start building all keywords ...")
    KeywordAggregater(tokenized_dir=TOKENIZER_ROOT, indexfile_dir=INDEX_ROOT, intermediate_dir=INTERMEDIATES_ROOT).execute()
    print("Start filtering keywords, domains and titles ...")
    NewsCollecting(result_dir=RESULT_ROOT, indexfile_dir=INDEX_ROOT, intermediate_dir=INTERMEDIATES_ROOT).execute()
    print("Start generate request data for NLAPI ...")
    APIReqGen(tokenized_dir=TOKENIZER_ROOT, api_req_dir=API_REQUEST_DIR, api_result_dir=API_RESULT_DIR, intermediate_dir=INTERMEDIATES_ROOT).execute()


