import copy
import glob
import json
import math
import operator
import os

# 產生每篇新聞的情緒分數
class SentimentCalculate:
    def execute(self, intermediates_dir: str, api_result_dir: str):
        news_scores = {}

        for file_path in glob.glob('./{}/*-sentiment.json'.format(api_result_dir)):
            with open(file_path, 'r') as f:
                for line in f.readlines():
                    news = json.loads(line)
                    magnitude_score = 0
                    document_score = 0
                    score = 0
                    sentence_count = 0
                    if "sentences" in news["sentiment"]:
                        for sentence in news["sentiment"]["sentences"]:
                            if sentence["sentiment"]["magnitude"] > 0.1:
                                sentence_count += 1
                                score += sentence["sentiment"]["score"] * self.__nm_magnitude(sentence["sentiment"]["magnitude"], len(sentence["text"]["content"]))
                                magnitude_score += sentence["sentiment"]["magnitude"]

                    if "documentSentiment" in news["sentiment"]:
                        document_score = news["sentiment"]["documentSentiment"]["score"]
                    else:
                        document_score = 0
                        
                    if sentence_count > 0:
                        score = round(score/sentence_count, 2)
                        magnitude_score = round(magnitude_score/sentence_count, 2)
                    else:
                        score = 0
                    news_scores[news["article_id"]] = {
                        "sentence_count": sentence_count,
                        "document_score": document_score,
                        "average_score": score,
                        "magnitude_score": magnitude_score
                    }
        self.__write_to_file(news_scores, intermediates_dir)

    def __write_to_file(self, news_scores, intermediates_dir):
        with open("./{}/news_score.json".format(intermediates_dir), "w") as f:
            f.write(json.dumps(news_scores, indent=4))

    def __nm_magnitude(self, magnitude, length):
        return round(self.__nm_logistic_fn(magnitude) / self.__nm_natual_log(length), 2) * 6.923

    def __nm_logistic_fn(self, x):
        return round((2 / (1 + math.exp(-x * 2))) - 1, 4)

    def __nm_natual_log(self, x):
        return max(round((math.log(x) + 1) / 6 * 5, 2), 5)

# 產生圖表與標題資料
class DataGen:
    def __init__(self, tokenized_dir: str, intermediates_dir: str, result_dir: str, indexes_dir: str):
        print("... init ...")
        self.__top_sources = [1,3,10,4,17,5,22,12,16,11,39,2,45,24,41,60,59,58,27,28,9,6,20,40,48,35,29,38,56,34]

        self.__intermediates_dir = intermediates_dir
        self.__result_dir = result_dir
        self.__indexes_dir = indexes_dir
        self.__domains = {}
        self.__keyword_mapping = {}
        self.__keyword_mapping_reverse = {}
        self.__k = {}

        with open("./{}/all_news_keywords.json".format(self.__intermediates_dir), "r") as f:
            self.__news_info = json.loads(f.read())

        with open("./{}/news_score.json".format(self.__intermediates_dir), "r") as f:
            self.__score = json.loads(f.read())

        with open("./{}/domains.json".format(self.__result_dir), "r") as f:
            d_index = {}
            d = json.loads(f.read())
            for v in d:
                d_index[d[v]] = v
            with open("./{}/news_resources_with_title_trimed.json".format(self.__indexes_dir), "r") as f:
                self.__domains = json.loads(f.read())
                for name in self.__domains:
                    self.__domains[name] = d_index[self.__domains[name]]

        with open("./{}/keywords.json".format(self.__result_dir), "r") as f:
            keywords = json.loads(f.read())
            with open("./{}/keyword_mapping.json".format(self.__indexes_dir), 'r') as fm:
                km = json.loads(fm.read())
                for kid in keywords:
                    if type(km[keywords[kid]]) is list:
                        self.__keyword_mapping[kid] = [keywords[kid]] + km[keywords[kid]]
                        for kw in km[keywords[kid]]:
                            self.__keyword_mapping_reverse[kw] = kid
                    else:
                        self.__keyword_mapping[kid] = [km[keywords[kid]]]
                        self.__keyword_mapping_reverse[km[keywords[kid]]] = kid

        with open('./{}/collectable_keywords_all.txt'.format(self.__intermediates_dir), 'r') as f:
            self.__collectable_keywords = [x.strip() for x in list(f.readlines())]

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

    def __set_rk(self, target, kws):
        if not target in self.__k:
            self.__k[target] = {}
        for kw in kws:
            if not kw in self.__keyword_mapping[target] and kw in self.__collectable_keywords:
                if not self.__keyword_mapping_reverse[kw] in self.__k[target]:
                    self.__k[target][self.__keyword_mapping_reverse[kw]] = 0
                self.__k[target][self.__keyword_mapping_reverse[kw]] += 1

    def execute(self):
        r = {}
        t = {}
        with open("./{}/result_formated.json".format(self.__intermediates_dir), "r") as f:
            r = json.loads(f.read())
            t = copy.deepcopy(r)
            for kw in r:
                self.__k[kw] = {}
                for date in r[kw]:
                    t[kw][date] = {}
                    news_aggregated = {
                        "total": {"newsCount": 0, "newsSentimentScore": 0}
                    }
                    for domain in r[kw][date]:
                        source = self.__domains[domain]
                        news_list = [x["i"] for x in r[kw][date][domain]]
                        for news_id in news_list:
                            if news_id in self.__score:
                                news_aggregated["total"]["newsCount"] += 1
                                news_aggregated["total"]["newsSentimentScore"] += self.__score[news_id]["average_score"]    

                                if int(source) in self.__top_sources:
                                    if not source in news_aggregated:
                                        news_aggregated[source] = {"newsCount": 0, "newsSentimentScore": 0}
                                        t[kw][date][source] = []
                                    title = self.__trim_title(self.__news_info[news_id]["title"])
                                    if news_id in self.__score and title not in [x["t"] for x in t[kw][date][source]]:
                                        news_aggregated[source]["newsCount"] += 1
                                        news_aggregated[source]["newsSentimentScore"] += self.__score[news_id]["average_score"]
                                        t[kw][date][source].append({"t": title, "s": self.__score[news_id]["average_score"]})
                                        self.__set_rk(kw, self.__news_info[news_id]["keywords"])

                    for source_id in news_aggregated:
                        if news_aggregated[source_id]["newsCount"] == 0:
                            news_aggregated[source_id] = {"newsCount": 0, "newsSentimentScore": 0}
                        else:
                            news_aggregated[source_id]["newsSentimentScore"] = round(news_aggregated[source_id]["newsSentimentScore"]/news_aggregated[source_id]["newsCount"],2)

                    r[kw][date] = news_aggregated

        self.__write_to_graph(r)
        self.__write_to_newscontent(t)

    def __write_to_graph(self, r):
        for kw in r:
            sort_kw = sorted(self.__k[kw].items(), key=operator.itemgetter(1), reverse=True)[0:3]
            r[kw]["related_keywords"] = [x[0] for x in sort_kw]
            with open("./{}/result_graph/{}.json".format(self.__result_dir, kw), "w") as wf:
                wf.write(json.dumps(r[kw], ensure_ascii=False))

    def __write_to_newscontent(self, t):
        for kw in t:
            for date in t[kw]:
                with open("./{}/result_newscontent/{}-{}.json".format(self.__result_dir, "".join(date.split("/")), kw), "w") as wf:
                    wf.write(json.dumps(t[kw][date], ensure_ascii=False))


#if __name__ == '__main__':
def main():
    INTERMEDIATES_ROOT = "NC_Intermediates"
    API_RESULT_DIR = "NC_APIResult"
    RESUTL_DIR = "NC_Result"
    TOKENIZER_ROOT = "NC_Tokenizer"
    INDEX_ROOT = "NC_Indexes"

    os.makedirs(os.path.join(RESUTL_DIR, "result_graph"), exist_ok=True)
    os.makedirs(os.path.join(RESUTL_DIR, "result_newscontent"), exist_ok=True)
    print("Start calculate all sentiment scores ...")
    SentimentCalculate().execute(intermediates_dir=INTERMEDIATES_ROOT, api_result_dir=API_RESULT_DIR)
    print("Generating graph and newscontent data ...")
    DataGen(tokenized_dir=TOKENIZER_ROOT, intermediates_dir=INTERMEDIATES_ROOT, result_dir=RESUTL_DIR, indexes_dir=INDEX_ROOT).execute()
