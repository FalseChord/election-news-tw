import json
import uuid
import os
import re
from tqdm import tqdm

def fetch_news_file(source_dir: str):
    news_file_list = [
        os.path.join(source_dir, file) for file in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, file))
    ]
    news_file_list = [
        file for file in news_file_list if os.path.splitext(os.path.basename(file))[-1] != ".gz"
    ]
    return news_file_list

def news_indexed(file_date, converted_news_dir):
    file_path = os.path.join(converted_news_dir, "{}-article.json".format(file_date))
    return os.path.isfile(file_path)

def keyword_extracted(file_date, converted_news_dir):
    file_path = os.path.join(converted_news_dir, "{}-keyword.json".format(file_date))
    return os.path.isfile(file_path)

class JsonFormatChecker:
    @staticmethod
    def execute(news_json_string: str):
        json_string_is_valid = True
        try:
            json.loads(news_json_string)
        except:
            json_string_is_valid = False

        return json_string_is_valid

class NewsFileIndexer:
    def __init__(self, source_file_path: str, converted_news_dir: str):
        self.__source_file_path = source_file_path
        self.__date_string = re.findall(r"[0-9]{8}", os.path.basename(self.__source_file_path))[0]
        self.__converted_news_dir = converted_news_dir

        self.__news_json_list = list()
        self.__invalid_news_json_string_list = list()

    def execute(self):
        self.__convert_all_news_json_string(source_news_list=self.__fetch_news_json_string())
        self.__save_json_to_file()

    def __fetch_news_json_string(self):
        with open(self.__source_file_path, "r", encoding='utf-8', errors='ignore') as file:
            news_json_string_list = file.readlines()
        return news_json_string_list

    def __convert_all_news_json_string(self, source_news_list: list):
        for news_json in tqdm(source_news_list):
            if JsonFormatChecker.execute(news_json):
                news_json = json.loads(news_json)
                self.__news_json_list.append(SingleNewsContentConverter().index(
                    news_json= news_json,
                    date_string= self.__date_string
                ))
            else:
                self.__invalid_news_json_string_list.append(news_json)

    def __save_json_to_file(self):
        article_file_path = os.path.join(self.__converted_news_dir, "{}-article.json".format(self.__date_string))
        invalid_news_json_string_file_path = os.path.join(
            self.__converted_news_dir, "{}-invalid.txt".format(self.__date_string)
        )

        self.__write_json_to_file(json_object=self.__news_json_list, file_path=article_file_path)
        self.__write_invalid_news_json_string_to_file(invalid_news_json_string_file_path)

    @staticmethod
    def __write_json_to_file(json_object: list, file_path: str):
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(json_object, file, ensure_ascii=False)

        print("{} saved.".format(file_path))

    def __write_invalid_news_json_string_to_file(self, invalid_news_json_string_file_path):
        if len(self.__invalid_news_json_string_list) > 0:
            with open(invalid_news_json_string_file_path, 'a', encoding='utf-8') as file:
                for invalid_news_json_string in self.__invalid_news_json_string_list:
                    file.write(invalid_news_json_string)

            print("{} saved.".format(invalid_news_json_string_file_path))

class KeywordExtractor:
    def __init__(self, source_file_path: str, converted_news_dir: str, index_dir:str):
        self.__source_file_path = source_file_path
        self.__date_string = re.findall(r"[0-9]{8}", os.path.basename(self.__source_file_path))[0]
        self.__converted_news_dir = converted_news_dir

        self.__news_json_list = dict()

        self.__keyword_list = list()
        with open(os.path.join(index_dir, "keyword_mapping.json"), "r") as f:
            kw_mapping = json.loads(f.read())
            for k in kw_mapping:
                if type(kw_mapping[k]) is list:
                    for kw in kw_mapping[k]:
                        self.__keyword_list.append(kw)
                else:
                    self.__keyword_list.append(k)

        self.__domain_string = re.compile(r'^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\?\/\n]+)',re.I)
        self.__domain_list = {}
        with open('./{}/news_domain_list.json'.format(index_dir), 'r') as f:
            self.__domain_list = json.loads(f.read())

    def execute(self):
        articles = self.__fetch_indexed_articles()
        self.__convert_all_news_json(article_dict=articles)
        self.__save_json_to_file()

    def __fetch_indexed_articles(self):
        article_dict = dict()
        try:
            with open(os.path.join(self.__converted_news_dir, "{}-article.json".format(self.__date_string)), "r", encoding='utf-8', errors='ignore') as file:
                articles = json.loads(file.read())
                for article in articles:
                    article_dict[article["article_id"]] = article
                return article_dict
        except:
            return dict()

    def __convert_all_news_json(self, article_dict: dict):
        for news_id in tqdm(article_dict):
            news_domain = self.__domain_string.search(article_dict[news_id]["url"]).group(0)

            if news_domain in self.__domain_list:
                extracted_json = SingleNewsContentConverter().extract(
                    news_json= article_dict[news_id],
                    date_string= self.__date_string,
                    keyword_list= self.__keyword_list
                )

                if "keywords" in extracted_json and len(extracted_json["keywords"]) > 0:
                    extracted_json["domain"] = news_domain
                    self.__news_json_list[extracted_json["article_id"]] = extracted_json

    def __save_json_to_file(self):
        keyword_file_path = os.path.join(self.__converted_news_dir, "{}-keyword.json".format(self.__date_string))
        self.__write_json_to_file(json_object=self.__news_json_list, file_path=keyword_file_path)

    @staticmethod
    def __write_json_to_file(json_object: list, file_path: str):
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(json_object, file, ensure_ascii=False)

        print("{} saved.".format(file_path))

class SingleNewsContentConverter:
    def extract(self, news_json: dict, date_string: str, keyword_list: list):
        news_keyword = list()
        substring_keywords = list()

        searchable_content = "{} \n {}".format(news_json["title"], news_json["content_text"])
        news_keyword = self.__fetch_keyword_json(content_text=searchable_content, keyword_list=keyword_list)

        if "substrings" in news_json:
            substring_keywords = self.__extract_from_substrings(substrings=news_json['substrings'], keyword_list=keyword_list)

        news_json["keywords"] = list(set(news_keyword + substring_keywords))

        return news_json

    def index(self, news_json: dict, date_string: str):
        if not "article_id" in news_json:
            article_id = "{}-{}".format(date_string, uuid.uuid4())
            news_json['article_id'] = article_id

        return news_json

    @staticmethod
    def __fetch_keyword_json(content_text: str, keyword_list: str) -> list:
        article_keywords = list()
        for keyword in keyword_list:
            if keyword in content_text:
                article_keywords.append(keyword)

        return article_keywords

    @staticmethod
    def __extract_from_substrings(substrings: dict, keyword_list: str) -> list:
        article_keywords = list()
        for sub_cate in substrings:
            for keyword_candidate in substrings[sub_cate]:
                for kw in keyword_list:
                    if keyword_candidate == kw:
                        article_keywords.append(keyword_candidate)

        return article_keywords

#if __name__ == '__main__':
    #REDO=True
def main(REDO=False):
    DATA_ROOT = "NC_Origin"
    INDEX_ROOT = "NC_Indexes"
    TOKENIZER_ROOT = "NC_Tokenizer"

    source_dir = os.path.join(DATA_ROOT)
    index_dir = os.path.join(INDEX_ROOT)
    converted_news_dir = os.path.join(TOKENIZER_ROOT)

    source_file_list = fetch_news_file(source_dir)
    os.makedirs(converted_news_dir, exist_ok=True)

    for source_file in source_file_list:

        splited_file_path = os.path.splitext(os.path.basename(source_file))
        file_date = splited_file_path[0].split("-")[1]

        # if the news not indexed (with no article_id)
        if not news_indexed(file_date=file_date, converted_news_dir=converted_news_dir):
            print("converting: {}".format(source_file), "...")
            NewsFileIndexer(source_file_path=source_file, converted_news_dir=converted_news_dir).execute()

        # if the news keyword not extracted or need to redo keyword extraction
        if not keyword_extracted(file_date=file_date, converted_news_dir=converted_news_dir) or REDO:
            KeywordExtractor(source_file_path=source_file, converted_news_dir=converted_news_dir, index_dir=index_dir).execute()
