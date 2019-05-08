import json
import jieba
import jieba.analyse
import uuid
import os
import re
from tqdm import tqdm

def file_tokenized(converted_news_dir_path, news_file_path):
    file_name = os.path.basename(news_file_path)
    splited_file_path = os.path.splitext(file_name)

    date = splited_file_path[0].split("-")[1]

    full_file_path = os.path.join(converted_news_dir_path, "{}-keyword.json".format(date))
    return os.path.isfile(full_file_path)

def fetch_news_file(news_dir: str):
    news_file_list = [
        os.path.join(news_dir, file) for file in os.listdir(news_dir) if os.path.isfile(os.path.join(news_dir, file))
    ]
    news_file_list = [
        file for file in news_file_list if os.path.splitext(os.path.basename(file))[-1] != ".gz"
    ]
    return news_file_list

class NewsFileConverter:
    def __init__(self, news_file_path: str, converted_news_dir_path: str):
        self.__news_file_path = news_file_path
        self.__converted_news_dir_path = converted_news_dir_path

        self.__news_json_list = list()
        self.__keyword_json_list = list()
        self.__invalid_news_json_string_list = list()

    def execute(self):
        news_json_string_list = self.__fetch_news_json_string()
        date_string = re.findall(r"[0-9]{8}", os.path.basename(news_file))[0]
        self.__convert_all_news_json_string(news_json_string_list=news_json_string_list, date_string=date_string)
        self.__save_json_to_file(date_string)

    def __fetch_news_json_string(self):
        with open(self.__news_file_path, "r", encoding='utf-8', errors='ignore') as file:
            news_json_string_list = file.readlines()

        return news_json_string_list

    def __convert_all_news_json_string(self, news_json_string_list: list, date_string: str):
        for news_json_string in tqdm(news_json_string_list):
            if JsonFormatChecker.execute(news_json_string) is True:
                news_json, keyword_json = SingleNewsContentConverter().execute(
                    news_json_string=news_json_string,
                    date_string=date_string
                )

                self.__news_json_list.append(news_json)
                self.__keyword_json_list.append(keyword_json)
            else:
                self.__invalid_news_json_string_list.append(news_json_string)

    def __save_json_to_file(self, date_string: str):
        converted_news_file_path = os.path.join(self.__converted_news_dir_path, "{}-article.json".format(date_string))
        keyword_path = os.path.join(self.__converted_news_dir_path, "{}-keyword.json".format(date_string))
        invalid_news_json_string_file_path = os.path.join(
            self.__converted_news_dir_path, "{}-invalid.txt".format(date_string)
        )

        self.__write_json_to_file(json_object=self.__news_json_list, file_path=converted_news_file_path)
        self.__write_json_to_file(json_object=self.__keyword_json_list, file_path=keyword_path)
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

class JsonFormatChecker:
    @staticmethod
    def execute(news_json_string: str):
        json_string_is_valid = True
        try:
            json.loads(news_json_string)
        except:
            json_string_is_valid = False

        return json_string_is_valid

class SingleNewsContentConverter:
    def execute(self, news_json_string: str, date_string: str):
        article_id = "{}-{}".format(date_string, uuid.uuid4())

        news_json = json.loads(news_json_string)
        news_json['article_id'] = article_id

        keyword_json = self.__fetch_keyword_json(content_text=news_json['content_text'], article_id=article_id)

        return news_json, keyword_json

    @staticmethod
    def __fetch_keyword_json(content_text: str, article_id: str) -> dict:
        keyword_list = jieba.analyse.extract_tags(content_text, topK=10, withWeight=True)
        keyword_json = {"article_id": article_id, "keyword_list": list()}

        for keyword, weight in keyword_list:
            keyword_json["keyword_list"].append({"keyword": keyword, "weight": weight})

        return keyword_json

#if __name__ == '__main__':
def main():
    DATA_ROOT = "NC_Origin"
    INDEX_ROOT = "NC_Indexes"
    TOKENIZER_ROOT = "NC_Tokenizer"

    moe_dict_path = os.path.join(INDEX_ROOT, "dict", "moe_expand.dict")
    stop_word_dict_path = os.path.join(INDEX_ROOT, "dict", "stopping_words.dict")
    
    news_dir = os.path.join(DATA_ROOT)
    converted_news_dir = os.path.join(TOKENIZER_ROOT)

    jieba.load_userdict(moe_dict_path)
    jieba.analyse.set_stop_words(stop_word_dict_path)

    news_file_list = fetch_news_file(news_dir)
    os.makedirs(converted_news_dir, exist_ok=True)

    for news_file in news_file_list:
        if not file_tokenized(news_file_path=news_file, converted_news_dir_path=converted_news_dir):
            print("converting: {}".format(news_file))
            NewsFileConverter(news_file_path=news_file, converted_news_dir_path=converted_news_dir).execute()
