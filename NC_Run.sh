#!/usr/bin/env bash

source env/bin/activate

python NC_Run.py

gsutil cp NC_Result/date.json gs://projects.readr.tw/election-news
gsutil -m cp -r NC_Result/result_graph gs://projects.readr.tw/election-news
gsutil -m cp -r NC_Result/result_newscontent gs://projects.readr.tw/election-news

gsutil -m acl ch -u AllUsers:R gs://projects.readr.tw/election-news/date.json
gsutil -m acl ch -u AllUsers:R gs://projects.readr.tw/election-news/result_graph/*
gsutil -m acl ch -u AllUsers:R gs://projects.readr.tw/election-news/result_newscontent/*
