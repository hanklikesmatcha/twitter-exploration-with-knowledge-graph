import os

import toml
import json
import tweepy
import datetime
import csv
import pandas as pd
import spacy


def _get_current_time() -> datetime:
    return datetime.datetime.today().strftime("%Y-%m-%d")


def fetch_tweets():
    current_time = _get_current_time()
    file_name = f"top-trends-in-us-{current_time}.json"
    local_files = os.listdir()
    if file_name in local_files:
        print(f"We have got the latest tweets from {file_name}")
        return file_name
    else:
        config = toml.load("config.toml")
        bearer_token = config["BEARER_TOKEN"]
        woeid = config["WOEID"]
        auth = tweepy.OAuth2BearerHandler(bearer_token)
        api = tweepy.API(auth)

        # fetching the trendy topics
        trends = api.get_place_trends(id=woeid)
        with open(f"data/trends/{file_name}", "w") as wp:
            wp.write(json.dumps(trends, indent=1))


def read_and_sort_tweets():
    files = [f for f in os.listdir("data/trends") if not f.endswith("DS_Store")]
    sorted_files = sorted(
        files,
        key=lambda date: datetime.datetime.strptime(date[17:-5], "%Y-%m-%d"),
        reverse=True,
    )
    latest_file = sorted_files[0]
    json_file = open(f"data/trends/{latest_file}", "r")
    trend_list = json.load(json_file)[0]["trends"]
    unsorted_topics = [
        (t["name"], t["tweet_volume"])
        for t in trend_list
        if t["tweet_volume"] is not None
    ]
    sorted_topics = sorted(unsorted_topics, key=lambda t: int(t[1]), reverse=True)
    json_file.close()
    current_time = _get_current_time()
    file_name = f"top-10-trends-in-us-{current_time}.csv"
    with open(f"data/sorted_trends/{file_name}", "w") as csv_file:
        column_names = ["class_value", "name", "volume"]
        writer = csv.DictWriter(csv_file, fieldnames=column_names)
        writer.writeheader()
        for t in sorted_topics:
            writer.writerow({"name": t[0], "volume": t[1], "class_value": 0})


def analyse_tweets():
    files = [f for f in os.listdir("data/sorted_trends") if not f.endswith("DS_Store")]
    sorted_files = sorted(
        files,
        key=lambda date: datetime.datetime.strptime(date[20:-4], "%Y-%m-%d"),
        reverse=True,
    )
    latest_source = sorted_files[0]
    csv_file = open(f"data/sorted_trends/{latest_source}", "r")
    featured_data = pd.read_csv(csv_file)
    nlp = spacy.load("en_core_web_sm")
    labels = []
    for t in featured_data.iloc[:, 1].values:
        doc = nlp(str(t).replace("#", ''))
        label = [entity.label_ if entity else ' ' for entity in doc.ents]
        labels.append(''.join(label))
    featured_data.insert(3, "features", labels)
    current_time= _get_current_time()
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, f'data/categorised_data/{current_time}.csv')
    featured_data.to_csv(filename)


if __name__ == "__main__":
    file = fetch_tweets()
    read_and_sort_tweets()
    analyse_tweets()
