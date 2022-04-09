import os
import toml
import json
import tweepy
import datetime


def _get_current_time() -> datetime:
    return datetime.datetime.today().strftime("%Y-%m-%d")


def fetch_tweets() -> str:
    current_time = _get_current_time()
    file_name = f"top-trends-in-us-{current_time}.json"
    local_files = os.listdir()
    if file_name in local_files:
        print(f"We have got the latest tweets from {file_name}")
        return file_name
    else:
        config = toml.load('config.toml')
        bearer_token = config["BEARER_TOKEN"]
        woeid = config["WOEID"]
        auth = tweepy.OAuth2BearerHandler(bearer_token)
        api = tweepy.API(auth)

        # fetching the trendy topics
        trends = api.get_place_trends(id=woeid)
        with open(file_name, "w") as wp:
            wp.write(json.dumps(trends, indent=1))
        return file_name


def read_and_sort_tweets(source: str) -> str:
    json_file = open(source, 'r')
    trend_list = json.load(json_file)[0]['trends']
    unsorted_topics = [(t['name'], t['tweet_volume']) for t in trend_list if t['tweet_volume'] is not None]
    sorted_topics = sorted(unsorted_topics, key=lambda t: int(t[1]), reverse=True)
    json_file.close()
    current_time = _get_current_time()
    file_name = f"top-10-trends-in-us-{current_time}.json"
    with open(file_name, "w") as wp:
        wp.write(json.dumps(sorted_topics, indent=1))
    return file_name


def analyse_tweets(source: str):
    json_file = open(source, 'r')
    for t in json.load(json_file):
        print(f"{t[0]} - {t[1]}")


if __name__ == '__main__':
    file = fetch_tweets()
    sorted_trend_list = read_and_sort_tweets(source=file)
    analyse_tweets(source=sorted_trend_list)
