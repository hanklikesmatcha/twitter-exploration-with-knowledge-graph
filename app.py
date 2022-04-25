import os

import toml
import json
import tweepy
import datetime
import csv
import pandas as pd
import spacy
from spacy import matcher as spacy_matcher
from tqdm import tqdm
import networkx as nx
import matplotlib.pyplot as plt


def _get_nlp():
    return spacy.load("en_core_web_sm")


def _get_files(folder: str):
    return [f for f in os.listdir(f"data/{folder}") if not f.endswith("DS_Store")]


def _get_current_time() -> datetime:
    return datetime.datetime.today().strftime("%Y-%m-%d")


def _get_relation(input):
    nlp = _get_nlp()
    doc = nlp(input)

    # Matcher class object
    matcher = spacy_matcher.Matcher(nlp.vocab)
    # define the pattern
    pattern = [
        {"DEP": "ROOT"},
        {"DEP": "prep", "OP": "?"},
        {"DEP": "agent", "OP": "?"},
        {"POS": "ADJ", "OP": "?"},
    ]

    matcher.add("matching_1", [pattern])

    matches = matcher(doc)
    k = len(matches) - 1

    span = doc[matches[k][1] : matches[k][2]]

    return span.text


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
    files = _get_files(folder="sorted_trends")
    sorted_files = sorted(
        files,
        key=lambda date: datetime.datetime.strptime(date[20:-4], "%Y-%m-%d"),
        reverse=True,
    )
    latest_source = sorted_files[0]
    csv_file = open(f"data/sorted_trends/{latest_source}", "r")
    featured_data = pd.read_csv(csv_file)
    nlp = _get_nlp()
    labels = []
    for t in featured_data.iloc[:, 1].values:
        doc = nlp(str(t).replace("#", ""))
        label = [entity.label_ if entity else " " for entity in doc.ents]
        labels.append("".join(label))
    featured_data.insert(3, "features", labels)
    current_time = _get_current_time()
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, f"data/categorised_data/{current_time}.csv")
    featured_data.to_csv(filename)


def find_relation():
    files = _get_files(folder="categorised_data")
    sorted_files = sorted(
        files,
        key=lambda date: datetime.datetime.strptime(date[0:-4], "%Y-%m-%d"),
        reverse=True,
    )
    # latest_source = sorted_files[0]
    with open(f"tmp.csv", "wb") as write:
        for f in files:
            with open(f"data/categorised_data/{f}", "rb") as read:
                next(read)
                write.write(read.read())
    featured_data = pd.read_csv("tmp.csv").fillna("UNKNOWN")
    featured_data.columns = ["id", "class_value", "name", "volume", "features"]
    print(featured_data)
    entity_pairs = [(i[2], i[4]) for i in featured_data.values]
    relations = [_get_relation(i) for i in tqdm(featured_data["features"])]
    ranking = pd.Series(relations).value_counts()[:5]
    print(ranking)
    # extract subject
    source = [i[0] for i in entity_pairs]

    # extract object
    target = [i[1] for i in entity_pairs]

    kg_df = pd.DataFrame({"source": source, "target": target, "edge": relations})
    print(kg_df)
    # create a directed-graph from a dataframe
    # Composed By
    G = nx.from_pandas_edgelist(
        kg_df[kg_df["edge"] == "PERSON"],
        "source",
        "target",
        edge_attr=True,
        create_using=nx.MultiDiGraph(),
    )

    plt.figure(figsize=(12, 12))
    pos = nx.spring_layout(G, k=0.5)  # k regulates the distance between nodes
    nx.draw(
        G,
        with_labels=True,
        node_color="red",
        node_size=1500,
        edge_cmap=plt.cm.Blues,
        pos=pos,
        font_weight="bold",
    )
    plt.show()
    # Written By
    G = nx.from_pandas_edgelist(
        kg_df[kg_df["edge"] == "ORG"],
        "source",
        "target",
        edge_attr=True,
        create_using=nx.MultiDiGraph(),
    )

    plt.figure(figsize=(12, 12))
    pos = nx.spring_layout(G, k=0.5)
    nx.draw(
        G,
        with_labels=True,
        node_color="red",
        node_size=1500,
        edge_cmap=plt.cm.Blues,
        pos=pos,
    )
    plt.show()
    # Release In
    G = nx.from_pandas_edgelist(
        kg_df[kg_df["edge"] == "GPE"],
        "source",
        "target",
        edge_attr=True,
        create_using=nx.MultiDiGraph(),
    )

    plt.figure(figsize=(12, 12))
    pos = nx.spring_layout(G, k=0.5)
    nx.draw(
        G,
        with_labels=True,
        node_color="red",
        node_size=1500,
        edge_cmap=plt.cm.Blues,
        pos=pos,
    )
    plt.show()


if __name__ == "__main__":
    file = fetch_tweets()
    read_and_sort_tweets()
    analyse_tweets()
    find_relation()
