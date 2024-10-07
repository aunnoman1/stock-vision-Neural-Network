# stockapp/utils.py

import json
import requests
import pandas as pd
from textblob import TextBlob
from datetime import datetime

def fetch_reddit_posts(subreddit, stocks, limit_per_stock=15):
    url = f"https://reddit.com/r/{subreddit}.json"
    headers = {"User-Agent": "Mozilla/5.0"}
    stock_posts = {stock: [] for stock in stocks}
    params = {'limit': 100}

    while any(len(posts) < limit_per_stock for posts in stock_posts.values()):
        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        if 'data' not in data or 'children' not in data['data']:
            break

        for post in data["data"]["children"]:
            post_data = post["data"]
            title = post_data["title"].lower()
            author = post_data.get("author", "N/A")
            created_utc = post_data.get("created_utc", None)

            # Convert Unix timestamp to human-readable format
            if created_utc:
                created_time = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d %H:%M:%S')
            else:
                created_time = "N/A"

            for stock in stocks:
                if stock in title and len(stock_posts[stock]) < limit_per_stock:
                    stock_posts[stock].append({
                        'title': title,
                        'author': author,
                        'created_time': created_time
                    })
                    if all(len(posts) >= limit_per_stock for posts in stock_posts.values()):
                        break

        if 'data' in data and 'after' in data['data'] and data['data']['after']:
            url = f"https://reddit.com/r/{subreddit}.json?after={data['data']['after']}"
        else:
            break

    return stock_posts

def analyze_sentiment(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

