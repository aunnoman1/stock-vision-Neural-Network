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
    analysis=Textblob(text)
    if analysis.sentiment.polarity >0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

stocks = [
    'tesla', 'apple', 'amazon', 'google', 'microsoft', 'facebook', 'nvidia', 'netflix',
    'twitter', 'shopify', 'ibm', 'oracle', 'intel', 'amd', 'salesforce', 'paypal', 
    'adobe', 'zoom', 'snap', 'spotify', 'uber', 'lyft', 'airbnb', 'square', 
    'paypal', 'baidu', 'alibaba', 'tencent', 'microsoft', 'cisco', 'hp', 'dell', 
    'twitter', 'roku', 'qualcomm', 'baba', 'lg', 't-mobile', 'morgan', 'wells'
]

# List of subreddits to fetch posts from
subreddits = ['Investing', 'Stocks', 'WallStreetBets', 'Options', 'GlobalMarkets']

# Initialize an empty list to hold all posts data
all_data = []

# Fetch Reddit posts for each subreddit and aggregate them
for subreddit in subreddits:
    print(f"Fetching posts from r/{subreddit}")
    stock_posts = fetch_reddit_posts(subreddit, stocks, 15)
    
    for stock in stocks:
        for post in stock_posts[stock]:
            sentiment = analyze_sentiment(post['title'])  # Perform sentiment analysis
            all_data.append({
                'subreddit': subreddit,
                'stock': stock, 
                'post': post['title'], 
                'author': post['author'], 
                'created_time': post['created_time'], 
                'sentiment': sentiment
            })

# Create a DataFrame with the collected data
df = pd.DataFrame(all_data)

# Display the DataFrame with subreddit, stock, post, author, time, and sentiment
print(df[['subreddit', 'stock', 'post', 'author', 'created_time', 'sentiment']])

# Saving the DataFrame to CSV for future reference
df.to_csv('reddit_stock_posts.csv', index=False)

# Testing: Check the shape of the dataframe to ensure data is collected
assert df.shape[0] > 0, "No data was collected, check API responses."
