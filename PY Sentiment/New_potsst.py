# utility.py
import json
import requests
import pandas as pd
from textblob import TextBlob
from datetime import datetime

# Function to fetch Reddit posts for a list of stocks in a specific subreddit
def fetch_reddit_posts(subreddit, stocks, limit_per_stock=15):
    """
    Fetches Reddit posts for specified stocks in a subreddit.
    """
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

# Function to perform sentiment analysis on text
def analyze_sentiment(text):
    """
    Analyzes the sentiment of a given text.
    Returns:
        str: 'positive', 'neutral', or 'negative' sentiment.
    """
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

# Main function to aggregate Reddit posts across multiple subreddits and perform sentiment analysis
def get_aggregated_stock_posts(subreddits, stocks, limit_per_stock=15, save_to_csv=False):
    """
    Fetches and aggregates Reddit posts from multiple subreddits for specified stocks and analyzes sentiment.

    Returns:
        pd.DataFrame: A DataFrame containing aggregated data with sentiment analysis.
    """
    all_data = []

    for subreddit in subreddits:
        print(f"Fetching posts from r/{subreddit}")
        stock_posts = fetch_reddit_posts(subreddit, stocks, limit_per_stock)
        
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
    
    # Save DataFrame to CSV if required
    #if save_to_csv:
       # df.to_csv('reddit_stock_posts.csv', index=False)
    
    return df

# Example usage
if __name__ == "__main__":
    
    stocks = [
        'tesla', 'apple', 'amazon', 'google', 'microsoft', 'facebook', 'nvidia', 'netflix',
        'twitter', 'shopify', 'ibm', 'oracle', 'intel', 'amd', 'salesforce', 'paypal', 
        'adobe', 'zoom', 'snap', 'spotify', 'uber', 'lyft', 'airbnb', 'square', 
        'paypal', 'baidu', 'alibaba', 'tencent', 'microsoft', 'cisco', 'hp', 'dell', 
        'twitter', 'roku', 'qualcomm', 'baba', 'lg', 't-mobile', 'morgan', 'wells'
    ]
    
    subreddits = ['Investing', 'Stocks', 'WallStreetBets', 'Options', 'GlobalMarkets']
    
    
    df = get_aggregated_stock_posts(subreddits, stocks, limit_per_stock=15, save_to_csv=True)
    print(df[['subreddit', 'stock', 'post', 'author', 'created_time', 'sentiment']])
