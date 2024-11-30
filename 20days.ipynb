import json
import requests
from datetime import datetime, timedelta
import nltk
import pandas as pd  

nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

STOCKS = [
    'nvidia', 'tesla', 'apple', 'facebook', 'microsoft', 'amazon', 'amd',
    'broadcom', 'palantir', 'microstrategy', 'exxon', 'micron', 'jpmorgan', 'google',
    'salesforce', 'lilly', 'vistra', 'home depot', 'netflix', 'unitedhealth',
    'bank of america', 'costco', 'super micro', 'chevron', 'visa', 'coinbase'
]

TICKERS = [
    'NVDA', 'TSLA', 'AAPL', 'META', 'MSFT', 'AMZN', 'AMD',
    'AVGO', 'PLTR', 'MSTR', 'XOM', 'MU', 'JPM', 'GOOG',
    'CRM', 'LLY', 'VST', 'HD', 'NFLX', 'UNH', 'BAC',
    'COST', 'SMCI', 'CVX', 'V', 'COIN'
]

SUBREDDITS = ['Investing', 'Stocks', 'WallStreetBets', 'Options', 'GlobalMarkets']



def fetch_reddit_posts(subreddit, stocks, max_posts_per_stock=None):
    headers = {"User-Agent": "Mozilla/5.0"}
    stock_posts = {stock: [] for stock in stocks}
    base_url = f"https://reddit.com/r/{subreddit}/search.json"
    
    # Calculate the timestamp for 20 days ago (from now)
    twenty_days_ago = datetime.utcnow() - timedelta(days=20)
    twenty_days_ago_timestamp = int(twenty_days_ago.timestamp())
    
    for stock in stocks:
        after = twenty_days_ago_timestamp 
        
        while max_posts_per_stock is None or len(stock_posts[stock]) < max_posts_per_stock:
            params = {
                'q': stock,          # Search for the stock keyword
                'sort': 'new',       # Sort by the newest posts
                'limit': 100,        # Maximum allowed posts per request
                'restrict_sr': True, # Restrict to the subreddit
                'after': after       
            }
            
            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error {response.status_code} while fetching {stock} from {subreddit}")
                break
            
            try:
                data = response.json()
            except ValueError as e:
                print("Error parsing JSON:", e)
                break
            
            if 'data' not in data or 'children' not in data['data']:
                break
            
            posts = data['data']['children']
            if not posts:  # Exit if no more posts
                break
            
            for post in posts:
                post_data = post['data']
                title = post_data['title'].lower()
                author = post_data.get('author', 'N/A')
                created_utc = post_data.get('created_utc')
                created_time = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d %H:%M:%S') if created_utc else "N/A"

                
                if created_utc >= twenty_days_ago_timestamp:
                    stock_posts[stock].append({
                        'title': title,
                        'author': author,
                        'created_time': created_time
                    })
                
              
                if max_posts_per_stock and len(stock_posts[stock]) >= max_posts_per_stock:
                    break
            
            # Update `after` for pagination
            after = posts[-1]['data']['created_utc']
            if after < twenty_days_ago_timestamp: 
                break
    
    return stock_posts


# Function to perform sentiment analysis on text
def analyze_sentiment(text):
    sid = SentimentIntensityAnalyzer()
    result = sid.polarity_scores(text)
    return result['compound']

# Main function to fetch posts, analyze sentiment, and save to a DataFrame
def get_aggregated_stock_posts(subreddits, stocks, max_posts_per_stock=None):
    all_posts = []  # List to store all the post data for creating the DataFrame
    
    for subreddit in subreddits:
        print(f"Fetching posts from r/{subreddit}")
        stock_posts = fetch_reddit_posts(subreddit, stocks, max_posts_per_stock)
        
        for i, stock in enumerate(stocks):
            print(f"Processing stock: {TICKERS[i]}")
            
            for post in stock_posts[stock]:
                sentiment = analyze_sentiment(post['title'])
                
                # Append the post data to the all_posts list
                all_posts.append({
                    'stock': stock,
                    'author': post['author'],
                    'created_time': post['created_time'],
                    'sentiment': sentiment,
                    'title': post['title']
                })
    
    # Create a DataFrame from the all_posts list
    df = pd.DataFrame(all_posts)
    
    return df

# Run the function and display DataFrame in Jupyter Notebook
df = get_aggregated_stock_posts(SUBREDDITS, STOCKS, max_posts_per_stock=100)
df.head()  # Display the first few rows of the DataFrame
