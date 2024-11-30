import os
import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import tensorflow as tf
from tqdm import tqdm
import nltk
nltk.download('vader_lexicon')
from multiprocessing import Pool, cpu_count


from nltk.sentiment import SentimentIntensityAnalyzer

stocks = ['AAPL','GME', 'MCD', 'MSFT', 'NFLX', 'NVDA', 'TSLA']



posts_file_path=os.path.join(os.path.dirname(os.path.dirname(os.getcwd())),"post_data","posts.csv")
all_posts_df= pd.read_csv(posts_file_path)

stock_index_file_path=os.path.join(os.path.dirname(os.path.dirname(os.getcwd())),"post_data","stock_index.csv")
stock_index_df= pd.read_csv(stock_index_file_path)

#making date proper
all_posts_df['created_utc'] = pd.to_datetime(all_posts_df['created_utc'], unit='s')
stock_index_df['created_utc'] = pd.to_datetime(stock_index_df['created_utc'], unit='s')
#merging on basis of id,time
post_stock_df= pd.merge(all_posts_df, stock_index_df, on=['id'], how='inner')


post_stock_df=post_stock_df.drop(columns= ['subreddit', 'author', 'permalink', 'url','created_utc_y'])
post_stock_df=post_stock_df.rename(columns={'created_utc_x': 'created_at'})
post_stock_df=post_stock_df.sort_values(by=['stock_symbol','created_at'])



for stock in stocks:

    print(f"Processing {stock} data")
    
    price_file_path=os.path.join(os.path.dirname(os.path.dirname(os.getcwd())),"price_data",f"{stock}.csv")
    price_df = pd.read_csv(price_file_path)


    #Correct date format
    price_df['Date'] = pd.to_datetime(price_df['Date'], errors='coerce', utc=True)

    #drop dividents and stock splits
    price_df.drop(columns=['Dividends', 'Stock Splits'], inplace=True)

    price_df['Date'] = price_df['Date'].dt.date


    # Filter posts related to stock and format the 'created_at' column
    posts_df = post_stock_df[post_stock_df['stock_symbol'].str.lower() == stock.lower()].copy(deep=True)
    posts_df['Date'] = pd.to_datetime(posts_df['created_at']).dt.date
    posts_df.drop(columns=['created_at'], inplace=True)

    

    # Replace NaN values in the 'selftext' column with an empty string
    posts_df['selftext'] = posts_df['selftext'].fillna('')

    # Filter Apple price data for the date range 2018-01-01 to 2022-12-31
    start_date = pd.to_datetime('2018-01-01').date()
    end_date = pd.to_datetime('2022-12-31').date()
    price_df_filtered = price_df[(price_df['Date'] >= start_date) & (price_df['Date'] <= end_date)]

    #merging title and selftext
    posts_df['text'] = posts_df['title'] + '. ' + posts_df['selftext']
    posts_df=posts_df.drop(columns=['title','selftext'])


    def analyze_text(text):
        analyzer = SentimentIntensityAnalyzer()  # Each process gets its own instance
        scores = analyzer.polarity_scores(str(text))
        return scores['pos'], scores['neg'], scores['neu']
    # Wrapper function for tqdm to work with multiprocessing
    def tqdm_wrapper(func):
        def inner(data):
            return list(tqdm(func(data), total=len(data), desc="Processing Sentiments"))
        return inner
    # Parallel sentiment analysis with progress bar
    def parallel_sentiment_analysis(df, text_column):
        texts = df[text_column].tolist()  # Extract the text column as a list
        num_cores = cpu_count()
        print(f"Using {num_cores} CPU cores")

        # Initialize tqdm
        with Pool(num_cores) as pool:
            results = list(tqdm(pool.imap(analyze_text, texts), total=len(texts), desc="Sentiment Analysis"))

        # Convert the results into a DataFrame
        sentiment_df = pd.DataFrame(results, columns=['pos', 'neg', 'neu'])
        return pd.concat([df.reset_index(drop=True), sentiment_df], axis=1)
    posts_df = parallel_sentiment_analysis(posts_df, 'text')

    # Remove the 'text' column
    posts_df = posts_df.drop(columns=["text"])

    # Group by 'Date' and calculate the mean of 'pos', 'neg', and 'neu'
    average_sentiments = posts_df.groupby('Date')[['pos', 'neg', 'neu']].mean().reset_index()
    average_sentiments.columns = ['Date', 'average_pos', 'average_neg', 'average_neu']

    def categorize_sentiment(row):
        if row['pos'] > row['neg'] and row['pos'] > row['neu']:
            return 'positive'
        elif row['neg'] > row['pos'] and row['neg'] > row['neu']:
            return 'negative'
        else:
            return 'neutral'
    posts_df['sentiment_category'] = posts_df.apply(categorize_sentiment, axis=1)

    # Calculate daily sentiment ratios
    sentiment_ratios = (
        posts_df.groupby(['Date', 'sentiment_category'])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    sentiment_ratios['total_posts'] = sentiment_ratios[['positive', 'negative', 'neutral']].sum(axis=1)
    sentiment_ratios['positive_ratio'] = sentiment_ratios['positive'] / sentiment_ratios['total_posts']
    sentiment_ratios['negative_ratio'] = sentiment_ratios['negative'] / sentiment_ratios['total_posts']
    sentiment_ratios['neutral_ratio'] = sentiment_ratios['neutral'] / sentiment_ratios['total_posts']

    # Merge the average sentiments and sentiment ratios back into the original DataFrame
    combined_sentiments = pd.merge(average_sentiments, sentiment_ratios, on='Date', how='inner')


    combined_sentiments.drop(columns=['positive', 'negative', 'neutral', 'total_posts'], inplace=True)




    #Merging and creating
    merged_df = pd.merge(price_df_filtered,combined_sentiments, on='Date', how='left')


    merged_df.to_pickle(os.path.join(f'all_avg_ratio',f'{stock}_avg_ratio.pkl'))