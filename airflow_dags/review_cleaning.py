import pandas as pd
import string
import re
from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk
import json

# Ensure NLTK data is downloaded
nltk.download('punkt')
nltk.download('stopwords')

#stop_words = set(stopwords.words('english'))
custom_stopwords = {'book', 'read', 'say', 'everything', 'story', 'page', 'thing', 'make', 'go', 'one','like','feel','series','audiobook',"still"}
stop_words = set(stopwords.words('english')).union(custom_stopwords)


def clean_and_tokenize(text):
    if pd.isna(text):
        return []
    text = text.lower()
    text = re.sub(r'https?://\S+|www\.\S+', '', text)  # remove URLs
    text = text.translate(str.maketrans('', '', string.punctuation))  # remove punctuation
    tokens = word_tokenize(text)
    
    return [word for word in tokens if word not in stop_words and word.isalpha() and len(word) > 2]


def tokenize_reviews_per_book(book_col='title', review_col='review'):
    """
    Group reviews by book title, clean and tokenize all text for each book.
    Returns DataFrame with book title and aggregated token frequency.
    """
    df = pd.read_parquet("/home/vaibhavi/spark-ml-venv/Book_Analysis/data/book_reviews.parquet")

    # Combine all reviews per book into a single string
    grouped = df.groupby(book_col)[review_col].apply(lambda reviews: " ".join(reviews.dropna())).reset_index()

    # Tokenize and get word frequency for each book
    grouped['word_freq'] = grouped[review_col].apply(lambda text: dict(Counter(clean_and_tokenize(text))))

    # Convert dict to JSON string so Parquet can store it cleanly
    grouped['word_freq'] = grouped['word_freq'].apply(json.dumps)

# Save to Parquet
    grouped[[book_col, 'word_freq']].to_parquet(
        "/home/vaibhavi/spark-ml-venv/Book_Analysis/data/book_word_frequencies.parquet",
        index=False,
        engine='pyarrow'  # or 'fastparquet'
    )


    return grouped


result_df = tokenize_reviews_per_book()
print(result_df.head())
